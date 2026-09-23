use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use pgsteward_core::admission::ClientLimit;
use pgsteward_core::allocation::{InstanceId, ProxyId};
use pgsteward_core::auth::ClientCredentials;
use pgsteward_core::budget::InstanceBudget;
use pgsteward_core::cancel::{CancelRegistry, ProxyTag, forward_cancel};
use pgsteward_core::convergence::ProxyPools;
use pgsteward_core::grant::InProcessCoordinator;
use pgsteward_core::inspect::{InspectError, count_foreign_connections, read_server_limits};
use pgsteward_core::policy::Policies;
use pgsteward_core::pool::{InstanceOpener, Pool, PoolLimits};
use pgsteward_core::relay::transaction_mode;
use pgsteward_core::rt::{JoinHandle, Listener, Runtime};
use pgsteward_core::server::{
    ApplicationName, ConnectError, ServerConnection, ServerCredentials, connect,
};
use pgsteward_core::session::{Accepted, ClientSession};
use pgsteward_core::tenant::TenantId;
use pgsteward_protocol::backend::{ErrorResponse, encode_error_response, sqlstate};
use pgsteward_protocol::startup::CancelKey;
use pgsteward_sched::fair::WeightedMaxMinFair;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::config::{ClusterConfig, ConfigError, NodeConfig, PoolMode};

const PROXY: &str = "proxy";
const MONITOR: &str = "monitor";

/// Values the design leaves to measurement. None of them is a config key yet;
/// the defaults are provisional.
#[derive(Debug, Clone, Copy)]
pub struct ServeOptions {
    pub margin: u32,
    pub foreign_window: Duration,
    pub observe_interval: Duration,
    pub wait_timeout: Duration,
}

impl Default for ServeOptions {
    fn default() -> Self {
        Self {
            margin: 15,
            foreign_window: Duration::from_secs(60),
            observe_interval: Duration::from_secs(1),
            wait_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error("pool_mode = \"session\" is not served yet; set pool_mode = \"transaction\"")]
    SessionMode,
    #[error(
        "no [monitor] section: the node needs a login that can read max_connections and count every backend on each instance"
    )]
    NoMonitor,
    #[error("invalid config: {0}")]
    Config(#[from] ConfigError),
    #[error("cannot reach instance `{instance}` to derive its total budget: {source}")]
    Connect {
        instance: InstanceId,
        source: ConnectError,
    },
    #[error("cannot read the limits of instance `{instance}`: {source}")]
    Inspect {
        instance: InstanceId,
        source: InspectError,
    },
    #[error("cannot listen on {addr}: {source}")]
    Listen { addr: SocketAddr, source: io::Error },
}

type NodePools<R> = ProxyPools<InstanceOpener<R>, R>;
type Coordinator = InProcessCoordinator<WeightedMaxMinFair>;

/// A node that is serving. Dropping it stops every task it started.
pub struct Serving<R: Runtime> {
    addr: SocketAddr,
    coordinator: Arc<Coordinator>,
    pools: Arc<NodePools<R>>,
    tasks: Vec<JoinHandle<()>>,
}

impl<R: Runtime> Serving<R> {
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    #[must_use]
    pub fn budget(&self, instance: &InstanceId) -> u32 {
        self.coordinator.table().budget(instance)
    }

    #[must_use]
    pub fn pool_count(&self) -> usize {
        self.pools.len()
    }
}

impl<R: Runtime> Drop for Serving<R> {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl<R: Runtime> std::fmt::Debug for Serving<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Serving")
            .field("addr", &self.addr)
            .field("pools", &self.pools)
            .finish_non_exhaustive()
    }
}

/// Runs the degenerate form of the system in one process: the coordinator
/// derives each instance's total budget and computes the grants, and the proxy
/// accepts clients and converges its pools to those grants.
pub async fn serve<R: Runtime>(
    rt: R,
    node: NodeConfig,
    cluster: ClusterConfig,
    options: ServeOptions,
) -> Result<Serving<R>, ServeError> {
    if cluster.cluster.pool_mode == PoolMode::Session {
        return Err(ServeError::SessionMode);
    }
    let monitor = node.monitor_credentials().ok_or(ServeError::NoMonitor)?;
    let policies = cluster.policies();
    let proxy = ProxyId::new(node.node.listen.to_string());
    let coordinator = Arc::new(InProcessCoordinator::new(
        proxy.clone(),
        WeightedMaxMinFair::default(),
    ));
    coordinator.set_policies(policies.clone());

    let mut tasks = Vec::new();
    let mut addresses = BTreeMap::new();
    for instance in &cluster.instance {
        let observer = Observer::start(
            rt.clone(),
            instance.id(),
            instance.address(),
            monitor.clone(),
            options,
            Arc::clone(&coordinator),
        )
        .await?;
        tasks.push(rt.spawn(observer.run()));
        addresses.insert(instance.id(), instance.address());
    }
    if let Err(error) = coordinator.reconcile() {
        tracing::error!(%error, "the first desired state was rejected by the allocation table");
    }

    let interval = cluster.cluster.arbitration_interval;
    let pools = Arc::new(NodePools::<R>::new());
    tasks.push(rt.spawn({
        let coordinator = Arc::clone(&coordinator);
        let rt = rt.clone();
        async move { coordinator.run(&rt, interval).await }
    }));
    tasks.push(rt.spawn({
        let pools = Arc::clone(&pools);
        let coordinator = Arc::clone(&coordinator);
        let rt = rt.clone();
        async move { pools.run(coordinator.as_ref(), &rt, interval).await }
    }));

    let listen = node.node.listen;
    let listener = rt
        .bind(&listen.to_string())
        .await
        .map_err(|source| ServeError::Listen {
            addr: listen,
            source,
        })?;
    let addr = listener.local_addr().map_err(|source| ServeError::Listen {
        addr: listen,
        source,
    })?;
    let front = Front {
        rt: rt.clone(),
        cancels: CancelRegistry::new(ProxyTag::of(&proxy)),
        limit: node.client_limit(),
        credentials: Arc::new(node.client_credentials()?),
        policies: Arc::new(policies),
        addresses: Arc::new(addresses),
        node: Arc::new(node),
        pools: Arc::clone(&pools),
        wait_timeout: options.wait_timeout,
    };
    tasks.push(rt.spawn(front.accept(listener)));

    Ok(Serving {
        addr,
        coordinator,
        pools,
        tasks,
    })
}

/// Keeps one instance's total budget up to date from what the instance reports.
///
/// Its own connection is one this system opened, so the foreign count leaves it
/// out; it is taken off the budget with the margin instead.
struct Observer<R: Runtime> {
    rt: R,
    instance: InstanceId,
    address: String,
    credentials: ServerCredentials,
    interval: Duration,
    budget: InstanceBudget,
    connection: Option<ServerConnection<R::Stream>>,
    coordinator: Arc<Coordinator>,
}

impl<R: Runtime> Observer<R> {
    async fn start(
        rt: R,
        instance: InstanceId,
        address: String,
        credentials: ServerCredentials,
        options: ServeOptions,
        coordinator: Arc<Coordinator>,
    ) -> Result<Self, ServeError> {
        let mut connection = connect(&rt, &address, &credentials, &ApplicationName::new(MONITOR))
            .await
            .map_err(|source| ServeError::Connect {
                instance: instance.clone(),
                source,
            })?;
        let inspect = |source| ServeError::Inspect {
            instance: instance.clone(),
            source,
        };
        let limits = read_server_limits(&mut connection).await.map_err(inspect)?;
        let foreign = count_foreign_connections(&mut connection)
            .await
            .map_err(inspect)?;
        let mut budget = InstanceBudget::new(
            limits,
            options.margin.saturating_add(1),
            options.foreign_window,
        );
        budget.observe(rt.now(), foreign);
        tracing::info!(%instance, budget = %budget.current(), "derived the total budget");
        coordinator.set_budget(instance.clone(), budget.current().total());
        Ok(Self {
            rt,
            instance,
            address,
            credentials,
            interval: options.observe_interval,
            budget,
            connection: Some(connection),
            coordinator,
        })
    }

    async fn run(mut self) {
        loop {
            self.rt.sleep(self.interval).await;
            match self.observe().await {
                Ok(foreign) => {
                    self.budget.observe(self.rt.now(), foreign);
                    self.coordinator
                        .set_budget(self.instance.clone(), self.budget.current().total());
                }
                Err(error) => {
                    self.connection = None;
                    tracing::error!(
                        instance = %self.instance,
                        %error,
                        "cannot count the foreign connections; the total budget stays where it was"
                    );
                }
            }
        }
    }

    async fn observe(&mut self) -> Result<u32, ServeError> {
        if self.connection.is_none() {
            let connection = connect(
                &self.rt,
                &self.address,
                &self.credentials,
                &ApplicationName::new(MONITOR),
            )
            .await
            .map_err(|source| ServeError::Connect {
                instance: self.instance.clone(),
                source,
            })?;
            self.connection = Some(connection);
        }
        let connection = self.connection.as_mut().expect("connected above");
        count_foreign_connections(connection)
            .await
            .map_err(|source| ServeError::Inspect {
                instance: self.instance.clone(),
                source,
            })
    }
}

struct Front<R: Runtime> {
    rt: R,
    cancels: CancelRegistry,
    limit: ClientLimit,
    credentials: Arc<ClientCredentials>,
    policies: Arc<Policies>,
    addresses: Arc<BTreeMap<InstanceId, String>>,
    node: Arc<NodeConfig>,
    pools: Arc<NodePools<R>>,
    wait_timeout: Duration,
}

impl<R: Runtime> Clone for Front<R> {
    fn clone(&self) -> Self {
        Self {
            rt: self.rt.clone(),
            cancels: self.cancels.clone(),
            limit: self.limit.clone(),
            credentials: Arc::clone(&self.credentials),
            policies: Arc::clone(&self.policies),
            addresses: Arc::clone(&self.addresses),
            node: Arc::clone(&self.node),
            pools: Arc::clone(&self.pools),
            wait_timeout: self.wait_timeout,
        }
    }
}

impl<R: Runtime> Front<R> {
    async fn accept(self, listener: R::Listener) {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let front = self.clone();
                    self.rt.spawn(front.serve_client(stream));
                }
                Err(error) => tracing::warn!(%error, "cannot accept a client connection"),
            }
        }
    }

    async fn serve_client(self, stream: R::Stream) {
        let (admitted, accepted) = match self.limit.accept(stream, self.credentials.as_ref()).await
        {
            Ok(accepted) => accepted,
            Err(error) => {
                tracing::info!(%error, "refused a client connection");
                return;
            }
        };
        let session = match accepted {
            Accepted::Session(session) => session,
            Accepted::Cancel(key) => {
                self.cancel(key).await;
                return;
            }
        };
        let tenant = session.tenant().clone();
        let Some(instance) = self
            .policies
            .route(&tenant, |total| rand::random_range(0..total))
            .cloned()
        else {
            refuse_unknown_tenant(session).await;
            return;
        };
        let (pool, welcome) = self
            .pools
            .checkout(&instance, &tenant, || self.open(&instance, &tenant));
        if let Err(error) =
            transaction_mode(session, &pool, &welcome, &self.cancels, &instance).await
        {
            tracing::warn!(%tenant, %instance, %error, "a client session ended with an error");
        }
        drop(admitted);
    }

    /// Stops what the client that carries `key` is running, if it is running
    /// anything right now.
    ///
    /// A key this node did not issue, and one whose client is between requests,
    /// are both dropped without a reply: a `CancelRequest` is unauthenticated,
    /// and the protocol gives it no answer either way.
    async fn cancel(&self, key: CancelKey) {
        let Some(target) = self.cancels.target(key) else {
            tracing::debug!("dropped a cancel request that names nothing this node is running");
            return;
        };
        let Some(address) = self.addresses.get(target.instance()) else {
            return;
        };
        if let Err(error) = forward_cancel(&self.rt, address, target.backend()).await {
            tracing::warn!(instance = %target.instance(), %error, "cannot forward a cancel request");
        }
    }

    fn open(&self, instance: &InstanceId, tenant: &TenantId) -> Pool<InstanceOpener<R>, R> {
        Pool::new(
            InstanceOpener::new(
                self.rt.clone(),
                self.addresses[instance].clone(),
                self.node.server_credentials(tenant),
                ApplicationName::new(PROXY),
            ),
            self.rt.clone(),
            PoolLimits {
                slots: 0,
                wait_timeout: self.wait_timeout,
            },
        )
    }
}

async fn refuse_unknown_tenant<S: AsyncRead + AsyncWrite + Unpin>(session: ClientSession<S>) {
    let tenant = session.tenant().clone();
    let (mut stream, _) = session.into_parts();
    let mut out = BytesMut::new();
    encode_error_response(
        &ErrorResponse::fatal(
            sqlstate::INVALID_AUTHORIZATION_SPECIFICATION,
            format!("no [tenant.\"…\"] rule covers {tenant}, and there is no [tenant.\"*\"]"),
        ),
        &mut out,
    );
    let _ = stream.write_all(&out).await;
    let _ = stream.shutdown().await;
}
