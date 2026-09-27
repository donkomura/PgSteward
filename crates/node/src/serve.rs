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
use pgsteward_core::console::{
    ConsoleNode, ConsoleView, InstanceSnapshot, PoolSnapshot, is_console, serve_console,
};
use pgsteward_core::convergence::ProxyPools;
use pgsteward_core::grant::InProcessCoordinator;
use pgsteward_core::inspect::{InspectError, count_foreign_connections, read_server_limits};
use pgsteward_core::policy::{PolicyChange, SettingError};
use pgsteward_core::pool::{InstanceOpener, Pool, PoolLimits};
use pgsteward_core::relay::transaction_mode;
use pgsteward_core::rt::{JoinHandle, Listener, Runtime};
use pgsteward_core::server::{
    ApplicationName, ConnectError, ServerConnection, ServerCredentials, connect,
};
use pgsteward_core::session::{Accepted, ClientSession};
use pgsteward_core::tenant::TenantId;
use pgsteward_core::tls::{ClientTls, MaybeTls, ServerTls};
use pgsteward_protocol::backend::{ErrorResponse, encode_error_response, sqlstate};
use pgsteward_protocol::startup::CancelKey;
use pgsteward_sched::fair::WeightedMaxMinFair;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::config::{ClusterConfig, ConfigError, NodeConfig, PoolMode};
use crate::metrics::serve_scrapes;

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
    pub shutdown_grace: Duration,
    pub release_delay: Duration,
}

impl Default for ServeOptions {
    fn default() -> Self {
        Self {
            margin: 15,
            foreign_window: Duration::from_secs(60),
            observe_interval: Duration::from_secs(1),
            wait_timeout: Duration::from_secs(30),
            shutdown_grace: Duration::from_secs(15),
            release_delay: Duration::from_secs(60),
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

/// What stopping a node left behind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stopped {
    /// The server connections the node closed on its way out.
    pub closed: usize,
    /// The server connections a client was still using when the grace ended.
    pub held: usize,
}

/// A node that is serving. Dropping it stops every task it started.
pub struct Serving<R: Runtime> {
    rt: R,
    addr: SocketAddr,
    metrics_addr: Option<SocketAddr>,
    limit: ClientLimit,
    grace: Duration,
    coordinator: Arc<Coordinator>,
    pools: Arc<NodePools<R>>,
    accepting: JoinHandle<()>,
    tasks: Vec<JoinHandle<()>>,
}

impl<R: Runtime> Serving<R> {
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Where a Prometheus scrape reads this node, if it publishes metrics.
    #[must_use]
    pub fn metrics_addr(&self) -> Option<SocketAddr> {
        self.metrics_addr
    }

    #[must_use]
    pub fn budget(&self, instance: &InstanceId) -> u32 {
        self.coordinator.table().budget(instance)
    }

    #[must_use]
    pub fn pool_count(&self) -> usize {
        self.pools.len()
    }

    /// How many slots this node is granted on `instance` right now.
    #[must_use]
    pub fn granted(&self, instance: &InstanceId) -> u32 {
        self.coordinator.table().granted_total(instance)
    }

    /// Stops this node: it takes no further client, waits for the sessions it
    /// still holds until the grace period ends, closes the server connections
    /// that came back, and gives its grants back once the instances hold
    /// nothing of its.
    ///
    /// A transaction that is still running when the grace ends is not cut off,
    /// and the slot behind the connection it holds is not given back. A grant
    /// handed back is a slot another holder may open into, so giving one back
    /// while this node still holds its connection would put the instance over
    /// its total budget.
    ///
    /// The control loops stop first: a node on its way out asks for nothing
    /// more, and every connection it closes from here is closed by the stop
    /// itself, in one place.
    pub async fn shutdown(&self) -> Stopped {
        self.accepting.abort();
        for task in &self.tasks {
            task.abort();
        }
        tracing::info!("stopping: this node takes no further client");
        tokio::select! {
            () = self.limit.drained() => {
                tracing::info!("stopping: every client session has ended");
            }
            () = self.rt.sleep(self.grace) => {
                tracing::info!(
                    clients = self.limit.live(),
                    "stopping: the grace ended with clients still being served"
                );
            }
        }
        let closed = self.pools.drain().await;
        let held = self.pools.occupied();
        if held == 0 {
            self.coordinator.withdraw();
            tracing::info!(closed, "stopped: every grant is back with its instance");
        } else {
            tracing::warn!(
                closed,
                held,
                "stopped: the grants of the connections still in use are left to expire"
            );
        }
        Stopped { closed, held }
    }
}

impl<R: Runtime> Drop for Serving<R> {
    fn drop(&mut self) {
        self.accepting.abort();
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
    let server_tls = Arc::new(node.server_tls()?);
    let policies = cluster.policies();
    let proxy = ProxyId::new(node.node.listen.to_string());
    let coordinator = Arc::new(
        InProcessCoordinator::new(proxy.clone(), WeightedMaxMinFair::default())
            .with_release_delay(options.release_delay),
    );
    coordinator.set_policies(policies.clone());

    let mut tasks = Vec::new();
    let mut addresses = BTreeMap::new();
    for instance in &cluster.instance {
        let observer = Observer::start(
            rt.clone(),
            instance.id(),
            instance.address(),
            monitor.clone(),
            Arc::clone(&server_tls),
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
    let node_metrics_listen = node.node.metrics_listen;
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
    let limit = node.client_limit();
    let front = Front {
        rt: rt.clone(),
        proxy: proxy.clone(),
        pool_mode: cluster.cluster.pool_mode.as_str(),
        cancels: CancelRegistry::new(ProxyTag::of(&proxy)),
        limit: limit.clone(),
        credentials: Arc::new(node.client_credentials()?),
        tls: node.client_tls()?.map(Arc::new),
        server_tls,
        addresses: Arc::new(addresses),
        node: Arc::new(node),
        pools: Arc::clone(&pools),
        coordinator: Arc::clone(&coordinator),
        wait_timeout: options.wait_timeout,
    };
    let metrics_addr = match node_metrics_listen {
        Some(listen) => Some(publish_metrics(&rt, listen, front.clone(), &mut tasks).await?),
        None => None,
    };
    let accepting = rt.spawn(front.accept(listener));

    Ok(Serving {
        rt,
        addr,
        metrics_addr,
        limit,
        grace: options.shutdown_grace,
        coordinator,
        pools,
        accepting,
        tasks,
    })
}

/// Opens the listener a Prometheus scrape reads this node on.
///
/// It is a listener of its own rather than a path on the one clients connect
/// to: what the metrics reach is a deployment's choice, and a scraper has no
/// business speaking the wire protocol to get them.
async fn publish_metrics<R: Runtime>(
    rt: &R,
    listen: SocketAddr,
    front: Front<R>,
    tasks: &mut Vec<JoinHandle<()>>,
) -> Result<SocketAddr, ServeError> {
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
    tasks.push(rt.spawn(serve_scrapes(rt.clone(), listener, front)));
    tracing::info!(%addr, "publishing metrics");
    Ok(addr)
}

/// Counts what each instance reports, so that the coordinator can keep the
/// total budget it derived up to date.
struct Observer<R: Runtime> {
    rt: R,
    instance: InstanceId,
    address: String,
    credentials: ServerCredentials,
    tls: Arc<ServerTls>,
    interval: Duration,
    connection: Option<ServerConnection<MaybeTls<R::Stream>>>,
    coordinator: Arc<Coordinator>,
}

impl<R: Runtime> Observer<R> {
    async fn start(
        rt: R,
        instance: InstanceId,
        address: String,
        credentials: ServerCredentials,
        tls: Arc<ServerTls>,
        options: ServeOptions,
        coordinator: Arc<Coordinator>,
    ) -> Result<Self, ServeError> {
        let mut connection = connect(
            &rt,
            &address,
            &credentials,
            &ApplicationName::new(MONITOR),
            &tls,
        )
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
        let mut budget = InstanceBudget::new(limits, options.margin, options.foreign_window);
        budget.observe(rt.now(), including_the_observer(foreign));
        tracing::info!(%instance, budget = %budget.current(), "derived the total budget");
        coordinator.add_instance(instance.clone(), budget);
        Ok(Self {
            rt,
            instance,
            address,
            credentials,
            tls,
            interval: options.observe_interval,
            connection: Some(connection),
            coordinator,
        })
    }

    async fn run(mut self) {
        loop {
            self.rt.sleep(self.interval).await;
            match self.observe().await {
                Ok(foreign) => {
                    self.coordinator.observe_instance(
                        &self.instance,
                        self.rt.now(),
                        including_the_observer(foreign),
                    );
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
                &self.tls,
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

/// The observer's own connection carries this system's application name, so
/// the foreign count leaves it out. No pool holds it either, so it is counted
/// here rather than taken off the budget with the margin: the margin is a
/// setting an operator writes and reads back, and it would no longer be the
/// number they wrote.
fn including_the_observer(foreign: u32) -> u32 {
    foreign.saturating_add(1)
}

struct Front<R: Runtime> {
    rt: R,
    proxy: ProxyId,
    pool_mode: &'static str,
    cancels: CancelRegistry,
    limit: ClientLimit,
    credentials: Arc<ClientCredentials>,
    tls: Option<Arc<ClientTls>>,
    server_tls: Arc<ServerTls>,
    addresses: Arc<BTreeMap<InstanceId, String>>,
    node: Arc<NodeConfig>,
    pools: Arc<NodePools<R>>,
    coordinator: Arc<Coordinator>,
    wait_timeout: Duration,
}

impl<R: Runtime> Clone for Front<R> {
    fn clone(&self) -> Self {
        Self {
            rt: self.rt.clone(),
            proxy: self.proxy.clone(),
            pool_mode: self.pool_mode,
            cancels: self.cancels.clone(),
            limit: self.limit.clone(),
            credentials: Arc::clone(&self.credentials),
            tls: self.tls.clone(),
            server_tls: Arc::clone(&self.server_tls),
            addresses: Arc::clone(&self.addresses),
            node: Arc::clone(&self.node),
            pools: Arc::clone(&self.pools),
            coordinator: Arc::clone(&self.coordinator),
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
        let (admitted, accepted) = match self
            .limit
            .accept(stream, self.credentials.as_ref(), self.tls.as_deref())
            .await
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
        if is_console(&tenant) {
            let console = self.clone();
            if let Err(error) = serve_console(session, console).await {
                tracing::warn!(%tenant, %error, "an admin console session ended with an error");
            }
            drop(admitted);
            return;
        }
        let Some(instance) = self
            .coordinator
            .policies()
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
                Arc::clone(&self.server_tls),
            ),
            self.rt.clone(),
            PoolLimits {
                slots: 0,
                wait_timeout: self.wait_timeout,
            },
        )
    }
}

/// The console reads this node through the coordinator, which holds the
/// cluster configuration this stage allocates against, and writes settings
/// back to the same place.
impl<R: Runtime> ConsoleNode for Front<R> {
    fn view(&self) -> ConsoleView {
        let policies = self.coordinator.policies();
        ConsoleView {
            proxies: vec![self.proxy.clone()],
            pool_mode: self.pool_mode.to_owned(),
            pools: self
                .pools
                .stats()
                .into_iter()
                .map(|(instance, tenant, stats)| PoolSnapshot {
                    proxy: self.proxy.clone(),
                    policy: policies.policy(&instance, &tenant),
                    instance,
                    tenant,
                    stats,
                })
                .collect(),
            instances: self
                .coordinator
                .instances()
                .into_iter()
                .map(|(instance, budget)| InstanceSnapshot { instance, budget })
                .collect(),
            table: self.coordinator.table(),
        }
    }

    fn set_tenant(&self, tenant: &str, change: PolicyChange) -> Result<(), SettingError> {
        self.coordinator.set_tenant(tenant, change)
    }

    async fn set_instance(&self, instance: &str, margin: u32) -> Result<(), SettingError> {
        self.coordinator
            .set_margin(&InstanceId::new(instance), margin)
            .await
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
