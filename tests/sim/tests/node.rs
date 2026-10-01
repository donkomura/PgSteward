use std::sync::{Arc, Mutex};
use std::time::Duration;

use pgsteward_core::allocation::InstanceId;
use pgsteward_core::budget::ServerLimits;
use pgsteward_core::rt::{Clock, Net, Spawner, turmoil_rt::TurmoilRuntime};
use pgsteward_harness::cap::{CapMonitor, CapReport};
use pgsteward_harness::fake_postgres::{FakePostgres, FakePostgresStats};
use pgsteward_harness::wire_client::WireClient;
use pgsteward_node::config::{ClusterConfig, NodeConfig};
use pgsteward_node::serve::{ServeOptions, serve};

const PENCIL_VERIFIER: &str = "SCRAM-SHA-256$4096:W22ZaJ0SNY7soEsUEjb6gQ==$\
WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=:wfPLwcE6nTWhTAmQ7tl2KeoiWGPlZqQxSrmfPwDl2dU=";
const POLL: Duration = Duration::from_millis(1);
const SEEDS: u64 = 10;
const CLIENTS: usize = 20;
const TRANSACTIONS: usize = 5;
const LIMITS: ServerLimits = ServerLimits {
    max_connections: 12,
    superuser_reserved_connections: 3,
    reserved_connections: 0,
};
const OBSERVER: usize = 1;
const BUDGET: u32 = 8;

fn node_config() -> NodeConfig {
    NodeConfig::parse(&format!(
        r#"
[node]
role = "proxy"
listen = "0.0.0.0:6432"
coordinator = "coordinator:7432"
max_client_connections = 100

[monitor]
user = "postgres"

[client."alice"]
verifier = "{PENCIL_VERIFIER}"
"#
    ))
    .unwrap()
}

fn cluster_config() -> ClusterConfig {
    ClusterConfig::parse(
        r#"
[cluster]
pool_mode = "transaction"
grant_ttl = "10s"
arbitration_interval = "10ms"

[[instance]]
name = "db"

[tenant."*"]
instances = ["db"]
"#,
    )
    .unwrap()
}

fn options() -> ServeOptions {
    ServeOptions {
        margin: 0,
        observe_interval: Duration::from_millis(100),
        ..ServeOptions::default()
    }
}

fn start_db(sim: &mut turmoil::Sim<'_>, stats: FakePostgresStats) {
    sim.host("db", move || {
        let stats = stats.clone();
        async move {
            let rt = TurmoilRuntime::new();
            FakePostgres::start_with_limits(&rt, "0.0.0.0:5432", stats, LIMITS).await?;
            std::future::pending::<()>().await;
            Ok(())
        }
    });
}

fn start_node(sim: &mut turmoil::Sim<'_>, budget: Arc<Mutex<Option<u32>>>) {
    sim.host("node", move || {
        let budget = Arc::clone(&budget);
        async move {
            let rt = TurmoilRuntime::new();
            let serving = serve(rt, node_config(), cluster_config(), options()).await?;
            *budget.lock().unwrap() = Some(serving.budget(&InstanceId::new("db")));
            std::future::pending::<()>().await;
            drop(serving);
            Ok(())
        }
    });
}

/// The node listens only once it has derived the total budget, so a client
/// that starts with it is refused until then.
async fn connect_when_listening(
    rt: TurmoilRuntime,
) -> turmoil::Result<<TurmoilRuntime as Net>::Stream> {
    loop {
        match rt.connect("node:6432").await {
            Ok(stream) => return Ok(stream),
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
                rt.sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

async fn run_transactions(rt: TurmoilRuntime) -> turmoil::Result {
    let stream = connect_when_listening(rt).await?;
    let mut client = WireClient::login(stream, "alice", "postgres", Some("pencil")).await?;
    for _ in 0..TRANSACTIONS {
        assert_eq!(client.query("BEGIN").await?.1, b'T');
        assert_eq!(client.query("SELECT 1").await?.1, b'T');
        assert_eq!(client.query("COMMIT").await?.1, b'I');
    }
    Ok(())
}

#[test]
fn a_node_on_turmoil_serves_its_clients_within_the_budget_it_derives() {
    for seed in 0..SEEDS {
        serves_within_the_budget(seed);
    }
}

fn serves_within_the_budget(seed: u64) {
    let stats = FakePostgresStats::default();
    let budget: Arc<Mutex<Option<u32>>> = Arc::new(Mutex::new(None));
    let report: Arc<Mutex<Option<CapReport>>> = Arc::new(Mutex::new(None));
    let mut sim = turmoil::Builder::new()
        .rng_seed(seed)
        .simulation_duration(Duration::from_secs(60))
        .build();
    start_db(&mut sim, stats.clone());
    start_node(&mut sim, Arc::clone(&budget));

    let observed = stats.clone();
    let capped = Arc::clone(&report);
    sim.client("app", async move {
        let rt = TurmoilRuntime::new();
        let monitor = CapMonitor::start(&rt, observed, BUDGET as usize + OBSERVER, POLL);
        let clients: Vec<_> = (0..CLIENTS)
            .map(|_| {
                rt.spawn(async move {
                    run_transactions(rt)
                        .await
                        .map_err(|error| error.to_string())
                })
            })
            .collect();
        for client in clients {
            client.await??;
        }
        *capped.lock().unwrap() = Some(monitor.stop().await);
        Ok(())
    });

    sim.run().unwrap();

    assert_eq!(
        *budget.lock().unwrap(),
        Some(BUDGET),
        "seed {seed}: the node must derive the total budget from what the fake PostgreSQL shows"
    );
    let report = report.lock().unwrap().take().expect("a cap report");
    report.assert_never_exceeded();
    assert!(
        report.peak() > OBSERVER,
        "seed {seed}: the clients must have been served over server connections"
    );
}
