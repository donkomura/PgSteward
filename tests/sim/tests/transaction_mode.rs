use std::sync::{Arc, Mutex};
use std::time::Duration;

use pgsteward_core::allocation::InstanceId;
use pgsteward_core::auth::TrustAll;
use pgsteward_core::cancel::{CancelRegistry, ProxyTag};
use pgsteward_core::pool::{InstanceOpener, Pool, PoolLimits};
use pgsteward_core::relay::{Welcome, transaction_mode};
use pgsteward_core::rt::{Clock, Net, Spawner, turmoil_rt::TurmoilRuntime};
use pgsteward_core::server::{ApplicationName, ServerCredentials};
use pgsteward_core::session::{Accepted, accept};
use pgsteward_harness::cap::{CapMonitor, CapReport};
use pgsteward_harness::fake_postgres::{FakePostgres, FakePostgresStats};
use pgsteward_harness::server_tls;
use pgsteward_harness::wire_client::WireClient;

const IDENTIFIER: &str = "sim-transaction-mode";
const POLL: Duration = Duration::from_millis(1);
const SEEDS: u64 = 40;
const TOO_MANY_CONNECTIONS: &str = pgsteward_protocol::backend::sqlstate::TOO_MANY_CONNECTIONS;

fn credentials() -> ServerCredentials {
    ServerCredentials {
        user: "postgres".to_owned(),
        database: "postgres".to_owned(),
        password: None,
    }
}

fn start_db(sim: &mut turmoil::Sim<'_>, stats: FakePostgresStats) {
    sim.host("db", move || {
        let stats = stats.clone();
        async move {
            let rt = TurmoilRuntime::new();
            FakePostgres::start(&rt, "0.0.0.0:5432", stats).await?;
            std::future::pending::<()>().await;
            Ok(())
        }
    });
}

fn start_proxy(sim: &mut turmoil::Sim<'_>, slots: usize, wait_timeout: Duration) {
    sim.host("proxy", move || async move {
        let rt = TurmoilRuntime::new();
        let pool = Pool::new(
            InstanceOpener::new(
                rt,
                "db:5432".to_owned(),
                credentials(),
                ApplicationName::new(IDENTIFIER),
                server_tls(),
            ),
            rt,
            PoolLimits {
                slots,
                wait_timeout,
            },
        );
        let welcome = Welcome::default();
        let cancels = CancelRegistry::new(ProxyTag::new(1).unwrap());
        let instance = InstanceId::new("postgres");
        let listener = rt.bind("0.0.0.0:6432").await?;
        loop {
            let (stream, _) = listener.accept().await?;
            let pool = pool.clone();
            let welcome = welcome.clone();
            let cancels = cancels.clone();
            let instance = instance.clone();
            rt.spawn(async move {
                let Ok(Accepted::Session(session)) = accept(stream, TrustAll, None).await else {
                    return;
                };
                let _ = transaction_mode(session, &pool, &welcome, &cancels, &instance).await;
            });
        }
    });
}

async fn connect(rt: &TurmoilRuntime) -> turmoil::Result<WireClientOverTurmoil> {
    let stream = rt.connect("proxy:6432").await?;
    Ok(WireClient::login(stream, "app_web", "shop", None).await?)
}

type WireClientOverTurmoil = WireClient<<TurmoilRuntime as Net>::Stream>;

#[test]
fn two_clients_are_served_by_one_server_connection() {
    let stats = FakePostgresStats::default();
    let report: Arc<Mutex<Option<CapReport>>> = Arc::new(Mutex::new(None));
    let mut sim = turmoil::Builder::new().build();
    start_db(&mut sim, stats.clone());
    start_proxy(&mut sim, 1, Duration::from_secs(5));

    let observed = stats.clone();
    let capped = Arc::clone(&report);
    sim.client("app", async move {
        let rt = TurmoilRuntime::new();
        let monitor = CapMonitor::start(&rt, observed, 1, POLL);

        let stream = rt.connect("proxy:6432").await?;
        let mut first = WireClient::login(stream, "app_web", "shop", None).await?;
        let first_key = first.cancel_key();
        assert!(
            first
                .parameters()
                .iter()
                .any(|(name, _)| name == "server_version"),
            "the greeting must carry the server parameters: {:?}",
            first.parameters()
        );
        let (rows, ready) = first.query("SELECT 1").await?;
        assert_eq!(ready, b'I');
        let backend = rows[0].clone();

        let stream = rt.connect("proxy:6432").await?;
        let mut second = WireClient::login(stream, "app_web", "shop", None).await?;
        let second_key = second.cancel_key();
        let (rows, ready) = second.query("SELECT 1").await?;
        assert_eq!(ready, b'I');
        assert_eq!(
            rows[0], backend,
            "the second client must run on the server connection the first one gave back"
        );
        assert_ne!(
            first_key, second_key,
            "each client must get a cancel key of its own"
        );

        *capped.lock().unwrap() = Some(monitor.stop().await);
        Ok(())
    });

    sim.run().unwrap();

    report
        .lock()
        .unwrap()
        .take()
        .expect("a cap report")
        .assert_never_exceeded();
    assert_eq!(
        stats.accepted(),
        1,
        "two clients on one slot must open one server connection"
    );
}

#[test]
fn an_open_transaction_keeps_the_slot_from_the_next_client() {
    let stats = FakePostgresStats::default();
    let mut sim = turmoil::Builder::new().build();
    start_db(&mut sim, stats.clone());
    start_proxy(&mut sim, 1, Duration::from_secs(30));

    let opened = stats.clone();
    sim.client("app", async move {
        let rt = TurmoilRuntime::new();
        let mut holder = connect(&rt).await?;
        assert_eq!(holder.query("BEGIN").await?.1, b'T');

        let mut waiting = connect(&rt).await?;
        waiting.send_query("SELECT 1").await?;
        tokio::select! {
            frame = waiting.read_frame() => panic!("served while a transaction was open: {frame:?}"),
            () = rt.sleep(Duration::from_secs(1)) => {}
        }

        assert_eq!(holder.query("COMMIT").await?.1, b'I');
        assert_eq!(waiting.read_result().await?.1, b'I');
        assert_eq!(
            opened.accepted(),
            1,
            "waiting for the transaction must not open a second server connection"
        );
        Ok(())
    });

    sim.run().unwrap();
    assert_eq!(stats.peak(), 1);
}

#[test]
fn a_client_that_leaves_mid_transaction_does_not_hand_its_server_connection_on() {
    for seed in 0..SEEDS {
        leaves_mid_transaction(seed);
    }
}

fn leaves_mid_transaction(seed: u64) {
    let stats = FakePostgresStats::default();
    let report: Arc<Mutex<Option<CapReport>>> = Arc::new(Mutex::new(None));
    let mut sim = turmoil::Builder::new().rng_seed(seed).build();
    start_db(&mut sim, stats.clone());
    start_proxy(&mut sim, 1, Duration::from_secs(5));

    let observed = stats.clone();
    let capped = Arc::clone(&report);
    sim.client("app", async move {
        let rt = TurmoilRuntime::new();
        let monitor = CapMonitor::start(&rt, observed, 1, POLL);

        let mut holder = connect(&rt).await?;
        assert_eq!(holder.query("BEGIN").await?.1, b'T');
        drop(holder);

        let mut next = connect(&rt).await?;
        assert_eq!(next.query("SELECT 1").await?.1, b'I');

        *capped.lock().unwrap() = Some(monitor.stop().await);
        Ok(())
    });

    sim.run().unwrap();

    report
        .lock()
        .unwrap()
        .take()
        .expect("a cap report")
        .assert_never_exceeded();
    assert_eq!(
        stats.accepted(),
        2,
        "a connection left mid-transaction must be closed, not handed to the next client"
    );
}

#[test]
fn a_client_that_outwaits_the_wait_timeout_is_told_why() {
    let stats = FakePostgresStats::default();
    let mut sim = turmoil::Builder::new().build();
    start_db(&mut sim, stats.clone());
    start_proxy(&mut sim, 1, Duration::from_millis(500));

    sim.client("app", async move {
        let rt = TurmoilRuntime::new();
        let mut holder = connect(&rt).await?;
        assert_eq!(holder.query("BEGIN").await?.1, b'T');

        let mut waiting = connect(&rt).await?;
        waiting.send_query("SELECT 1").await?;
        let refusal = waiting.read_frame().await?;

        assert_eq!(refusal.tag, b'E');
        assert!(
            refusal
                .body
                .windows(TOO_MANY_CONNECTIONS.len())
                .any(|field| field == TOO_MANY_CONNECTIONS.as_bytes()),
            "the refusal must carry the too_many_connections SQLSTATE"
        );
        Ok(())
    });

    sim.run().unwrap();
    assert_eq!(stats.accepted(), 1);
}
