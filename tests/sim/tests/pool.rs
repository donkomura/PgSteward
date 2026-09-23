use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pgsteward_core::pool::{InstanceOpener, Pool, PoolError, PoolLimits};
use pgsteward_core::rt::{Clock, Spawner, turmoil_rt::TurmoilRuntime};
use pgsteward_core::server::{ApplicationName, ServerCredentials, SimpleQuery};
use pgsteward_harness::cap::{CapMonitor, CapReport};
use pgsteward_harness::fake_postgres::{FakePostgres, FakePostgresStats};
use pgsteward_harness::server_tls;

const IDENTIFIER: &str = "sim-pool";
const SLOTS: usize = 2;
const CLIENTS: usize = 8;
const POLL: Duration = Duration::from_millis(1);
const HOLD: Duration = Duration::from_millis(10);

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

fn pool(
    rt: TurmoilRuntime,
    limits: PoolLimits,
) -> Pool<InstanceOpener<TurmoilRuntime>, TurmoilRuntime> {
    Pool::new(
        InstanceOpener::new(
            rt,
            "db:5432".to_owned(),
            credentials(),
            ApplicationName::new(IDENTIFIER),
            server_tls(),
        ),
        rt,
        limits,
    )
}

#[test]
fn the_pool_serves_more_clients_than_it_has_slots_without_exceeding_them() {
    let stats = FakePostgresStats::default();
    let backends: Arc<Mutex<BTreeSet<String>>> = Arc::new(Mutex::new(BTreeSet::new()));
    let report: Arc<Mutex<Option<CapReport>>> = Arc::new(Mutex::new(None));
    let mut sim = turmoil::Builder::new().build();
    start_db(&mut sim, stats.clone());

    let observed = stats.clone();
    let served = Arc::clone(&backends);
    let capped = Arc::clone(&report);
    sim.client("app", async move {
        let rt = TurmoilRuntime::new();
        let monitor = CapMonitor::start(&rt, observed, SLOTS, POLL);
        let pool = pool(
            rt,
            PoolLimits {
                slots: SLOTS,
                wait_timeout: Duration::from_secs(5),
            },
        );

        let clients: Vec<_> = (0..CLIENTS)
            .map(|_| {
                let pool = pool.clone();
                rt.spawn(async move {
                    let mut assigned = pool.acquire().await.unwrap();
                    let rows = assigned
                        .simple_query("SELECT pg_backend_pid()")
                        .await
                        .unwrap();
                    rt.sleep(HOLD).await;
                    rows[0][0].clone().expect("a backend identifier")
                })
            })
            .collect();
        for client in clients {
            let backend = client.await.unwrap();
            served.lock().unwrap().insert(backend);
        }

        assert_eq!(pool.stats().actual(), SLOTS);
        assert_eq!(pool.stats().in_use, 0);
        *capped.lock().unwrap() = Some(monitor.stop().await);
        Ok(())
    });

    sim.run().unwrap();

    let report = report.lock().unwrap().take().expect("a cap report");
    report.assert_never_exceeded();
    assert_eq!(report.peak(), SLOTS);
    let backends = backends.lock().unwrap();
    assert!(
        backends.len() <= SLOTS,
        "{CLIENTS} clients ran on {} server connections: {backends:?}",
        backends.len()
    );
    assert_eq!(
        stats.accepted(),
        SLOTS,
        "the pool must open one server connection per slot and reuse them"
    );
}

#[test]
fn a_client_that_waits_longer_than_the_timeout_gives_up_its_place() {
    let stats = FakePostgresStats::default();
    let mut sim = turmoil::Builder::new().build();
    start_db(&mut sim, stats.clone());

    let opened = stats.clone();
    sim.client("app", async move {
        let rt = TurmoilRuntime::new();
        let pool = pool(
            rt,
            PoolLimits {
                slots: 1,
                wait_timeout: Duration::from_secs(1),
            },
        );

        let held = pool.acquire().await.unwrap();
        let waiting = rt.spawn({
            let pool = pool.clone();
            async move { pool.acquire().await.err() }
        });
        rt.sleep(Duration::from_secs(3)).await;
        let gave_up = waiting.await.unwrap();
        assert!(
            matches!(gave_up, Some(PoolError::WaitTimeout { .. })),
            "a client that outwaits the timeout must be told so: {gave_up:?}"
        );
        assert_eq!(pool.stats().waiting, 0);

        drop(held);
        let mut next = pool.acquire().await.unwrap();
        next.simple_query("SELECT pg_backend_pid()").await.unwrap();
        assert_eq!(
            opened.accepted(),
            1,
            "a client that gave up waiting must not have opened a server connection"
        );
        Ok(())
    });

    sim.run().unwrap();
    assert_eq!(stats.peak(), 1);
}

#[test]
fn a_lowered_grant_is_closed_on_the_instance_before_the_next_client_arrives() {
    let stats = FakePostgresStats::default();
    let report: Arc<Mutex<Option<CapReport>>> = Arc::new(Mutex::new(None));
    let mut sim = turmoil::Builder::new().build();
    start_db(&mut sim, stats.clone());

    let observed = stats.clone();
    let capped = Arc::clone(&report);
    sim.client("app", async move {
        let rt = TurmoilRuntime::new();
        let pool = pool(
            rt,
            PoolLimits {
                slots: SLOTS,
                wait_timeout: Duration::from_secs(5),
            },
        );

        let first = pool.acquire().await.unwrap();
        let second = pool.acquire().await.unwrap();
        drop(first);
        drop(second);
        assert_eq!(observed.live(), SLOTS);

        let closed = pool.converge(1).await;

        assert_eq!(closed, 1);
        assert_eq!(observed.live(), 1);

        let monitor = CapMonitor::start(&rt, observed.clone(), 1, POLL);
        let clients: Vec<_> = (0..CLIENTS)
            .map(|_| {
                let pool = pool.clone();
                rt.spawn(async move {
                    let mut assigned = pool.acquire().await.unwrap();
                    assigned
                        .simple_query("SELECT pg_backend_pid()")
                        .await
                        .unwrap();
                    rt.sleep(HOLD).await;
                })
            })
            .collect();
        for client in clients {
            client.await.unwrap();
        }

        assert_eq!(pool.stats().actual(), 1);
        *capped.lock().unwrap() = Some(monitor.stop().await);
        Ok(())
    });

    sim.run().unwrap();

    let report = report.lock().unwrap().take().expect("a cap report");
    report.assert_never_exceeded();
    assert_eq!(report.peak(), 1);
    assert_eq!(
        stats.accepted(),
        SLOTS,
        "the clients after the convergence must share the connection that was kept"
    );
}
