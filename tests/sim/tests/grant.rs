use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pgsteward_core::allocation::{InstanceId, ProxyId};
use pgsteward_core::convergence::ProxyPools;
use pgsteward_core::grant::{InProcessCoordinator, TenantPolicy};
use pgsteward_core::policy::{Policies, TenantRule};
use pgsteward_core::pool::{InstanceOpener, Pool, PoolLimits};
use pgsteward_core::rt::{Clock, Spawner, turmoil_rt::TurmoilRuntime};
use pgsteward_core::server::{ApplicationName, ServerCredentials, SimpleQuery};
use pgsteward_core::tenant::TenantId;
use pgsteward_harness::cap::{CapMonitor, CapReport};
use pgsteward_harness::fake_postgres::{FakePostgres, FakePostgresStats};
use pgsteward_sched::fair::WeightedMaxMinFair;

const IDENTIFIER: &str = "sim-grant";
const BUDGET: u32 = 2;
const TENANTS: [&str; 3] = ["alice", "bob", "carol"];
const CLIENTS_PER_TENANT: usize = 4;
const INTERVAL: Duration = Duration::from_millis(5);
const POLL: Duration = Duration::from_millis(1);
const HOLD: Duration = Duration::from_millis(10);
const SEEDS: u64 = 20;

fn primary() -> InstanceId {
    InstanceId::new("primary")
}

fn tenant(user: &str) -> TenantId {
    TenantId::new(user, user)
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

fn pool(rt: TurmoilRuntime) -> Pool<InstanceOpener<TurmoilRuntime>, TurmoilRuntime> {
    Pool::new(
        InstanceOpener::new(
            rt,
            "db:5432".to_owned(),
            ServerCredentials {
                user: "postgres".to_owned(),
                database: "postgres".to_owned(),
                password: None,
            },
            ApplicationName::new(IDENTIFIER),
        ),
        rt,
        PoolLimits {
            slots: 0,
            wait_timeout: Duration::from_secs(5),
        },
    )
}

#[test]
fn tenants_competing_for_a_budget_are_all_served_without_exceeding_it() {
    for seed in 0..SEEDS {
        compete(seed);
    }
}

fn compete(seed: u64) {
    let stats = FakePostgresStats::default();
    let report: Arc<Mutex<Option<CapReport>>> = Arc::new(Mutex::new(None));
    let mut sim = turmoil::Builder::new().rng_seed(seed).build();
    start_db(&mut sim, stats.clone());

    let observed = stats.clone();
    let capped = Arc::clone(&report);
    sim.client("proxy", async move {
        let rt = TurmoilRuntime::new();
        let monitor = CapMonitor::start(&rt, observed, BUDGET as usize, POLL);
        let coordinator = Arc::new(InProcessCoordinator::new(
            ProxyId::new("proxy-1"),
            WeightedMaxMinFair::default(),
        ));
        coordinator.set_budget(primary(), BUDGET);
        coordinator.set_policies(
            Policies::new()
                .instance(primary(), NonZeroU32::new(1).unwrap())
                .tenant(
                    "*",
                    TenantRule {
                        instances: vec![primary()],
                        policy: TenantPolicy {
                            min: 0,
                            max: u32::MAX,
                            weight: NonZeroU32::new(1).unwrap(),
                        },
                    },
                ),
        );
        let pools = Arc::new(ProxyPools::new());
        let coordinating = rt.spawn({
            let coordinator = Arc::clone(&coordinator);
            async move { coordinator.run(&rt, INTERVAL).await }
        });
        let converging = rt.spawn({
            let pools = Arc::clone(&pools);
            let coordinator = Arc::clone(&coordinator);
            async move { pools.run(coordinator.as_ref(), &rt, INTERVAL).await }
        });

        let clients: Vec<_> = TENANTS
            .iter()
            .flat_map(|user| {
                let pools = Arc::clone(&pools);
                (0..CLIENTS_PER_TENANT).map(move |_| {
                    let pools = Arc::clone(&pools);
                    rt.spawn(async move {
                        let (pool, _) = pools.checkout(&primary(), &tenant(user), || pool(rt));
                        let mut assigned = pool.acquire().await.unwrap();
                        assigned
                            .simple_query("SELECT pg_backend_pid()")
                            .await
                            .unwrap();
                        rt.sleep(HOLD).await;
                    })
                })
            })
            .collect();
        for client in clients {
            client.await.unwrap();
        }
        rt.sleep(Duration::from_secs(1)).await;
        assert_eq!(
            pools.len(),
            0,
            "a tenant whose clients are gone keeps no pool"
        );

        coordinating.abort();
        converging.abort();
        *capped.lock().unwrap() = Some(monitor.stop().await);
        Ok(())
    });

    sim.run().unwrap();

    let report = report.lock().unwrap().take().expect("a cap report");
    report.assert_never_exceeded();
    assert!(stats.peak() <= BUDGET as usize, "seed {seed}");
}
