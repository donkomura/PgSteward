use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use pgsteward_core::allocation::{InstanceId, ProxyId};
use pgsteward_core::convergence::ProxyPools;
use pgsteward_core::grant::{GrantChannel, GrantSet, InProcessCoordinator, TenantPolicy};
use pgsteward_core::pool::{CloseServer, OpenServer, Pool, PoolLimits, PoolStats};
use pgsteward_core::rt::{Clock, tokio_rt::TokioRuntime};
use pgsteward_core::server::ConnectError;
use pgsteward_core::tenant::TenantId;
use pgsteward_sched::fair::WeightedMaxMinFair;

const WAIT_TIMEOUT: Duration = Duration::from_secs(1);
const INTERVAL: Duration = Duration::from_millis(5);

#[derive(Debug)]
struct FakeConnection {
    live: Arc<AtomicUsize>,
}

impl Drop for FakeConnection {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

impl CloseServer for FakeConnection {
    async fn close(self) {}
}

#[derive(Debug, Clone, Default)]
struct Opener {
    live: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    delay: Duration,
}

impl Opener {
    fn slow(delay: Duration) -> Self {
        Self {
            delay,
            ..Self::default()
        }
    }

    fn live(&self) -> usize {
        self.live.load(Ordering::SeqCst)
    }

    fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }
}

impl OpenServer for Opener {
    type Connection = FakeConnection;

    async fn open(&self) -> Result<FakeConnection, ConnectError> {
        if !self.delay.is_zero() {
            TokioRuntime::new().sleep(self.delay).await;
        }
        let live = self.live.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(live, Ordering::SeqCst);
        Ok(FakeConnection {
            live: Arc::clone(&self.live),
        })
    }
}

fn primary() -> InstanceId {
    InstanceId::new("primary")
}

fn tenant(user: &str) -> TenantId {
    TenantId::new(user, user)
}

fn pool(opener: Opener) -> Pool<Opener, TokioRuntime> {
    Pool::new(
        opener,
        TokioRuntime::new(),
        PoolLimits {
            slots: 0,
            wait_timeout: WAIT_TIMEOUT,
        },
    )
}

fn coordinator(budget: u32, tenants: &[&str]) -> Arc<InProcessCoordinator<WeightedMaxMinFair>> {
    let coordinator =
        InProcessCoordinator::new(ProxyId::new("proxy-1"), WeightedMaxMinFair::default());
    coordinator.set_budget(primary(), budget);
    for user in tenants {
        coordinator.set_policy(
            primary(),
            tenant(user),
            TenantPolicy {
                min: 0,
                max: u32::MAX,
                weight: NonZeroU32::new(1).unwrap(),
            },
        );
    }
    Arc::new(coordinator)
}

async fn settle() {
    TokioRuntime::new().sleep(Duration::from_millis(1)).await;
}

#[test]
fn demand_is_the_waiting_clients_and_the_connections_in_use() {
    let stats = PoolStats {
        slots: 2,
        idle: 1,
        in_use: 2,
        opening: 1,
        closing: 1,
        waiting: 3,
    };

    assert_eq!(stats.demand(), 6);
}

#[test]
fn occupied_counts_every_connection_the_instance_may_hold() {
    let stats = PoolStats {
        slots: 2,
        idle: 1,
        in_use: 2,
        opening: 1,
        closing: 1,
        waiting: 3,
    };

    assert_eq!(stats.occupied(), 5);
}

#[tokio::test(start_paused = true)]
async fn a_pool_without_a_grant_opens_nothing_and_reports_its_waiting_client() {
    let opener = Opener::default();
    let pools = ProxyPools::new();
    pools.insert(primary(), tenant("alice"), pool(opener.clone()));
    let waiting = tokio::spawn({
        let pool = pools.get(&primary(), &tenant("alice")).unwrap();
        async move { pool.acquire().await }
    });
    settle().await;

    let report = pools.report(7);

    assert_eq!(report.generation(), 7);
    assert_eq!(report.get(&primary(), &tenant("alice")).demand, 1);
    assert_eq!(report.get(&primary(), &tenant("alice")).actual, 0);
    assert_eq!(opener.live(), 0);
    waiting.abort();
}

#[tokio::test(start_paused = true)]
async fn converging_hands_the_granted_slot_to_the_waiting_client() {
    let opener = Opener::default();
    let pools = ProxyPools::new();
    pools.insert(primary(), tenant("alice"), pool(opener.clone()));
    let coordinator = coordinator(4, &["alice"]);
    let waiting = tokio::spawn({
        let pool = pools.get(&primary(), &tenant("alice")).unwrap();
        async move { pool.acquire().await }
    });
    settle().await;
    coordinator.report(pools.report(0));
    coordinator.reconcile().unwrap();

    let grants = coordinator.grants().borrow().clone();
    pools.converge(&grants).await;

    let served = waiting.await.unwrap().unwrap();
    assert_eq!(opener.live(), 1);
    let report = pools.report(grants.generation());
    assert_eq!(report.get(&primary(), &tenant("alice")).actual, 1);
    drop(served);
}

#[tokio::test(start_paused = true)]
async fn a_pool_the_grants_leave_out_is_closed_down_to_nothing() {
    let opener = Opener::default();
    let pools = ProxyPools::new();
    let alice = pool(opener.clone());
    pools.insert(primary(), tenant("alice"), alice.clone());
    alice.converge(1).await;
    drop(alice.acquire().await.unwrap());
    assert_eq!(opener.live(), 1);

    let closed = pools.converge(&GrantSet::default()).await;

    assert_eq!(closed, 1);
    assert_eq!(opener.live(), 0);
    assert_eq!(alice.stats().slots, 0);
}

#[tokio::test(start_paused = true)]
async fn a_connection_still_opening_is_reported_as_actual() {
    let opener = Opener::slow(Duration::from_millis(50));
    let pools = ProxyPools::new();
    let alice = pool(opener.clone());
    pools.insert(primary(), tenant("alice"), alice.clone());
    alice.converge(1).await;
    let opening = tokio::spawn({
        let alice = alice.clone();
        async move { alice.acquire().await }
    });
    settle().await;

    let report = pools.report(1);

    assert_eq!(report.get(&primary(), &tenant("alice")).actual, 1);
    drop(opening.await.unwrap().unwrap());
}

#[tokio::test(start_paused = true)]
async fn the_control_loop_serves_a_client_without_a_fixed_pool_size() {
    let opener = Opener::default();
    let pools = Arc::new(ProxyPools::new());
    pools.insert(primary(), tenant("alice"), pool(opener.clone()));
    let coordinator = coordinator(2, &["alice"]);
    let clock = TokioRuntime::new();
    let coordinating = tokio::spawn({
        let coordinator = Arc::clone(&coordinator);
        async move { coordinator.run(&clock, INTERVAL).await }
    });
    let converging = tokio::spawn({
        let pools = Arc::clone(&pools);
        let coordinator = Arc::clone(&coordinator);
        async move { pools.run(coordinator.as_ref(), &clock, INTERVAL).await }
    });

    let pool = pools.get(&primary(), &tenant("alice")).unwrap();
    let served = pool.acquire().await.unwrap();

    assert_eq!(opener.live(), 1);
    drop(served);
    coordinating.abort();
    converging.abort();
}

#[tokio::test(start_paused = true)]
async fn a_slot_moves_between_tenants_without_exceeding_the_budget() {
    let opener = Opener::default();
    let pools = Arc::new(ProxyPools::new());
    pools.insert(primary(), tenant("alice"), pool(opener.clone()));
    pools.insert(primary(), tenant("bob"), pool(opener.clone()));
    let coordinator = coordinator(1, &["alice", "bob"]);
    let clock = TokioRuntime::new();
    let coordinating = tokio::spawn({
        let coordinator = Arc::clone(&coordinator);
        async move { coordinator.run(&clock, INTERVAL).await }
    });
    let converging = tokio::spawn({
        let pools = Arc::clone(&pools);
        let coordinator = Arc::clone(&coordinator);
        async move { pools.run(coordinator.as_ref(), &clock, INTERVAL).await }
    });

    let alice = pools.get(&primary(), &tenant("alice")).unwrap();
    let bob = pools.get(&primary(), &tenant("bob")).unwrap();
    drop(alice.acquire().await.unwrap());
    let served = bob.acquire().await.unwrap();

    assert_eq!(opener.live(), 1);
    assert_eq!(opener.peak(), 1);
    assert_eq!(alice.stats().occupied(), 0);
    drop(served);
    coordinating.abort();
    converging.abort();
}
