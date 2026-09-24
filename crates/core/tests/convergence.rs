use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use pgsteward_core::allocation::{InstanceId, ProxyId};
use pgsteward_core::budget::{InstanceBudget, ServerLimits};
use pgsteward_core::convergence::ProxyPools;
use pgsteward_core::grant::{GrantChannel, GrantSet, InProcessCoordinator, TenantPolicy};
use pgsteward_core::policy::{Policies, TenantRule};
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

fn flat(total: u32) -> InstanceBudget {
    InstanceBudget::new(
        ServerLimits {
            max_connections: total,
            superuser_reserved_connections: 0,
            reserved_connections: 0,
        },
        0,
        Duration::from_secs(60),
    )
}

fn coordinator(budget: u32, tenants: &[&str]) -> Arc<InProcessCoordinator<WeightedMaxMinFair>> {
    let coordinator =
        InProcessCoordinator::new(ProxyId::new("proxy-1"), WeightedMaxMinFair::default());
    coordinator.add_instance(primary(), flat(budget));
    coordinator.set_policies(tenants.iter().fold(
        Policies::new().instance(primary(), NonZeroU32::new(1).unwrap()),
        |policies, user| {
            policies.tenant(
                *user,
                TenantRule {
                    instances: vec![primary()],
                    policy: TenantPolicy {
                        min: 0,
                        max: u32::MAX,
                        weight: NonZeroU32::new(1).unwrap(),
                    },
                },
            )
        },
    ));
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
async fn the_report_leaves_out_the_client_that_left_while_it_waited() {
    let opener = Opener::default();
    let pools = ProxyPools::new();
    pools.insert(primary(), tenant("alice"), pool(opener.clone()));
    let leaving = tokio::spawn({
        let pool = pools.get(&primary(), &tenant("alice")).unwrap();
        async move { pool.acquire().await }
    });
    settle().await;
    assert_eq!(pools.report(0).get(&primary(), &tenant("alice")).demand, 1);

    leaving.abort();
    let _ = leaving.await;
    settle().await;

    let report = pools.report(1);
    assert_eq!(
        report.get(&primary(), &tenant("alice")).demand,
        0,
        "a client that went away asks for nothing"
    );
    assert_eq!(opener.live(), 0);
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

    let (pool, _) = pools.checkout(&primary(), &tenant("alice"), || pool(opener.clone()));
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

    let (alice, _) = pools.checkout(&primary(), &tenant("alice"), || pool(opener.clone()));
    let (bob, _) = pools.checkout(&primary(), &tenant("bob"), || pool(opener.clone()));
    drop(alice.acquire().await.unwrap());
    let served = bob.acquire().await.unwrap();

    assert_eq!(opener.live(), 1);
    assert_eq!(opener.peak(), 1);
    assert_eq!(alice.stats().occupied(), 0);
    drop(served);
    coordinating.abort();
    converging.abort();
}

#[tokio::test(start_paused = true)]
async fn a_checkout_opens_one_pool_per_instance_and_tenant() {
    let opener = Opener::default();
    let pools = ProxyPools::new();
    let mut made = 0;

    let (first, _) = pools.checkout(&primary(), &tenant("alice"), || {
        made += 1;
        pool(opener.clone())
    });
    let (second, _) = pools.checkout(&primary(), &tenant("alice"), || {
        made += 1;
        pool(opener.clone())
    });
    let (other, _) = pools.checkout(&primary(), &tenant("bob"), || {
        made += 1;
        pool(opener.clone())
    });

    assert_eq!(made, 2);
    first.converge(3).await;
    assert_eq!(second.stats().slots, 3);
    assert_eq!(other.stats().slots, 0);
}

#[tokio::test(start_paused = true)]
async fn a_pool_nobody_uses_is_dropped_once_its_grant_is_gone() {
    let opener = Opener::default();
    let pools = ProxyPools::new();
    let (alice, _) = pools.checkout(&primary(), &tenant("alice"), || pool(opener.clone()));
    alice.converge(1).await;
    drop(alice.acquire().await.unwrap());
    drop(alice);

    pools.converge(&GrantSet::default()).await;

    assert!(pools.get(&primary(), &tenant("alice")).is_none());
    assert_eq!(pools.len(), 0);
    assert_eq!(opener.live(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_pool_a_client_still_holds_is_kept_without_a_grant() {
    let opener = Opener::default();
    let pools = ProxyPools::new();
    let (alice, _) = pools.checkout(&primary(), &tenant("alice"), || pool(opener.clone()));

    pools.converge(&GrantSet::default()).await;

    assert!(pools.get(&primary(), &tenant("alice")).is_some());
    drop(alice);
    pools.converge(&GrantSet::default()).await;
    assert!(pools.get(&primary(), &tenant("alice")).is_none());
}

#[tokio::test(start_paused = true)]
async fn a_pool_that_still_has_a_grant_is_kept_while_idle() {
    let opener = Opener::default();
    let pools = Arc::new(ProxyPools::new());
    let coordinator = coordinator(2, &["alice"]);
    let (alice, _) = pools.checkout(&primary(), &tenant("alice"), || pool(opener.clone()));
    let waiting = tokio::spawn({
        let alice = alice.clone();
        async move { alice.acquire().await }
    });
    settle().await;
    coordinator.report(pools.report(0));
    coordinator.reconcile().unwrap();
    let grants = coordinator.grants().borrow().clone();
    pools.converge(&grants).await;
    drop(waiting.await.unwrap().unwrap());
    drop(alice);

    pools.converge(&grants).await;

    assert!(pools.get(&primary(), &tenant("alice")).is_some());
    assert_eq!(opener.live(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_tenant_without_a_minimum_keeps_no_pool_once_its_clients_are_gone() {
    let opener = Opener::default();
    let pools = Arc::new(ProxyPools::new());
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
    let (alice, _) = pools.checkout(&primary(), &tenant("alice"), || pool(opener.clone()));
    drop(alice.acquire().await.unwrap());
    drop(alice);

    clock.sleep(INTERVAL * 20).await;

    assert_eq!(pools.len(), 0);
    assert_eq!(opener.live(), 0);
    assert_eq!(coordinator.table().holders(&primary()).count(), 0);
    coordinating.abort();
    converging.abort();
}

#[tokio::test(start_paused = true)]
async fn draining_closes_every_connection_whatever_the_grants_say() {
    let opener = Opener::default();
    let pools = ProxyPools::new();
    let alice = pool(opener.clone());
    pools.insert(primary(), tenant("alice"), alice.clone());
    alice.converge(2).await;
    let first = alice.acquire().await.unwrap();
    let second = alice.acquire().await.unwrap();
    drop(first);
    drop(second);
    assert_eq!(opener.live(), 2);

    let closed = pools.drain().await;

    assert_eq!(closed, 2);
    assert_eq!(opener.live(), 0);
    assert_eq!(pools.occupied(), 0);
}

#[tokio::test(start_paused = true)]
async fn draining_leaves_the_connection_a_client_still_holds() {
    let opener = Opener::default();
    let pools = ProxyPools::new();
    let alice = pool(opener.clone());
    pools.insert(primary(), tenant("alice"), alice.clone());
    alice.converge(1).await;
    let held = alice.acquire().await.unwrap();

    let closed = pools.drain().await;

    assert_eq!(closed, 0);
    assert_eq!(opener.live(), 1);
    assert_eq!(pools.occupied(), 1);
    drop(held);
}
