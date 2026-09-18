use std::collections::BTreeMap;
use std::num::NonZeroU32;

use pgsteward_core::allocation::{Holder, InstanceId, ProxyId};
use pgsteward_core::grant::{GrantChannel, InProcessCoordinator, Report, TenantPolicy, Usage};
use pgsteward_core::tenant::TenantId;
use pgsteward_sched::fair::WeightedMaxMinFair;
use proptest::prelude::*;

fn primary() -> InstanceId {
    InstanceId::new("primary")
}

fn tenant(user: &str) -> TenantId {
    TenantId::new(user, user)
}

fn proxy() -> ProxyId {
    ProxyId::new("proxy-1")
}

fn open_policy() -> TenantPolicy {
    TenantPolicy {
        min: 0,
        max: u32::MAX,
        weight: NonZeroU32::new(1).unwrap(),
    }
}

fn coordinator(budget: u32, tenants: &[&str]) -> InProcessCoordinator<WeightedMaxMinFair> {
    let coordinator = InProcessCoordinator::new(proxy(), WeightedMaxMinFair::default());
    coordinator.set_budget(primary(), budget);
    for user in tenants {
        coordinator.set_policy(primary(), tenant(user), open_policy());
    }
    coordinator
}

fn granted(coordinator: &InProcessCoordinator<WeightedMaxMinFair>, user: &str) -> u32 {
    coordinator.grants().borrow().get(&primary(), &tenant(user))
}

fn generation(coordinator: &InProcessCoordinator<WeightedMaxMinFair>) -> u64 {
    coordinator.grants().borrow().generation()
}

fn report(coordinator: &InProcessCoordinator<WeightedMaxMinFair>, usage: &[(&str, u32, u32)]) {
    let generation = generation(coordinator);
    let report = usage
        .iter()
        .fold(Report::new(generation), |report, (user, demand, actual)| {
            report.usage(
                primary(),
                tenant(user),
                Usage {
                    demand: *demand,
                    actual: *actual,
                },
            )
        });
    coordinator.report(report);
}

#[test]
fn a_tenant_that_asks_is_granted_what_it_asks_for() {
    let coordinator = coordinator(10, &["alice"]);
    report(&coordinator, &[("alice", 3, 0)]);

    coordinator.reconcile().unwrap();

    assert_eq!(granted(&coordinator, "alice"), 3);
    assert_eq!(
        coordinator
            .table()
            .granted(&primary(), &Holder::new(tenant("alice"), proxy())),
        3
    );
}

#[test]
fn nothing_is_granted_before_a_reconciliation() {
    let coordinator = coordinator(10, &["alice"]);
    report(&coordinator, &[("alice", 3, 0)]);

    assert_eq!(granted(&coordinator, "alice"), 0);
    assert_eq!(generation(&coordinator), 0);
}

#[test]
fn a_tenant_without_demand_is_granted_nothing_despite_its_minimum() {
    let coordinator = InProcessCoordinator::new(proxy(), WeightedMaxMinFair::default());
    coordinator.set_budget(primary(), 10);
    coordinator.set_policy(
        primary(),
        tenant("alice"),
        TenantPolicy {
            min: 5,
            ..open_policy()
        },
    );
    report(&coordinator, &[("alice", 0, 0)]);

    coordinator.reconcile().unwrap();

    assert_eq!(granted(&coordinator, "alice"), 0);
    assert_eq!(coordinator.table().holders(&primary()).count(), 0);
}

#[test]
fn a_tenant_without_a_policy_is_granted_nothing() {
    let coordinator = coordinator(10, &[]);
    report(&coordinator, &[("alice", 3, 0)]);

    coordinator.reconcile().unwrap();

    assert_eq!(granted(&coordinator, "alice"), 0);
}

#[test]
fn an_instance_without_a_budget_grants_nothing() {
    let coordinator = InProcessCoordinator::new(proxy(), WeightedMaxMinFair::default());
    coordinator.set_policy(primary(), tenant("alice"), open_policy());
    report(&coordinator, &[("alice", 3, 0)]);

    coordinator.reconcile().unwrap();

    assert_eq!(granted(&coordinator, "alice"), 0);
}

#[test]
fn competing_tenants_share_the_budget() {
    let coordinator = coordinator(10, &["alice", "bob"]);
    report(&coordinator, &[("alice", 8, 0), ("bob", 8, 0)]);

    coordinator.reconcile().unwrap();

    assert_eq!(granted(&coordinator, "alice"), 5);
    assert_eq!(granted(&coordinator, "bob"), 5);
    assert_eq!(coordinator.table().granted_total(&primary()), 10);
}

#[test]
fn a_reconciliation_that_changes_nothing_keeps_the_generation() {
    let coordinator = coordinator(10, &["alice"]);
    report(&coordinator, &[("alice", 3, 0)]);
    coordinator.reconcile().unwrap();
    let before = generation(&coordinator);
    report(&coordinator, &[("alice", 3, 3)]);

    coordinator.reconcile().unwrap();

    assert_eq!(generation(&coordinator), before);
    assert_eq!(granted(&coordinator, "alice"), 3);
}

#[test]
fn a_changed_grant_is_published_under_a_new_generation() {
    let coordinator = coordinator(10, &["alice"]);
    report(&coordinator, &[("alice", 3, 0)]);
    coordinator.reconcile().unwrap();
    let first = generation(&coordinator);
    report(&coordinator, &[("alice", 5, 3)]);

    coordinator.reconcile().unwrap();

    assert!(generation(&coordinator) > first);
    assert_eq!(granted(&coordinator, "alice"), 5);
}

#[test]
fn a_subscriber_sees_the_new_grants() {
    let coordinator = coordinator(10, &["alice"]);
    let mut grants = coordinator.grants();
    report(&coordinator, &[("alice", 3, 0)]);

    coordinator.reconcile().unwrap();

    assert!(grants.has_changed().unwrap());
    assert_eq!(
        grants.borrow_and_update().get(&primary(), &tenant("alice")),
        3
    );
}

#[test]
fn a_reclaimed_slot_is_not_regranted_until_its_holder_reports_it_closed() {
    let coordinator = coordinator(4, &["alice", "bob"]);
    report(&coordinator, &[("alice", 4, 0)]);
    coordinator.reconcile().unwrap();
    report(&coordinator, &[("alice", 4, 4)]);
    coordinator.reconcile().unwrap();
    assert_eq!(granted(&coordinator, "alice"), 4);

    report(&coordinator, &[("alice", 0, 4), ("bob", 4, 0)]);
    coordinator.reconcile().unwrap();
    assert_eq!(granted(&coordinator, "alice"), 0);
    assert_eq!(granted(&coordinator, "bob"), 0);

    coordinator.reconcile().unwrap();
    assert_eq!(granted(&coordinator, "bob"), 0);

    report(&coordinator, &[("alice", 0, 1), ("bob", 4, 0)]);
    coordinator.reconcile().unwrap();
    assert_eq!(granted(&coordinator, "bob"), 3);

    report(&coordinator, &[("alice", 0, 0), ("bob", 4, 3)]);
    coordinator.reconcile().unwrap();
    assert_eq!(granted(&coordinator, "bob"), 4);
}

#[test]
fn a_report_older_than_the_reclaim_does_not_release_the_slot() {
    let coordinator = coordinator(4, &["alice", "bob"]);
    report(&coordinator, &[("alice", 4, 0)]);
    coordinator.reconcile().unwrap();
    let before_reclaim = generation(&coordinator);
    report(&coordinator, &[("alice", 0, 4), ("bob", 4, 0)]);
    coordinator.reconcile().unwrap();
    assert!(generation(&coordinator) > before_reclaim);

    coordinator.report(
        Report::new(before_reclaim)
            .usage(
                primary(),
                tenant("alice"),
                Usage {
                    demand: 0,
                    actual: 0,
                },
            )
            .usage(
                primary(),
                tenant("bob"),
                Usage {
                    demand: 4,
                    actual: 0,
                },
            ),
    );
    coordinator.reconcile().unwrap();

    assert_eq!(granted(&coordinator, "bob"), 0);
}

#[test]
fn a_grant_not_yet_reported_counts_in_full() {
    let coordinator = coordinator(4, &["alice", "bob"]);
    report(&coordinator, &[("alice", 4, 0)]);
    coordinator.reconcile().unwrap();

    report(&coordinator, &[("alice", 0, 0), ("bob", 4, 0)]);
    coordinator.reconcile().unwrap();

    assert_eq!(granted(&coordinator, "alice"), 0);
    assert_eq!(granted(&coordinator, "bob"), 0);
}

#[test]
fn a_larger_budget_is_granted_at_once() {
    let coordinator = coordinator(4, &["alice"]);
    report(&coordinator, &[("alice", 6, 0)]);
    coordinator.reconcile().unwrap();
    assert_eq!(granted(&coordinator, "alice"), 4);

    coordinator.set_budget(primary(), 8);
    coordinator.reconcile().unwrap();

    assert_eq!(granted(&coordinator, "alice"), 6);
}

#[test]
fn a_smaller_budget_lowers_the_grants_with_it() {
    let coordinator = coordinator(4, &["alice"]);
    report(&coordinator, &[("alice", 4, 0)]);
    coordinator.reconcile().unwrap();
    report(&coordinator, &[("alice", 4, 4)]);

    coordinator.set_budget(primary(), 2);
    coordinator.reconcile().unwrap();

    assert_eq!(granted(&coordinator, "alice"), 2);
    assert_eq!(coordinator.table().budget(&primary()), 2);
}

#[test]
fn a_smaller_budget_holds_back_growth_until_the_excess_is_closed() {
    let coordinator = coordinator(4, &["alice", "bob"]);
    report(&coordinator, &[("alice", 4, 0)]);
    coordinator.reconcile().unwrap();
    report(&coordinator, &[("alice", 4, 4)]);

    coordinator.set_budget(primary(), 2);
    report(&coordinator, &[("alice", 4, 4), ("bob", 2, 0)]);
    coordinator.reconcile().unwrap();

    assert_eq!(granted(&coordinator, "alice"), 1);
    assert_eq!(granted(&coordinator, "bob"), 0);

    report(&coordinator, &[("alice", 4, 1), ("bob", 2, 0)]);
    coordinator.reconcile().unwrap();

    assert_eq!(granted(&coordinator, "bob"), 1);
}

#[derive(Debug, Clone)]
enum Step {
    Demand(usize, u32),
    Reconcile,
    Open,
    Close(usize),
    Report,
}

fn any_step() -> impl Strategy<Value = Step> {
    prop_oneof![
        (0usize..3, 0u32..=6).prop_map(|(tenant, demand)| Step::Demand(tenant, demand)),
        Just(Step::Reconcile),
        Just(Step::Open),
        (0usize..3).prop_map(Step::Close),
        Just(Step::Report),
    ]
}

const USERS: [&str; 3] = ["alice", "bob", "carol"];

struct ModelProxy {
    seen: BTreeMap<usize, u32>,
    generation: u64,
    demand: [u32; 3],
    actual: [u32; 3],
}

impl ModelProxy {
    fn open(&mut self, coordinator: &InProcessCoordinator<WeightedMaxMinFair>) {
        let grants = coordinator.grants().borrow().clone();
        self.generation = grants.generation();
        for (index, user) in USERS.iter().enumerate() {
            let grant = grants.get(&primary(), &tenant(user));
            self.seen.insert(index, grant);
            let wanted = self.demand[index].min(grant);
            if self.actual[index] < wanted {
                self.actual[index] = wanted;
            }
        }
    }

    fn close(&mut self, index: usize) {
        let grant = self.seen.get(&index).copied().unwrap_or(0);
        self.actual[index] = self.actual[index].min(grant);
    }

    fn report(&self, coordinator: &InProcessCoordinator<WeightedMaxMinFair>) {
        let report =
            USERS
                .iter()
                .enumerate()
                .fold(Report::new(self.generation), |report, (index, user)| {
                    report.usage(
                        primary(),
                        tenant(user),
                        Usage {
                            demand: self.demand[index],
                            actual: self.actual[index],
                        },
                    )
                });
        coordinator.report(report);
    }
}

proptest! {
    #[test]
    fn a_proxy_that_follows_its_grants_never_puts_the_instance_over_its_budget(
        budget in 1u32..=6,
        steps in prop::collection::vec(any_step(), 1..80),
    ) {
        let coordinator = coordinator(budget, &USERS);
        let mut proxy = ModelProxy {
            seen: BTreeMap::new(),
            generation: 0,
            demand: [0; 3],
            actual: [0; 3],
        };
        for step in steps {
            match step {
                Step::Demand(index, demand) => proxy.demand[index] = demand,
                Step::Reconcile => coordinator.reconcile().unwrap(),
                Step::Open => proxy.open(&coordinator),
                Step::Close(index) => proxy.close(index),
                Step::Report => proxy.report(&coordinator),
            }
            prop_assert!(proxy.actual.iter().sum::<u32>() <= budget);
            prop_assert!(coordinator.table().granted_total(&primary()) <= budget);
        }
    }
}
