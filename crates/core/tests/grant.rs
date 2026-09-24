use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use pgsteward_core::allocation::{Holder, InstanceId, ProxyId};
use pgsteward_core::budget::{BudgetChange, InstanceBudget, ServerLimits};
use pgsteward_core::grant::{GrantChannel, InProcessCoordinator, Report, TenantPolicy, Usage};
use pgsteward_core::policy::{Policies, PolicyChange, SettingError, TenantRule};
use pgsteward_core::rt::Instant;
use pgsteward_core::tenant::TenantId;
use pgsteward_sched::fair::WeightedMaxMinFair;
use proptest::prelude::*;
use tokio::time::timeout;

const WINDOW: Duration = Duration::from_secs(60);

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
    coordinator.add_instance(primary(), flat(budget));
    coordinator.set_policies(policies(tenants, open_policy()));
    coordinator
}

fn limits(max_connections: u32) -> ServerLimits {
    ServerLimits {
        max_connections,
        superuser_reserved_connections: 3,
        reserved_connections: 2,
    }
}

fn flat(total: u32) -> InstanceBudget {
    InstanceBudget::new(
        ServerLimits {
            max_connections: total,
            superuser_reserved_connections: 0,
            reserved_connections: 0,
        },
        0,
        WINDOW,
    )
}

fn policies(tenants: &[&str], policy: TenantPolicy) -> Policies {
    tenants.iter().fold(
        Policies::new().instance(primary(), NonZeroU32::new(1).unwrap()),
        |policies, user| {
            policies.tenant(
                *user,
                TenantRule {
                    instances: vec![primary()],
                    policy,
                },
            )
        },
    )
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
    coordinator.add_instance(primary(), flat(10));
    coordinator.set_policies(policies(
        &["alice"],
        TenantPolicy {
            min: 5,
            ..open_policy()
        },
    ));
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
    coordinator.set_policies(policies(&["alice"], open_policy()));
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

    coordinator.add_instance(primary(), flat(8));
    coordinator.reconcile().unwrap();

    assert_eq!(granted(&coordinator, "alice"), 6);
}

#[test]
fn a_smaller_budget_lowers_the_grants_with_it() {
    let coordinator = coordinator(4, &["alice"]);
    report(&coordinator, &[("alice", 4, 0)]);
    coordinator.reconcile().unwrap();
    report(&coordinator, &[("alice", 4, 4)]);

    coordinator.add_instance(primary(), flat(2));
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

    coordinator.add_instance(primary(), flat(2));
    report(&coordinator, &[("alice", 4, 4), ("bob", 2, 0)]);
    coordinator.reconcile().unwrap();

    assert_eq!(granted(&coordinator, "alice"), 1);
    assert_eq!(granted(&coordinator, "bob"), 0);

    report(&coordinator, &[("alice", 4, 1), ("bob", 2, 0)]);
    coordinator.reconcile().unwrap();

    assert_eq!(granted(&coordinator, "bob"), 1);
}

#[test]
fn the_coordinator_allocates_against_the_budget_it_derives() {
    let coordinator = InProcessCoordinator::new(proxy(), WeightedMaxMinFair::default());
    coordinator.add_instance(primary(), InstanceBudget::new(limits(200), 15, WINDOW));
    coordinator.observe_instance(&primary(), Instant::now(), 30);
    coordinator.set_policies(policies(&["alice"], open_policy()));
    report(&coordinator, &[("alice", 500, 0)]);

    coordinator.reconcile().unwrap();

    assert_eq!(coordinator.table().budget(&primary()), 150);
    assert_eq!(granted(&coordinator, "alice"), 150);
}

#[test]
fn foreign_connections_that_appear_shrink_what_the_coordinator_allocates() {
    let coordinator = InProcessCoordinator::new(proxy(), WeightedMaxMinFair::default());
    coordinator.add_instance(primary(), InstanceBudget::new(limits(200), 15, WINDOW));
    coordinator.set_policies(policies(&["alice"], open_policy()));

    let change = coordinator.observe_instance(&primary(), Instant::now(), 50);

    assert_eq!(change, Some(BudgetChange::Shrank { from: 180, to: 130 }));
    coordinator.reconcile().unwrap();
    assert_eq!(coordinator.table().budget(&primary()), 130);
}

#[test]
fn the_coordinator_answers_how_it_derived_each_budget() {
    let coordinator = InProcessCoordinator::new(proxy(), WeightedMaxMinFair::default());
    coordinator.add_instance(primary(), InstanceBudget::new(limits(200), 15, WINDOW));
    coordinator.observe_instance(&primary(), Instant::now(), 30);

    let derived = coordinator.instances();

    assert_eq!(derived.len(), 1);
    let (instance, budget) = &derived[0];
    assert_eq!(instance, &primary());
    assert_eq!(budget.total(), 150);
    assert_eq!(budget.inputs().limits, limits(200));
    assert_eq!(budget.inputs().foreign_peak, 30);
    assert_eq!(budget.inputs().margin, 15);
}

#[test]
fn an_instance_the_coordinator_does_not_hold_is_not_observed() {
    let coordinator = InProcessCoordinator::new(proxy(), WeightedMaxMinFair::default());

    assert_eq!(
        coordinator.observe_instance(&primary(), Instant::now(), 30),
        None
    );
    assert!(coordinator.instances().is_empty());
}

#[tokio::test]
async fn a_written_margin_moves_the_budget_that_is_derived_from_it() {
    let coordinator = InProcessCoordinator::new(proxy(), WeightedMaxMinFair::default());
    coordinator.add_instance(primary(), InstanceBudget::new(limits(200), 15, WINDOW));

    coordinator.set_margin(&primary(), 45).await.unwrap();

    let (_, budget) = &coordinator.instances()[0];
    assert_eq!(budget.inputs().margin, 45);
    assert_eq!(budget.total(), 150);
}

#[tokio::test]
async fn a_margin_that_grows_the_budget_is_in_force_when_the_setting_is_answered() {
    let coordinator = InProcessCoordinator::new(proxy(), WeightedMaxMinFair::default());
    coordinator.add_instance(primary(), InstanceBudget::new(limits(200), 15, WINDOW));
    coordinator.set_policies(policies(&["alice"], open_policy()));
    report(&coordinator, &[("alice", 500, 0)]);
    coordinator.reconcile().unwrap();
    assert_eq!(granted(&coordinator, "alice"), 180);

    coordinator.set_margin(&primary(), 5).await.unwrap();

    assert_eq!(coordinator.table().budget(&primary()), 190);
    assert_eq!(granted(&coordinator, "alice"), 190);
}

#[tokio::test(start_paused = true)]
async fn a_margin_that_shrinks_the_budget_is_answered_after_the_connections_are_gone() {
    let coordinator = Arc::new(InProcessCoordinator::new(
        proxy(),
        WeightedMaxMinFair::default(),
    ));
    coordinator.add_instance(primary(), InstanceBudget::new(limits(200), 15, WINDOW));
    coordinator.set_policies(policies(&["alice"], open_policy()));
    report(&coordinator, &[("alice", 500, 0)]);
    coordinator.reconcile().unwrap();
    report(&coordinator, &[("alice", 500, 180)]);

    let mut writing = tokio::spawn({
        let coordinator = Arc::clone(&coordinator);
        async move { coordinator.set_margin(&primary(), 45).await }
    });

    assert!(
        timeout(Duration::from_secs(60), &mut writing)
            .await
            .is_err(),
        "the setting is not answered while the proxy still holds the connections"
    );
    assert_eq!(
        granted(&coordinator, "alice"),
        150,
        "the desired state is computed within the new budget at once"
    );

    report(&coordinator, &[("alice", 500, 150)]);

    writing.await.unwrap().unwrap();
    assert_eq!(coordinator.table().budget(&primary()), 150);
}

#[tokio::test]
async fn a_margin_for_an_instance_the_coordinator_does_not_hold_is_refused() {
    let coordinator = InProcessCoordinator::new(proxy(), WeightedMaxMinFair::default());

    let error = coordinator.set_margin(&primary(), 20).await.unwrap_err();

    assert_eq!(
        error,
        SettingError::NoSuchInstance {
            instance: primary()
        }
    );
}

#[tokio::test]
async fn a_margin_that_would_put_the_budget_below_the_minimums_is_refused() {
    let coordinator = InProcessCoordinator::new(proxy(), WeightedMaxMinFair::default());
    coordinator.add_instance(primary(), InstanceBudget::new(limits(200), 15, WINDOW));
    coordinator.set_policies(policies(
        &["alice"],
        TenantPolicy {
            min: 100,
            ..open_policy()
        },
    ));

    let error = coordinator.set_margin(&primary(), 150).await.unwrap_err();

    assert_eq!(
        error,
        SettingError::AboveBudget {
            instance: primary(),
            minimums: 100,
            budget: 45,
        }
    );
    let (_, budget) = &coordinator.instances()[0];
    assert_eq!(budget.inputs().margin, 15);
    assert_eq!(budget.total(), 180);
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

#[test]
fn a_tenant_covered_by_the_wildcard_is_granted() {
    let coordinator = InProcessCoordinator::new(proxy(), WeightedMaxMinFair::default());
    coordinator.add_instance(primary(), flat(10));
    coordinator.set_policies(policies(&["*"], open_policy()));
    report(&coordinator, &[("alice", 3, 0)]);

    coordinator.reconcile().unwrap();

    assert_eq!(granted(&coordinator, "alice"), 3);
}

#[test]
fn a_tenant_is_granted_nothing_on_an_instance_its_rule_does_not_list() {
    let replica = InstanceId::new("replica");
    let coordinator = InProcessCoordinator::new(proxy(), WeightedMaxMinFair::default());
    coordinator.add_instance(primary(), flat(10));
    coordinator.add_instance(replica.clone(), flat(10));
    coordinator.set_policies(policies(&["alice"], open_policy()));
    coordinator.report(Report::new(0).usage(
        replica.clone(),
        tenant("alice"),
        Usage {
            demand: 3,
            actual: 0,
        },
    ));

    coordinator.reconcile().unwrap();

    assert_eq!(
        coordinator
            .grants()
            .borrow()
            .get(&replica, &tenant("alice")),
        0
    );
}

#[test]
fn a_tenant_setting_moves_the_grants_without_waiting_for_the_next_round() {
    let coordinator = coordinator(10, &["alice"]);
    report(&coordinator, &[("alice", 8, 0)]);
    coordinator.reconcile().unwrap();
    assert_eq!(granted(&coordinator, "alice"), 8);

    coordinator
        .set_tenant(
            "alice",
            PolicyChange {
                max: Some(3),
                ..PolicyChange::default()
            },
        )
        .unwrap();

    assert_eq!(granted(&coordinator, "alice"), 3);
}

#[test]
fn a_refused_tenant_setting_leaves_the_rule_that_was_in_force() {
    let coordinator = coordinator(10, &["alice"]);
    report(&coordinator, &[("alice", 8, 0)]);
    coordinator.reconcile().unwrap();

    let error = coordinator
        .set_tenant(
            "alice",
            PolicyChange {
                min: Some(11),
                ..PolicyChange::default()
            },
        )
        .unwrap_err();

    assert_eq!(
        error,
        SettingError::AboveBudget {
            instance: primary(),
            minimums: 11,
            budget: 10,
        }
    );
    assert_eq!(coordinator.policies().rule("alice").unwrap().policy.min, 0);
    assert_eq!(granted(&coordinator, "alice"), 8);
}

#[test]
fn a_proxy_that_gave_its_grants_back_holds_none() {
    let coordinator = coordinator(10, &["alice"]);
    report(&coordinator, &[("alice", 3, 3)]);
    coordinator.reconcile().unwrap();
    let before = generation(&coordinator);

    coordinator.withdraw();

    assert_eq!(granted(&coordinator, "alice"), 0);
    assert_eq!(coordinator.table().holders(&primary()).count(), 0);
    assert_eq!(coordinator.table().headroom(&primary()), 10);
    assert!(
        generation(&coordinator) > before,
        "the proxy learns that it holds nothing"
    );
}

#[test]
fn a_proxy_that_gave_its_grants_back_is_granted_nothing_again() {
    let coordinator = coordinator(10, &["alice"]);
    report(&coordinator, &[("alice", 3, 3)]);
    coordinator.reconcile().unwrap();
    coordinator.withdraw();

    report(&coordinator, &[("alice", 5, 0)]);
    coordinator.reconcile().unwrap();

    assert_eq!(granted(&coordinator, "alice"), 0);
    assert_eq!(coordinator.table().holders(&primary()).count(), 0);
}
