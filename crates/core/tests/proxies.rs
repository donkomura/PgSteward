use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use pgsteward_core::allocation::{InstanceId, ProxyId};
use pgsteward_core::budget::{InstanceBudget, ServerLimits};
use pgsteward_core::grant::{GrantChannel, InProcessCoordinator, Report, TenantPolicy, Usage};
use pgsteward_core::policy::{Policies, TenantRule};
use pgsteward_core::rt::Instant;
use pgsteward_core::tenant::TenantId;
use pgsteward_sched::fair::WeightedMaxMinFair;

const RELEASE_DELAY: Duration = Duration::from_secs(1);

type Coordinator = InProcessCoordinator<WeightedMaxMinFair>;

fn primary() -> InstanceId {
    InstanceId::new("primary")
}

fn tenant(user: &str) -> TenantId {
    TenantId::new(user, user)
}

fn open_policy() -> TenantPolicy {
    TenantPolicy {
        min: 0,
        max: u32::MAX,
        weight: NonZeroU32::new(1).unwrap(),
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
        Duration::from_secs(60),
    )
}

fn coordinator(budget: u32, rules: &[(&str, TenantPolicy)]) -> Arc<Coordinator> {
    let coordinator =
        InProcessCoordinator::new(ProxyId::new("proxy-1"), WeightedMaxMinFair::default())
            .with_release_delay(RELEASE_DELAY);
    coordinator.add_instance(primary(), flat(budget));
    coordinator.set_policies(rules.iter().fold(
        Policies::new().instance(primary(), NonZeroU32::new(1).unwrap()),
        |policies, (user, policy)| {
            policies.tenant(
                *user,
                TenantRule {
                    instances: vec![primary()],
                    policy: *policy,
                },
            )
        },
    ));
    Arc::new(coordinator)
}

fn open(budget: u32, users: &[&str]) -> Arc<Coordinator> {
    let rules: Vec<(&str, TenantPolicy)> =
        users.iter().map(|user| (*user, open_policy())).collect();
    coordinator(budget, &rules)
}

fn granted(proxy: &impl GrantChannel, user: &str) -> u32 {
    proxy.grants().borrow().get(&primary(), &tenant(user))
}

fn report(proxy: &impl GrantChannel, usage: &[(&str, u32, u32)]) {
    let generation = proxy.grants().borrow().generation();
    proxy.report(
        usage
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
            }),
    );
}

#[test]
fn each_proxy_is_granted_what_its_own_clients_ask_for() {
    let coordinator = open(10, &["alice"]);
    let second = coordinator.channel(ProxyId::new("proxy-2"));
    report(coordinator.as_ref(), &[("alice", 3, 0)]);
    report(&second, &[("alice", 4, 0)]);

    coordinator.reconcile().unwrap();

    assert_eq!(granted(coordinator.as_ref(), "alice"), 3);
    assert_eq!(granted(&second, "alice"), 4);
    assert_eq!(coordinator.table().granted_total(&primary()), 7);
}

#[test]
fn a_tenant_maximum_caps_the_sum_over_its_proxies() {
    let coordinator = coordinator(
        10,
        &[(
            "alice",
            TenantPolicy {
                max: 5,
                ..open_policy()
            },
        )],
    );
    let second = coordinator.channel(ProxyId::new("proxy-2"));
    report(coordinator.as_ref(), &[("alice", 4, 0)]);
    report(&second, &[("alice", 4, 0)]);

    coordinator.reconcile().unwrap();

    assert_eq!(
        granted(coordinator.as_ref(), "alice") + granted(&second, "alice"),
        5
    );
}

#[test]
fn a_tenant_minimum_is_guaranteed_over_its_proxies_together() {
    let coordinator = coordinator(
        6,
        &[
            (
                "alice",
                TenantPolicy {
                    min: 4,
                    ..open_policy()
                },
            ),
            ("bob", open_policy()),
        ],
    );
    let second = coordinator.channel(ProxyId::new("proxy-2"));
    report(coordinator.as_ref(), &[("alice", 2, 0), ("bob", 10, 0)]);
    report(&second, &[("alice", 2, 0)]);

    coordinator.reconcile().unwrap();

    assert_eq!(granted(coordinator.as_ref(), "alice"), 2);
    assert_eq!(granted(&second, "alice"), 2);
    assert_eq!(granted(coordinator.as_ref(), "bob"), 2);
}

#[test]
fn the_grants_over_every_proxy_stay_within_the_budget() {
    let coordinator = open(6, &["alice", "bob"]);
    let second = coordinator.channel(ProxyId::new("proxy-2"));
    report(coordinator.as_ref(), &[("alice", 10, 0), ("bob", 10, 0)]);
    report(&second, &[("alice", 10, 0), ("bob", 10, 0)]);

    coordinator.reconcile().unwrap();

    assert_eq!(coordinator.table().granted_total(&primary()), 6);
    assert!(granted(&second, "alice") + granted(&second, "bob") > 0);
}

#[test]
fn a_report_from_one_proxy_leaves_the_demand_of_another() {
    let coordinator = open(10, &["alice"]);
    let second = coordinator.channel(ProxyId::new("proxy-2"));
    report(&second, &[("alice", 4, 0)]);
    report(coordinator.as_ref(), &[("alice", 3, 0)]);

    coordinator.reconcile().unwrap();

    assert_eq!(granted(&second, "alice"), 4);
}

#[test]
fn a_proxy_that_gave_its_grants_back_leaves_the_others_theirs() {
    let coordinator = open(10, &["alice"]);
    let second = coordinator.channel(ProxyId::new("proxy-2"));
    report(coordinator.as_ref(), &[("alice", 3, 3)]);
    report(&second, &[("alice", 4, 4)]);
    coordinator.reconcile().unwrap();
    assert_eq!(granted(&second, "alice"), 4);

    second.withdraw();

    assert_eq!(granted(&second, "alice"), 0);
    assert_eq!(granted(coordinator.as_ref(), "alice"), 3);
    assert_eq!(coordinator.table().granted_total(&primary()), 3);
}

#[test]
fn an_idle_grant_on_one_proxy_gives_way_before_a_busy_one() {
    let coordinator = open(8, &["alice", "bob"]);
    let second = coordinator.channel(ProxyId::new("proxy-2"));
    let start = Instant::now();
    report(coordinator.as_ref(), &[("alice", 5, 0)]);
    report(&second, &[("alice", 3, 0)]);
    coordinator.reconcile_at(start).unwrap();
    report(coordinator.as_ref(), &[("alice", 5, 5)]);
    report(&second, &[("alice", 0, 3)]);
    coordinator.reconcile_at(start).unwrap();
    assert_eq!(granted(&second, "alice"), 3);

    report(coordinator.as_ref(), &[("alice", 5, 5), ("bob", 3, 0)]);
    coordinator
        .reconcile_at(start + Duration::from_millis(10))
        .unwrap();

    assert_eq!(granted(coordinator.as_ref(), "alice"), 5);
    assert_eq!(granted(&second, "alice"), 0);
}
