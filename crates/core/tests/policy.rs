use std::num::NonZeroU32;

use pgsteward_core::allocation::InstanceId;
use pgsteward_core::grant::TenantPolicy;
use pgsteward_core::policy::{Policies, TenantRule};
use pgsteward_core::tenant::TenantId;

fn weight(value: u32) -> NonZeroU32 {
    NonZeroU32::new(value).unwrap()
}

fn rule(instances: &[&str], min: u32) -> TenantRule {
    TenantRule {
        instances: instances
            .iter()
            .map(|name| InstanceId::new(*name))
            .collect(),
        policy: TenantPolicy {
            min,
            max: u32::MAX,
            weight: weight(1),
        },
    }
}

fn policies() -> Policies {
    Policies::new()
        .instance(InstanceId::new("primary"), weight(1))
        .instance(InstanceId::new("replica"), weight(3))
        .tenant("app_web@reports", rule(&["replica"], 5))
        .tenant("app_web", rule(&["primary"], 30))
        .tenant("*", rule(&["primary"], 0))
}

#[test]
fn a_rule_for_the_user_and_database_comes_first() {
    let policies = policies();

    let resolved = policies
        .resolve(&TenantId::new("app_web", "reports"))
        .unwrap();

    assert_eq!(resolved.policy.min, 5);
    assert_eq!(resolved.instances, vec![InstanceId::new("replica")]);
}

#[test]
fn a_rule_for_the_user_covers_every_other_database() {
    let policies = policies();

    let resolved = policies
        .resolve(&TenantId::new("app_web", "app_web"))
        .unwrap();

    assert_eq!(resolved.policy.min, 30);
}

#[test]
fn the_wildcard_covers_a_tenant_nobody_named() {
    let policies = policies();

    let resolved = policies.resolve(&TenantId::new("someone", "else")).unwrap();

    assert_eq!(resolved.policy.min, 0);
    assert_eq!(resolved.instances, vec![InstanceId::new("primary")]);
}

#[test]
fn a_tenant_matching_no_rule_resolves_to_nothing() {
    let policies = Policies::new()
        .instance(InstanceId::new("primary"), weight(1))
        .tenant("app_web", rule(&["primary"], 0));

    assert!(
        policies
            .resolve(&TenantId::new("someone", "else"))
            .is_none()
    );
    assert!(
        policies
            .route(&TenantId::new("someone", "else"), |_| 0)
            .is_none()
    );
}

#[test]
fn a_tenant_on_one_instance_is_always_routed_there() {
    let policies = policies();

    for roll in 0..10 {
        assert_eq!(
            policies.route(&TenantId::new("app_web", "app_web"), |total| roll % total),
            Some(&InstanceId::new("primary"))
        );
    }
}

#[test]
fn a_tenant_on_several_instances_is_routed_by_their_weights() {
    let policies = Policies::new()
        .instance(InstanceId::new("primary"), weight(1))
        .instance(InstanceId::new("replica"), weight(3))
        .tenant("app_report", rule(&["primary", "replica"], 0));
    let tenant = TenantId::new("app_report", "app_report");

    let routed: Vec<_> = (0..4)
        .map(|roll| {
            policies
                .route(&tenant, |total| {
                    assert_eq!(total, 4);
                    roll
                })
                .unwrap()
                .to_string()
        })
        .collect();

    assert_eq!(routed, vec!["primary", "replica", "replica", "replica"]);
}

#[test]
fn an_instance_the_rule_lists_is_where_the_tenant_may_be_granted() {
    let policies = policies();
    let tenant = TenantId::new("app_web", "reports");

    assert!(
        policies
            .policy(&InstanceId::new("replica"), &tenant)
            .is_some()
    );
    assert!(
        policies
            .policy(&InstanceId::new("primary"), &tenant)
            .is_none()
    );
}
