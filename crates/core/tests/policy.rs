use std::num::NonZeroU32;

use pgsteward_core::allocation::InstanceId;
use pgsteward_core::grant::TenantPolicy;
use pgsteward_core::policy::{Policies, PolicyChange, SettingError, TenantRule};
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

fn change(min: Option<u32>, max: Option<u32>, weight: Option<u32>) -> PolicyChange {
    PolicyChange { min, max, weight }
}

fn everywhere(budget: u32) -> impl Fn(&InstanceId) -> Option<u32> {
    move |_| Some(budget)
}

#[test]
fn a_setting_moves_only_the_parts_it_names() {
    let policies = policies();

    let changed = policies
        .change_tenant("app_web", change(None, Some(40), None), everywhere(100))
        .unwrap();

    let rule = changed.rule("app_web").unwrap();
    assert_eq!(rule.policy.max, 40);
    assert_eq!(rule.policy.min, 30);
    assert_eq!(rule.policy.weight, weight(1));
    assert_eq!(rule.instances, vec![InstanceId::new("primary")]);
}

#[test]
fn the_name_is_a_rule_the_cluster_configuration_writes_not_a_tenant_it_covers() {
    let policies = policies();

    let error = policies
        .change_tenant(
            "app_web@orders",
            change(Some(1), None, None),
            everywhere(100),
        )
        .unwrap_err();

    assert_eq!(
        error,
        SettingError::NoSuchTenant {
            tenant: "app_web@orders".to_owned()
        }
    );
}

#[test]
fn a_minimum_above_the_maximum_is_refused() {
    let policies = policies();

    let error = policies
        .change_tenant("app_web", change(None, Some(10), None), everywhere(100))
        .unwrap_err();

    assert_eq!(error, SettingError::MinAboveMax { min: 30, max: 10 });
}

#[test]
fn a_minimum_and_a_maximum_written_together_move_both() {
    let policies = policies();

    let changed = policies
        .change_tenant("app_web", change(Some(5), Some(10), None), everywhere(100))
        .unwrap();

    let policy = changed.rule("app_web").unwrap().policy;
    assert_eq!((policy.min, policy.max), (5, 10));
}

#[test]
fn a_weight_of_zero_is_refused() {
    let policies = policies();

    let error = policies
        .change_tenant("app_web", change(None, None, Some(0)), everywhere(100))
        .unwrap_err();

    assert_eq!(error, SettingError::ZeroWeight);
}

#[test]
fn minimums_above_the_total_budget_are_refused() {
    let policies = policies();

    let error = policies
        .change_tenant("*", change(Some(15), None, None), everywhere(40))
        .unwrap_err();

    assert_eq!(
        error,
        SettingError::AboveBudget {
            instance: InstanceId::new("primary"),
            minimums: 45,
            budget: 40,
        }
    );
}

#[test]
fn only_the_instances_the_rule_names_are_weighed_against_a_budget() {
    let policies = policies();

    let changed = policies
        .change_tenant(
            "app_web@reports",
            change(Some(50), None, None),
            |instance| {
                Some(if instance == &InstanceId::new("replica") {
                    100
                } else {
                    1
                })
            },
        )
        .unwrap();

    assert_eq!(changed.rule("app_web@reports").unwrap().policy.min, 50);
}

#[test]
fn an_instance_whose_total_budget_is_not_derived_yet_weighs_nothing() {
    let policies = policies();

    let changed = policies
        .change_tenant("*", change(Some(1000), None, None), |_| None)
        .unwrap();

    assert_eq!(changed.rule("*").unwrap().policy.min, 1000);
}
