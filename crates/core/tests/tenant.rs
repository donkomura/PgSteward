use std::collections::{BTreeSet, HashSet};

use pgsteward_core::tenant::{TenantId, TenantResolveError};
use pgsteward_protocol::startup::{ProtocolVersion, StartupMessage};

fn startup(params: &[(&str, &str)]) -> StartupMessage {
    StartupMessage::new(
        ProtocolVersion { major: 3, minor: 0 },
        params
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect(),
    )
}

#[test]
fn tenant_is_the_user_and_database_of_the_startup_message() {
    let tenant =
        TenantId::from_startup(&startup(&[("user", "app_web"), ("database", "shop")])).unwrap();
    assert_eq!(tenant, TenantId::new("app_web", "shop"));
    assert_eq!(tenant.user(), "app_web");
    assert_eq!(tenant.database(), "shop");
}

#[test]
fn tenant_database_defaults_to_the_user() {
    let tenant = TenantId::from_startup(&startup(&[("user", "app_web")])).unwrap();
    assert_eq!(tenant, TenantId::new("app_web", "app_web"));
}

#[test]
fn tenant_requires_a_user() {
    assert_eq!(
        TenantId::from_startup(&startup(&[("database", "shop")])),
        Err(TenantResolveError::MissingUser)
    );
    assert_eq!(
        TenantId::from_startup(&startup(&[("user", "")])),
        Err(TenantResolveError::MissingUser)
    );
}

#[test]
fn tenant_ignores_parameters_other_than_user_and_database() {
    let psql = startup(&[
        ("user", "app_web"),
        ("database", "shop"),
        ("application_name", "psql"),
    ]);
    let worker = startup(&[
        ("application_name", "worker"),
        ("database", "shop"),
        ("user", "app_web"),
        ("client_encoding", "UTF8"),
    ]);
    assert_eq!(
        TenantId::from_startup(&psql).unwrap(),
        TenantId::from_startup(&worker).unwrap()
    );
}

#[test]
fn tenants_with_different_users_or_databases_are_distinct() {
    let a = TenantId::new("app_web", "shop");
    let b = TenantId::new("app_web", "reports");
    let c = TenantId::new("app_report", "shop");
    let set: HashSet<_> = [a.clone(), b.clone(), c.clone(), a.clone()]
        .into_iter()
        .collect();
    assert_eq!(set.len(), 3);
    let ordered: BTreeSet<_> = [c, b, a].into_iter().collect();
    assert_eq!(
        ordered.iter().map(ToString::to_string).collect::<Vec<_>>(),
        ["app_report@shop", "app_web@reports", "app_web@shop"]
    );
}

#[test]
fn tenant_displays_as_user_at_database() {
    assert_eq!(TenantId::new("app_web", "shop").to_string(), "app_web@shop");
}

#[test]
fn missing_user_error_tells_the_operator_what_was_missing() {
    let message = TenantResolveError::MissingUser.to_string();
    assert!(message.contains("user"), "{message}");
    assert!(message.contains("startup"), "{message}");
}
