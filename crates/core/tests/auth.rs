use pgsteward_core::auth::{AuthMethod, ClientCredentials, Credentials, TrustAll};
use pgsteward_core::scram::{DEFAULT_ITERATIONS, ScramVerifier};
use pgsteward_core::tenant::TenantId;

const SALT: &[u8] = b"pgsteward-salt-1";

fn verifier(password: &str) -> ScramVerifier {
    ScramVerifier::from_password(password, SALT, DEFAULT_ITERATIONS)
}

fn table() -> ClientCredentials {
    let mut credentials = ClientCredentials::new();
    credentials.insert("app_web", AuthMethod::ScramSha256(verifier("secret")));
    credentials.insert("app_report", AuthMethod::Trust);
    credentials
}

fn scram_verifier_for(
    credentials: &ClientCredentials,
    user: &str,
    database: &str,
) -> ScramVerifier {
    match credentials.method(&TenantId::new(user, database)) {
        Some(AuthMethod::ScramSha256(verifier)) => verifier,
        other => panic!("expected a SCRAM-SHA-256 verifier for {user}, got {other:?}"),
    }
}

#[test]
fn a_user_the_table_names_authenticates_with_the_verifier_it_was_given() {
    assert_eq!(
        scram_verifier_for(&table(), "app_web", "shop"),
        verifier("secret")
    );
}

#[test]
fn the_table_answers_the_same_way_for_every_database_one_user_reaches() {
    let credentials = table();

    assert_eq!(
        scram_verifier_for(&credentials, "app_web", "shop"),
        scram_verifier_for(&credentials, "app_web", "report"),
    );
}

#[test]
fn a_user_the_table_does_not_name_has_no_method_so_the_exchange_is_played_out_with_a_mock() {
    assert!(table().method(&TenantId::new("nobody", "shop")).is_none());
}

#[test]
fn an_empty_table_names_no_one() {
    assert!(
        ClientCredentials::new()
            .method(&TenantId::new("app_web", "shop"))
            .is_none()
    );
}

#[test]
fn trust_all_answers_for_every_tenant() {
    assert!(matches!(
        TrustAll.method(&TenantId::new("anyone", "anything")),
        Some(AuthMethod::Trust)
    ));
}
