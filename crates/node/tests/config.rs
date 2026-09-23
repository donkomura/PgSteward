use std::time::Duration;

use pgsteward_core::allocation::InstanceId;
use pgsteward_core::auth::{AuthMethod, Credentials};
use pgsteward_core::tenant::TenantId;
use pgsteward_node::config::{ClusterConfig, ConfigError, NodeConfig, PoolMode, Role};

const NODE_LOCAL: &str = r#"
[node]
role = "proxy"
listen = "0.0.0.0:6432"
coordinator = "pgsteward-0.pgsteward:7432"
max_client_connections = 5000

[node.tls]
cert = "/etc/pgsteward/tls.crt"
key  = "/etc/pgsteward/tls.key"
"#;

const PENCIL_VERIFIER: &str = "SCRAM-SHA-256$4096:W22ZaJ0SNY7soEsUEjb6gQ==$\
WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=:wfPLwcE6nTWhTAmQ7tl2KeoiWGPlZqQxSrmfPwDl2dU=";

const CLUSTER: &str = r#"
[cluster]
pool_mode = "transaction"
grant_ttl = "10s"
arbitration_interval = "10ms"

[[instance]]
name   = "db-primary.internal"
weight = 1

[[instance]]
name   = "db-replica-1.internal"
weight = 2

[tenant."app_web"]
instances = ["db-primary.internal"]
min       = 30
weight    = 1

[tenant."app_report"]
instances = ["db-replica-1.internal"]
min       = 0
weight    = 1

[tenant."*"]
instances = ["db-primary.internal"]
min       = 0
max       = 20
weight    = 1
"#;

#[test]
fn node_local_config_parses_design_doc_example() {
    let config = NodeConfig::parse(NODE_LOCAL).unwrap();
    assert_eq!(config.node.role, Role::Proxy);
    assert_eq!(config.node.listen.port(), 6432);
    assert_eq!(config.node.coordinator, "pgsteward-0.pgsteward:7432");
    assert_eq!(config.node.max_client_connections, 5000);
    let tls = config.node.tls.as_ref().unwrap();
    assert_eq!(tls.cert.to_str().unwrap(), "/etc/pgsteward/tls.crt");
}

#[test]
fn node_local_config_rejects_unknown_role_with_readable_message() {
    let err = NodeConfig::parse(&NODE_LOCAL.replace("\"proxy\"", "\"router\"")).unwrap_err();
    let message = err.to_string();
    assert!(message.contains("role"), "{message}");
    assert!(message.contains("router"), "{message}");
}

#[test]
fn node_local_config_rejects_zero_client_connections() {
    let err = NodeConfig::parse(&NODE_LOCAL.replace("5000", "0")).unwrap_err();
    assert!(matches!(err, ConfigError::Invalid { .. }), "{err}");
    assert!(err.to_string().contains("max_client_connections"), "{err}");
}

#[test]
fn node_local_config_without_tls_is_allowed() {
    let without_tls = NODE_LOCAL.split("[node.tls]").next().unwrap();
    let config = NodeConfig::parse(without_tls).unwrap();
    assert!(config.node.tls.is_none());
}

#[test]
fn cluster_config_parses_design_doc_example() {
    let config = ClusterConfig::parse(CLUSTER).unwrap();
    assert_eq!(config.cluster.pool_mode, PoolMode::Transaction);
    assert_eq!(config.cluster.grant_ttl, Duration::from_secs(10));
    assert_eq!(
        config.cluster.arbitration_interval,
        Duration::from_millis(10)
    );
    assert_eq!(config.instance.len(), 2);
    assert_eq!(config.instance[1].weight, 2);
    let web = &config.tenant["app_web"];
    assert_eq!(web.min, 30);
    assert_eq!(web.max, None);
    assert_eq!(web.weight, 1);
    assert_eq!(config.tenant["*"].max, Some(20));
}

#[test]
fn cluster_config_defaults_optional_fields() {
    let minimal = r#"
[cluster]
pool_mode = "transaction"
grant_ttl = "10s"
arbitration_interval = "10ms"

[[instance]]
name = "db"

[tenant."t"]
instances = ["db"]
"#;
    let config = ClusterConfig::parse(minimal).unwrap();
    assert_eq!(config.instance[0].weight, 1);
    let t = &config.tenant["t"];
    assert_eq!(t.min, 0);
    assert_eq!(t.max, None);
    assert_eq!(t.weight, 1);
}

#[test]
fn cluster_config_rejects_total_budget_as_a_setting() {
    let with_budget = CLUSTER.replace(
        "weight = 1\n\n[[instance]]",
        "weight = 1\nbudget = 150\n\n[[instance]]",
    );
    let err = ClusterConfig::parse(&with_budget).unwrap_err();
    assert!(err.to_string().contains("budget"), "{err}");

    let with_pool_size = CLUSTER.replace(
        "pool_mode = \"transaction\"",
        "pool_mode = \"transaction\"\npool_size = 100",
    );
    let err = ClusterConfig::parse(&with_pool_size).unwrap_err();
    assert!(err.to_string().contains("pool_size"), "{err}");
}

#[test]
fn cluster_config_rejects_tenant_referencing_unknown_instance() {
    let err = ClusterConfig::parse(&CLUSTER.replace(
        "instances = [\"db-replica-1.internal\"]",
        "instances = [\"db-replica-9.internal\"]",
    ))
    .unwrap_err();
    let message = err.to_string();
    assert!(message.contains("app_report"), "{message}");
    assert!(message.contains("db-replica-9.internal"), "{message}");
}

#[test]
fn cluster_config_rejects_min_greater_than_max() {
    let err = ClusterConfig::parse(&CLUSTER.replace(
        "max       = 20",
        "max       = 20\n[tenant.\"bad\"]\ninstances = [\"db-primary.internal\"]\nmin = 5\nmax = 4",
    ))
    .unwrap_err();
    let message = err.to_string();
    assert!(message.contains("bad"), "{message}");
    assert!(message.contains("min"), "{message}");
}

#[test]
fn cluster_config_rejects_zero_weight() {
    let err = ClusterConfig::parse(&CLUSTER.replace("weight = 2", "weight = 0")).unwrap_err();
    assert!(err.to_string().contains("weight"), "{err}");
}

#[test]
fn cluster_config_rejects_duplicate_instance_names() {
    let err = ClusterConfig::parse(&CLUSTER.replace(
        "db-replica-1.internal\"\nweight = 2",
        "db-primary.internal\"\nweight = 2",
    ))
    .unwrap_err();
    assert!(err.to_string().contains("db-primary.internal"), "{err}");
}

#[test]
fn cluster_config_rejects_zero_durations() {
    let err = ClusterConfig::parse(&CLUSTER.replace("\"10ms\"", "\"0ms\"")).unwrap_err();
    assert!(err.to_string().contains("arbitration_interval"), "{err}");
    let err = ClusterConfig::parse(&CLUSTER.replace("\"10s\"", "\"0s\"")).unwrap_err();
    assert!(err.to_string().contains("grant_ttl"), "{err}");
}

#[test]
fn cluster_config_requires_arbitration_interval_shorter_than_grant_ttl() {
    let err = ClusterConfig::parse(&CLUSTER.replace("\"10ms\"", "\"20s\"")).unwrap_err();
    let message = err.to_string();
    assert!(message.contains("arbitration_interval"), "{message}");
    assert!(message.contains("grant_ttl"), "{message}");
}

#[test]
fn cluster_config_rejects_tenant_with_no_instances() {
    let err = ClusterConfig::parse(
        &CLUSTER.replace("instances = [\"db-replica-1.internal\"]", "instances = []"),
    )
    .unwrap_err();
    assert!(err.to_string().contains("app_report"), "{err}");
}

#[test]
fn syntax_errors_are_reported_with_location() {
    let err = ClusterConfig::parse("[cluster\npool_mode = 1").unwrap_err();
    assert!(matches!(err, ConfigError::Syntax(_)), "{err}");
    assert!(err.to_string().contains("line 1"), "{err}");
}

fn with_clients(clients: &str) -> String {
    format!("{NODE_LOCAL}\n{clients}")
}

fn one_client(user: &str, verifier: &str) -> String {
    with_clients(&format!("[client.\"{user}\"]\nverifier = \"{verifier}\"\n"))
}

#[test]
fn node_local_config_reads_the_verifier_of_every_client_it_names() {
    let text = one_client("app_web", PENCIL_VERIFIER);

    let credentials = NodeConfig::parse(&text)
        .unwrap()
        .client_credentials()
        .unwrap();

    assert!(matches!(
        credentials.method(&TenantId::new("app_web", "shop")),
        Some(AuthMethod::ScramSha256(_))
    ));
    assert!(
        credentials
            .method(&TenantId::new("app_report", "shop"))
            .is_none()
    );
}

#[test]
fn node_local_config_without_clients_names_no_one() {
    let credentials = NodeConfig::parse(NODE_LOCAL)
        .unwrap()
        .client_credentials()
        .unwrap();

    assert!(
        credentials
            .method(&TenantId::new("app_web", "shop"))
            .is_none()
    );
}

#[test]
fn node_local_config_rejects_a_verifier_it_cannot_read_with_readable_message() {
    let err = NodeConfig::parse(&one_client("app_web", "hunter2")).unwrap_err();

    assert!(matches!(err, ConfigError::Invalid { .. }), "{err}");
    let message = err.to_string();
    assert!(message.contains("client.\"app_web\".verifier"), "{message}");
    assert!(!message.contains("hunter2"), "{message}");
}

#[test]
fn node_local_config_rejects_a_plain_password_in_place_of_a_verifier() {
    let text = with_clients("[client.\"app_web\"]\npassword = \"hunter2\"\n");

    let err = NodeConfig::parse(&text).unwrap_err();

    assert!(err.to_string().contains("password"), "{err}");
}

#[test]
fn node_local_config_rejects_a_client_without_a_name() {
    let err = NodeConfig::parse(&one_client("", PENCIL_VERIFIER)).unwrap_err();

    assert!(matches!(err, ConfigError::Invalid { .. }), "{err}");
    assert!(err.to_string().contains("client"), "{err}");
}

#[test]
fn the_client_connection_limit_comes_from_the_node_local_config() {
    let config = NodeConfig::parse(NODE_LOCAL).unwrap();
    let limit = config.client_limit();
    assert_eq!(limit.max(), 5000);
    assert_eq!(limit.live(), 0);
}

#[test]
fn a_server_connection_logs_in_as_the_tenant_user_to_the_tenant_database() {
    let text = with_clients("[server.\"app_web\"]\npassword = \"s3cret\"\n");
    let config = NodeConfig::parse(&text).unwrap();

    let credentials = config.server_credentials(&TenantId::new("app_web", "reports"));

    assert_eq!(credentials.user, "app_web");
    assert_eq!(credentials.database, "reports");
    assert_eq!(credentials.password.as_deref(), Some("s3cret"));
}

#[test]
fn a_user_without_a_server_section_logs_in_without_a_password() {
    let config = NodeConfig::parse(NODE_LOCAL).unwrap();

    let credentials = config.server_credentials(&TenantId::new("app_web", "app_web"));

    assert_eq!(credentials.user, "app_web");
    assert_eq!(credentials.password, None);
}

#[test]
fn a_server_password_never_appears_in_the_debug_output() {
    let text = with_clients("[server.\"app_web\"]\npassword = \"s3cret\"\n");
    let config = NodeConfig::parse(&text).unwrap();

    assert!(!format!("{config:?}").contains("s3cret"));
}

#[test]
fn a_server_section_takes_nothing_but_a_password() {
    let text = with_clients("[server.\"app_web\"]\npassword = \"s3cret\"\nuser = \"other\"\n");

    let err = NodeConfig::parse(&text).unwrap_err();

    assert!(err.to_string().contains("user"), "{err}");
    assert!(!err.to_string().contains("s3cret"), "{err}");
}

#[test]
fn the_monitor_logs_in_with_its_own_user_and_the_password_of_its_server_section() {
    let text = with_clients(
        "[monitor]\nuser = \"pgsteward_monitor\"\n\n[server.\"pgsteward_monitor\"]\npassword = \"watch\"\n",
    );
    let config = NodeConfig::parse(&text).unwrap();

    let credentials = config.monitor_credentials().unwrap();

    assert_eq!(credentials.user, "pgsteward_monitor");
    assert_eq!(credentials.database, "postgres");
    assert_eq!(credentials.password.as_deref(), Some("watch"));
}

#[test]
fn the_monitor_database_can_be_named() {
    let text = with_clients("[monitor]\nuser = \"watcher\"\ndatabase = \"ops\"\n");
    let config = NodeConfig::parse(&text).unwrap();

    assert_eq!(config.monitor_credentials().unwrap().database, "ops");
}

#[test]
fn a_node_without_a_monitor_section_has_no_monitor() {
    let config = NodeConfig::parse(NODE_LOCAL).unwrap();

    assert!(config.monitor_credentials().is_none());
}

#[test]
fn the_cluster_config_resolves_tenants_through_its_rules() {
    let config = ClusterConfig::parse(CLUSTER).unwrap();

    let policies = config.policies();

    let web = policies.resolve(&TenantId::new("app_web", "shop")).unwrap();
    assert_eq!(web.policy.min, 30);
    assert_eq!(web.policy.max, u32::MAX);
    assert_eq!(web.instances, vec![InstanceId::new("db-primary.internal")]);
    let unnamed = policies.resolve(&TenantId::new("batch", "batch")).unwrap();
    assert_eq!(unnamed.policy.max, 20);
}

#[test]
fn a_rule_can_name_a_user_and_a_database() {
    let text = format!(
        "{CLUSTER}\n[tenant.\"app_web@reports\"]\ninstances = [\"db-replica-1.internal\"]\nmin = 2\n"
    );
    let config = ClusterConfig::parse(&text).unwrap();

    let policies = config.policies();

    let reports = policies
        .resolve(&TenantId::new("app_web", "reports"))
        .unwrap();
    assert_eq!(reports.policy.min, 2);
    assert_eq!(
        policies
            .resolve(&TenantId::new("app_web", "shop"))
            .unwrap()
            .policy
            .min,
        30
    );
}

#[test]
fn the_cluster_config_routes_by_instance_weight() {
    let text = CLUSTER.replace(
        "instances = [\"db-replica-1.internal\"]\nmin       = 0",
        "instances = [\"db-primary.internal\", \"db-replica-1.internal\"]\nmin       = 0",
    );
    let config = ClusterConfig::parse(&text).unwrap();
    let policies = config.policies();
    let report = TenantId::new("app_report", "app_report");

    assert_eq!(
        policies.route(&report, |total| {
            assert_eq!(total, 3);
            0
        }),
        Some(&InstanceId::new("db-primary.internal"))
    );
    assert_eq!(
        policies.route(&report, |_| 1),
        Some(&InstanceId::new("db-replica-1.internal"))
    );
}

#[test]
fn an_instance_name_without_a_port_is_reached_on_5432() {
    let config = ClusterConfig::parse(CLUSTER).unwrap();

    assert_eq!(config.instance[0].address(), "db-primary.internal:5432");
}

#[test]
fn an_instance_name_with_a_port_is_reached_on_that_port() {
    let text = CLUSTER
        .replace(
            "name   = \"db-primary.internal\"",
            "name   = \"db-primary.internal:5433\"",
        )
        .replace(
            "instances = [\"db-primary.internal\"]",
            "instances = [\"db-primary.internal:5433\"]",
        );
    let config = ClusterConfig::parse(&text).unwrap();

    assert_eq!(config.instance[0].address(), "db-primary.internal:5433");
}

#[test]
fn node_local_config_loads_the_certificate_and_key_it_names() {
    let dir = tempfile::tempdir().unwrap();
    let certified = rcgen::generate_simple_self_signed(vec!["pgsteward.test".to_owned()]).unwrap();
    let cert = dir.path().join("tls.crt");
    let key = dir.path().join("tls.key");
    std::fs::write(&cert, certified.cert.pem()).unwrap();
    std::fs::write(&key, certified.signing_key.serialize_pem()).unwrap();
    let config = NodeConfig::parse(&tls_paths(&cert, &key)).unwrap();

    assert!(config.client_tls().unwrap().is_some());
}

#[test]
fn node_local_config_without_tls_terminates_nothing() {
    let without_tls = NODE_LOCAL.split("[node.tls]").next().unwrap();
    let config = NodeConfig::parse(without_tls).unwrap();

    assert!(config.client_tls().unwrap().is_none());
}

#[test]
fn a_certificate_that_cannot_be_read_names_the_key_it_came_from() {
    let dir = tempfile::tempdir().unwrap();
    let cert = dir.path().join("missing.crt");
    let key = dir.path().join("tls.key");
    std::fs::write(&key, "").unwrap();
    let config = NodeConfig::parse(&tls_paths(&cert, &key)).unwrap();

    let err = config.client_tls().unwrap_err();
    assert!(matches!(err, ConfigError::Invalid { .. }), "{err}");
    assert!(err.to_string().contains("node.tls.cert"), "{err}");
}

#[test]
fn a_key_that_is_not_a_key_names_the_key_it_came_from() {
    let dir = tempfile::tempdir().unwrap();
    let certified = rcgen::generate_simple_self_signed(vec!["pgsteward.test".to_owned()]).unwrap();
    let cert = dir.path().join("tls.crt");
    let key = dir.path().join("tls.key");
    std::fs::write(&cert, certified.cert.pem()).unwrap();
    std::fs::write(&key, "not a key at all").unwrap();
    let config = NodeConfig::parse(&tls_paths(&cert, &key)).unwrap();

    let err = config.client_tls().unwrap_err();
    assert!(matches!(err, ConfigError::Invalid { .. }), "{err}");
    assert!(err.to_string().contains("node.tls.key"), "{err}");
}

fn tls_paths(cert: &std::path::Path, key: &std::path::Path) -> String {
    format!(
        "{}\n[node.tls]\ncert = \"{}\"\nkey = \"{}\"\n",
        NODE_LOCAL.split("[node.tls]").next().unwrap(),
        cert.display(),
        key.display(),
    )
}
