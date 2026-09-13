use std::time::Duration;

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
