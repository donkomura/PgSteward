use std::net::SocketAddr;

use pgsteward_core::allocation::InstanceId;
use pgsteward_core::console::DATABASE;
use pgsteward_core::rt::tokio_rt::TokioRuntime;
use pgsteward_node::config::{ClusterConfig, NodeConfig};
use pgsteward_node::serve::{ServeOptions, serve};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage, SimpleQueryRow};

const PENCIL_VERIFIER: &str = "SCRAM-SHA-256$4096:W22ZaJ0SNY7soEsUEjb6gQ==$\
WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=:wfPLwcE6nTWhTAmQ7tl2KeoiWGPlZqQxSrmfPwDl2dU=";
const TENANT: &str = "alice";
const MAX_CONNECTIONS: &str = "20";
const MAX_CONNECTIONS_SETTING: &str = "max_connections=20";

async fn direct(port: u16, application_name: &str) -> Client {
    let conn_str = format!(
        "host=127.0.0.1 port={port} user=postgres dbname=postgres application_name={application_name}"
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

async fn through(proxy: SocketAddr, user: &str, database: &str) -> Client {
    let conn_str = format!(
        "host=127.0.0.1 port={} user={user} dbname={database} password=pencil",
        proxy.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

async fn table(client: &Client, command: &str) -> Vec<SimpleQueryRow> {
    client
        .simple_query(command)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect()
}

async fn proxy_connections(admin: &Client) -> String {
    table(
        admin,
        "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'pgsteward-proxy'",
    )
    .await[0]
        .get(0)
        .expect("a count")
        .to_owned()
}

fn node_config() -> NodeConfig {
    NodeConfig::parse(&format!(
        r#"
[node]
role = "proxy"
listen = "127.0.0.1:0"
coordinator = "127.0.0.1:7432"
max_client_connections = 100

[monitor]
user = "postgres"

[client."{TENANT}"]
verifier = "{PENCIL_VERIFIER}"
"#
    ))
    .unwrap()
}

fn cluster_config(instance: &str) -> ClusterConfig {
    ClusterConfig::parse(&format!(
        r#"
[cluster]
pool_mode = "transaction"
grant_ttl = "10s"
arbitration_interval = "10ms"

[[instance]]
name = "{instance}"

[tenant."*"]
instances = ["{instance}"]
"#
    ))
    .unwrap()
}

#[tokio::test]
async fn the_console_answers_on_the_reserved_database_without_holding_a_connection() {
    let container = Postgres::default()
        .with_host_auth()
        .with_tag("16")
        .with_cmd(["postgres", "-c", MAX_CONNECTIONS_SETTING])
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let admin = direct(port, "harness-admin").await;
    admin
        .simple_query(&format!("CREATE ROLE {TENANT} LOGIN"))
        .await
        .unwrap();
    let instance = format!("127.0.0.1:{port}");
    let rt = TokioRuntime::new();
    let serving = serve(
        rt,
        node_config(),
        cluster_config(&instance),
        ServeOptions {
            margin: 0,
            ..ServeOptions::default()
        },
    )
    .await
    .unwrap();
    let proxy = serving.local_addr();

    let tenant = through(proxy, TENANT, "postgres").await;
    tenant.simple_query("BEGIN").await.unwrap();
    tenant.simple_query("SELECT 1").await.unwrap();
    let held = proxy_connections(&admin).await;

    let console = through(proxy, TENANT, DATABASE).await;

    let pools = table(&console, "SHOW POOLS").await;
    assert_eq!(pools.len(), 1, "one pool is open for the tenant");
    assert_eq!(pools[0].get("database"), Some("postgres"));
    assert_eq!(pools[0].get("user"), Some(TENANT));
    assert_eq!(pools[0].get("instance"), Some(instance.as_str()));
    assert_eq!(pools[0].get("pool_mode"), Some("transaction"));
    assert_eq!(pools[0].get("sv_active"), Some("1"));

    let instances = table(&console, "SHOW INSTANCES").await;
    assert_eq!(instances.len(), 1);
    assert_eq!(instances[0].get("instance"), Some(instance.as_str()));
    assert_eq!(instances[0].get("max_connections"), Some(MAX_CONNECTIONS));
    let budget = serving.budget(&InstanceId::new(&instance)).to_string();
    assert_eq!(instances[0].get("total_budget"), Some(budget.as_str()));

    let table_of_grants = table(&console, "SHOW BUDGET").await;
    assert!(
        table_of_grants
            .iter()
            .any(|row| row.get("instance") == Some(instance.as_str())),
        "the allocation table names the instance"
    );

    assert_eq!(
        proxy_connections(&admin).await,
        held,
        "the console session opened no connection of its own"
    );
}

#[tokio::test]
async fn the_console_refuses_a_text_it_cannot_read_and_goes_on() {
    let container = Postgres::default()
        .with_host_auth()
        .with_tag("16")
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let instance = format!("127.0.0.1:{port}");
    let serving = serve(
        TokioRuntime::new(),
        node_config(),
        cluster_config(&instance),
        ServeOptions::default(),
    )
    .await
    .unwrap();

    let console = through(serving.local_addr(), TENANT, DATABASE).await;

    let error = console.simple_query("SELECT 1").await.unwrap_err();
    assert_eq!(
        error.code().map(tokio_postgres::error::SqlState::code),
        Some("42601")
    );
    assert!(
        !table(&console, "SHOW INSTANCES").await.is_empty(),
        "the session goes on after a refusal"
    );
}
