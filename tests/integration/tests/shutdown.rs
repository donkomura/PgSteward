use std::net::SocketAddr;
use std::time::Duration;

use pgsteward_core::allocation::InstanceId;
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
const MAX_CONNECTIONS_SETTING: &str = "max_connections=20";
const GRACE: Duration = Duration::from_millis(300);

async fn direct(port: u16) -> Client {
    let conn_str = format!(
        "host=127.0.0.1 port={port} user=postgres dbname=postgres application_name=harness"
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

async fn through(proxy: SocketAddr, user: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(&connection_string(proxy, user), NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

fn connection_string(proxy: SocketAddr, user: &str) -> String {
    format!(
        "host=127.0.0.1 port={} user={user} dbname=postgres password=pencil",
        proxy.port()
    )
}

async fn rows(client: &Client, command: &str) -> Vec<SimpleQueryRow> {
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
    rows(
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

async fn instance_on(port: u16) -> Client {
    let admin = direct(port).await;
    admin
        .simple_query(&format!("CREATE ROLE {TENANT} LOGIN"))
        .await
        .unwrap();
    admin
}

#[tokio::test]
async fn a_node_that_stops_closes_its_connections_and_gives_the_grants_back() {
    let container = Postgres::default()
        .with_host_auth()
        .with_tag("16")
        .with_cmd(["postgres", "-c", MAX_CONNECTIONS_SETTING])
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let admin = instance_on(port).await;
    let instance = format!("127.0.0.1:{port}");
    let serving = serve(
        TokioRuntime::new(),
        node_config(),
        cluster_config(&instance),
        ServeOptions {
            margin: 0,
            shutdown_grace: GRACE,
            ..ServeOptions::default()
        },
    )
    .await
    .unwrap();
    let tenant = through(serving.local_addr(), TENANT).await;
    tenant.simple_query("SELECT 1").await.unwrap();
    assert_eq!(proxy_connections(&admin).await, "1");
    drop(tenant);

    let stopped = serving.shutdown().await;

    assert_eq!(stopped.closed, 1);
    assert_eq!(stopped.held, 0);
    assert_eq!(
        proxy_connections(&admin).await,
        "0",
        "the node closed every connection it held"
    );
    assert_eq!(
        serving.granted(&InstanceId::new(&instance)),
        0,
        "the slots are back with the instance"
    );
}

#[tokio::test]
async fn a_node_that_stopped_takes_no_new_client() {
    let container = Postgres::default()
        .with_host_auth()
        .with_tag("16")
        .with_cmd(["postgres", "-c", MAX_CONNECTIONS_SETTING])
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let _admin = instance_on(port).await;
    let instance = format!("127.0.0.1:{port}");
    let serving = serve(
        TokioRuntime::new(),
        node_config(),
        cluster_config(&instance),
        ServeOptions {
            margin: 0,
            shutdown_grace: GRACE,
            ..ServeOptions::default()
        },
    )
    .await
    .unwrap();
    let proxy = serving.local_addr();

    serving.shutdown().await;

    let refused = tokio_postgres::connect(&connection_string(proxy, TENANT), NoTls).await;
    assert!(
        refused.is_err(),
        "a client that arrives after the stop is not taken"
    );
}

#[tokio::test]
async fn a_transaction_that_is_still_running_keeps_its_connection_past_the_grace() {
    let container = Postgres::default()
        .with_host_auth()
        .with_tag("16")
        .with_cmd(["postgres", "-c", MAX_CONNECTIONS_SETTING])
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let admin = instance_on(port).await;
    let instance = format!("127.0.0.1:{port}");
    let serving = serve(
        TokioRuntime::new(),
        node_config(),
        cluster_config(&instance),
        ServeOptions {
            margin: 0,
            shutdown_grace: GRACE,
            ..ServeOptions::default()
        },
    )
    .await
    .unwrap();
    let tenant = through(serving.local_addr(), TENANT).await;
    tenant.simple_query("BEGIN").await.unwrap();
    tenant.simple_query("SELECT 1").await.unwrap();

    let stopped = serving.shutdown().await;

    assert_eq!(stopped.closed, 0);
    assert_eq!(stopped.held, 1, "the transaction was not cut off");
    assert_eq!(proxy_connections(&admin).await, "1");
    assert!(
        serving.granted(&InstanceId::new(&instance)) > 0,
        "a slot still holding a connection is not given back"
    );
    tenant.simple_query("COMMIT").await.unwrap();
}
