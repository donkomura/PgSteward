use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;

use pgsteward_core::allocation::InstanceId;
use pgsteward_core::rt::tokio_rt::TokioRuntime;
use pgsteward_node::config::{ClusterConfig, NodeConfig};
use pgsteward_node::serve::{ServeOptions, Serving, serve};
use pgsteward_protocol::startup::{CancelKey, StartupRequest, encode_startup};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, NoTls};

const PENCIL_VERIFIER: &str = "SCRAM-SHA-256$4096:W22ZaJ0SNY7soEsUEjb6gQ==$\
WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=:wfPLwcE6nTWhTAmQ7tl2KeoiWGPlZqQxSrmfPwDl2dU=";
const TENANT: &str = "alice";

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

async fn through(proxy: SocketAddr) -> Client {
    let conn_str = format!(
        "host=127.0.0.1 port={} user={TENANT} dbname=postgres password=pencil",
        proxy.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
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

/// Starts a node in front of a PostgreSQL whose `max_connections` leaves the
/// tenant `slots` connection slots: three are reserved for superusers, one is
/// the admin connection this harness holds, and one is the node's monitor.
async fn start(slots: u32) -> (ContainerAsync<Postgres>, Serving<TokioRuntime>) {
    let max_connections = slots + 5;
    let container = Postgres::default()
        .with_host_auth()
        .with_tag("16")
        .with_cmd([
            "postgres",
            "-c",
            &format!("max_connections={max_connections}"),
        ])
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

    let serving = serve(
        TokioRuntime::new(),
        node_config(),
        cluster_config(&instance),
        ServeOptions {
            margin: 0,
            ..ServeOptions::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        serving.budget(&InstanceId::new(&instance)),
        slots,
        "the derived budget is what this test is built on"
    );
    (container, serving)
}

#[tokio::test]
async fn a_cancel_request_stops_the_query_the_client_is_running() {
    let (_container, serving) = start(2).await;
    let client = through(serving.local_addr()).await;
    let token = client.cancel_token();

    let running = tokio::spawn(async move { client.simple_query("SELECT pg_sleep(30)").await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    token.cancel_query(NoTls).await.unwrap();

    let error = tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("the query outlived the cancel request")
        .unwrap()
        .expect_err("the query was not cancelled");

    assert_eq!(error.code(), Some(&SqlState::QUERY_CANCELED));
}

#[tokio::test]
async fn a_cancel_request_that_arrives_after_its_own_query_leaves_the_next_one_alone() {
    let (_container, serving) = start(1).await;
    let proxy = serving.local_addr();

    let first = Arc::new(through(proxy).await);
    let stale = first.cancel_token();
    let running_first = {
        let first = Arc::clone(&first);
        tokio::spawn(async move { first.simple_query("SELECT pg_sleep(1)").await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    let second = through(proxy).await;
    let running_second =
        tokio::spawn(async move { second.simple_query("SELECT pg_sleep(5)").await });

    running_first.await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    stale.cancel_query(NoTls).await.unwrap();

    let result = tokio::time::timeout(Duration::from_secs(20), running_second)
        .await
        .expect("the query never finished")
        .unwrap();

    assert!(
        result.is_ok(),
        "a cancel request that outlived its own query stopped the client that took over the connection: {result:?}"
    );
}

#[tokio::test]
async fn a_cancel_request_with_a_key_this_node_never_issued_is_answered_by_closing() {
    let (_container, serving) = start(1).await;
    let proxy = serving.local_addr();

    let client = through(proxy).await;
    let running = tokio::spawn(async move { client.simple_query("SELECT pg_sleep(2)").await });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut socket = TcpStream::connect(proxy).await.unwrap();
    let mut out = BytesMut::new();
    encode_startup(
        &StartupRequest::Cancel(CancelKey {
            process_id: 0,
            secret_key: 0,
        }),
        &mut out,
    );
    socket.write_all(&out).await.unwrap();
    let mut answer = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), socket.read_to_end(&mut answer))
        .await
        .expect("the node held a cancel request open")
        .unwrap();

    assert!(
        answer.is_empty(),
        "a cancel request has no reply in the protocol, so the node must only close: {answer:?}"
    );

    let result = tokio::time::timeout(Duration::from_secs(20), running)
        .await
        .expect("the query never finished")
        .unwrap();

    assert!(
        result.is_ok(),
        "a cancel request this node never issued stopped a query: {result:?}"
    );
}
