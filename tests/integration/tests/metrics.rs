use std::net::SocketAddr;

use pgsteward_core::allocation::InstanceId;
use pgsteward_core::rt::tokio_rt::TokioRuntime;
use pgsteward_node::config::{ClusterConfig, NodeConfig};
use pgsteward_node::serve::{ServeOptions, serve};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_postgres::{Client, NoTls};

const PENCIL_VERIFIER: &str = "SCRAM-SHA-256$4096:W22ZaJ0SNY7soEsUEjb6gQ==$\
WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=:wfPLwcE6nTWhTAmQ7tl2KeoiWGPlZqQxSrmfPwDl2dU=";
const TENANT: &str = "alice";
const MAX_CONNECTIONS_SETTING: &str = "max_connections=20";

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

async fn scrape(addr: SocketAddr) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: node\r\n\r\n")
        .await
        .unwrap();
    let mut answer = String::new();
    stream.read_to_string(&mut answer).await.unwrap();
    assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
    answer.split_once("\r\n\r\n").expect("a body").1.to_owned()
}

fn sample(body: &str, series: &str) -> String {
    body.lines()
        .find(|line| line.starts_with(series))
        .unwrap_or_else(|| panic!("no series {series} in:\n{body}"))
        .rsplit(' ')
        .next()
        .expect("a value")
        .to_owned()
}

fn node_config() -> NodeConfig {
    NodeConfig::parse(&format!(
        r#"
[node]
role = "proxy"
listen = "127.0.0.1:0"
metrics_listen = "127.0.0.1:0"
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
async fn a_scrape_reads_the_derived_budget_and_the_connections_held_against_it() {
    let container = Postgres::default()
        .with_host_auth()
        .with_tag("16")
        .with_cmd(["postgres", "-c", MAX_CONNECTIONS_SETTING])
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let admin = direct(port).await;
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
    let metrics = serving.metrics_addr().expect("the scrape address");

    let tenant = through(serving.local_addr(), TENANT, "postgres").await;
    tenant.simple_query("BEGIN").await.unwrap();
    tenant.simple_query("SELECT 1").await.unwrap();

    let body = scrape(metrics).await;

    let budget = serving.budget(&InstanceId::new(&instance));
    assert_eq!(
        sample(
            &body,
            &format!("pgsteward_total_budget_slots{{instance=\"{instance}\"}}")
        ),
        budget.to_string()
    );
    assert_eq!(
        sample(&body, "pgsteward_max_connections"),
        "20",
        "the scrape carries what the budget was derived from"
    );
    assert_eq!(
        sample(
            &body,
            &format!(
                "pgsteward_server_connections{{instance=\"{instance}\",database=\"postgres\",user=\"{TENANT}\",state=\"active\"}}"
            )
        ),
        "1"
    );
    let granted = sample(
        &body,
        &format!("pgsteward_granted_slots{{instance=\"{instance}\""),
    );
    assert!(
        granted.parse::<u32>().expect("a whole number") >= 1,
        "the slots the pool was granted stand beside the connection it holds:\n{body}"
    );
}
