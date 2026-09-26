use std::fmt::Write as _;
use std::net::SocketAddr;
use std::time::Duration;

use pgsteward_core::allocation::InstanceId;
use pgsteward_core::rt::tokio_rt::TokioRuntime;
use pgsteward_harness::cap::CapMonitor;
use pgsteward_harness::pg_stat_activity::PgStatActivity;
use pgsteward_node::config::{ClusterConfig, NodeConfig};
use pgsteward_node::serve::{ServeOptions, serve};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

const PENCIL_VERIFIER: &str = "SCRAM-SHA-256$4096:W22ZaJ0SNY7soEsUEjb6gQ==$\
WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=:wfPLwcE6nTWhTAmQ7tl2KeoiWGPlZqQxSrmfPwDl2dU=";
const TENANTS: [&str; 2] = ["alice", "bob"];
const CLIENTS_PER_TENANT: usize = 4;

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

async fn through(proxy: SocketAddr, user: &str) -> Client {
    let conn_str = format!(
        "host=127.0.0.1 port={} user={user} dbname=postgres password=pencil",
        proxy.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

async fn first_value(client: &Client, sql: &str) -> String {
    client
        .simple_query(sql)
        .await
        .unwrap()
        .iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(row.get(0).expect("a value").to_owned()),
            _ => None,
        })
        .expect("a row")
}

fn node_config() -> NodeConfig {
    let clients = TENANTS.iter().fold(String::new(), |mut clients, user| {
        let _ = writeln!(
            clients,
            "[client.\"{user}\"]\nverifier = \"{PENCIL_VERIFIER}\""
        );
        clients
    });
    NodeConfig::parse(&format!(
        r#"
[node]
role = "proxy"
listen = "127.0.0.1:0"
coordinator = "127.0.0.1:7432"
max_client_connections = 100

[monitor]
user = "postgres"

{clients}
"#
    ))
    .unwrap()
}

fn cluster_config(instance: &str) -> ClusterConfig {
    ClusterConfig::parse(&cluster_config_text(instance)).unwrap()
}

fn cluster_config_text(instance: &str) -> String {
    format!(
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
    )
}

#[tokio::test]
async fn the_node_serves_its_tenants_within_the_budget_it_derives() {
    let container = Postgres::default()
        .with_host_auth()
        .with_tag("16")
        .with_cmd(["postgres", "-c", "max_connections=8"])
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let admin = direct(port, "harness-admin").await;
    for user in TENANTS {
        admin
            .simple_query(&format!("CREATE ROLE {user} LOGIN"))
            .await
            .unwrap();
    }
    let observer = direct(port, "harness-observer").await;
    let instance = format!("127.0.0.1:{port}");
    let rt = TokioRuntime::new();

    let serving = serve(
        rt,
        node_config(),
        cluster_config(&instance),
        ServeOptions {
            margin: 0,
            release_delay: Duration::from_millis(100),
            ..ServeOptions::default()
        },
    )
    .await
    .unwrap();

    let budget = serving.budget(&InstanceId::new(&instance));
    assert_eq!(
        budget, 2,
        "8 max_connections - 3 superuser reserved - 2 foreign - 1 monitor"
    );
    let monitor = CapMonitor::start(
        &rt,
        PgStatActivity::new(observer, "pgsteward-proxy"),
        budget as usize,
        Duration::from_millis(20),
    );

    let proxy = serving.local_addr();
    let clients: Vec<_> = TENANTS
        .iter()
        .flat_map(|user| {
            (0..CLIENTS_PER_TENANT).map(move |_| {
                tokio::spawn(async move {
                    let client = through(proxy, user).await;
                    client.simple_query("BEGIN").await.unwrap();
                    let current = first_value(&client, "SELECT current_user").await;
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    client.simple_query("COMMIT").await.unwrap();
                    assert_eq!(current, *user, "a tenant runs as its own user");
                })
            })
        })
        .collect();
    for client in clients {
        client.await.unwrap();
    }

    let report = monitor.stop().await;
    report.assert_never_exceeded();

    let mut waited = Duration::ZERO;
    while serving.pool_count() > 0 && waited < Duration::from_secs(5) {
        tokio::time::sleep(Duration::from_millis(20)).await;
        waited += Duration::from_millis(20);
    }
    assert_eq!(
        serving.pool_count(),
        0,
        "tenants whose clients have gone keep no pool once the release delay has passed"
    );
}

#[tokio::test]
async fn the_node_refuses_to_start_without_a_monitor() {
    let text = "[node]\nrole = \"proxy\"\nlisten = \"127.0.0.1:0\"\ncoordinator = \"127.0.0.1:7432\"\nmax_client_connections = 1\n";
    let node = NodeConfig::parse(text).unwrap();

    let error = serve(
        TokioRuntime::new(),
        node,
        cluster_config("127.0.0.1:1"),
        ServeOptions::default(),
    )
    .await
    .unwrap_err();

    assert!(error.to_string().contains("[monitor]"), "{error}");
}

#[tokio::test]
async fn the_node_refuses_a_pool_mode_it_does_not_serve_yet() {
    let text = cluster_config_text("127.0.0.1:1").replace("\"transaction\"", "\"session\"");
    let cluster = ClusterConfig::parse(&text).unwrap();

    let error = serve(
        TokioRuntime::new(),
        node_config(),
        cluster,
        ServeOptions::default(),
    )
    .await
    .unwrap_err();

    assert!(error.to_string().contains("session"), "{error}");
}
