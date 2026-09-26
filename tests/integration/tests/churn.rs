use std::collections::BTreeMap;
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
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Instant, sleep};
use tokio_postgres::{Client, NoTls};

const PENCIL_VERIFIER: &str = "SCRAM-SHA-256$4096:W22ZaJ0SNY7soEsUEjb6gQ==$\
WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=:wfPLwcE6nTWhTAmQ7tl2KeoiWGPlZqQxSrmfPwDl2dU=";
const TENANTS: [&str; 3] = ["alice", "bob", "carol"];
const WARM_UP: Duration = Duration::from_secs(5);
const STEADY: Duration = Duration::from_secs(15);
const RECREATED_PER_MINUTE_BELOW: u64 = 1;

struct Load {
    clients_per_tenant: usize,
    think: Duration,
}

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

async fn constant_load(client: Client, think: Duration, until: Instant) -> usize {
    let mut committed = 0;
    while Instant::now() < until {
        client.simple_query("BEGIN").await.unwrap();
        client.simple_query("SELECT pg_sleep(0.005)").await.unwrap();
        client.simple_query("COMMIT").await.unwrap();
        committed += 1;
        sleep(think).await;
    }
    committed
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

#[derive(Debug, Default, Clone, Copy)]
struct Connections {
    opened: u64,
    held: u64,
}

fn per_tenant(body: &str) -> BTreeMap<String, Connections> {
    let mut tenants: BTreeMap<String, Connections> = BTreeMap::new();
    for line in body.lines() {
        let Some((series, value)) = line.rsplit_once(' ') else {
            continue;
        };
        let Some(user) = series
            .split(',')
            .find_map(|label| label.strip_prefix("user=\""))
            .and_then(|rest| rest.split('"').next())
        else {
            continue;
        };
        let value: u64 = value.parse().expect("a whole number");
        let tenant = tenants.entry(user.to_owned()).or_default();
        if series.starts_with("pgsteward_server_connections_opened_total{") {
            tenant.opened += value;
        } else if series.starts_with("pgsteward_server_connections{") {
            tenant.held += value;
        }
    }
    tenants
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
metrics_listen = "127.0.0.1:0"
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

async fn steady_load_recreates_no_connection(load: Load) {
    let container = Postgres::default()
        .with_host_auth()
        .with_tag("16")
        .with_cmd(["postgres", "-c", "max_connections=16"])
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
            wait_timeout: Duration::from_secs(10),
            ..ServeOptions::default()
        },
    )
    .await
    .unwrap();
    let budget = serving.budget(&InstanceId::new(&instance));
    let monitor = CapMonitor::start(
        &rt,
        PgStatActivity::new(observer, "pgsteward-proxy"),
        budget as usize,
        Duration::from_millis(20),
    );
    let metrics = serving.metrics_addr().expect("the scrape address");

    let proxy = serving.local_addr();
    let mut clients = Vec::new();
    for user in TENANTS {
        for _ in 0..load.clients_per_tenant {
            clients.push((user, through(proxy, user).await));
        }
    }
    let until = Instant::now() + WARM_UP + STEADY;
    let loads: Vec<_> = clients
        .into_iter()
        .map(|(user, client)| (user, tokio::spawn(constant_load(client, load.think, until))))
        .collect();

    sleep(WARM_UP).await;
    let before = per_tenant(&scrape(metrics).await);
    sleep(STEADY).await;
    let after = per_tenant(&scrape(metrics).await);

    let mut committed: BTreeMap<&str, usize> = BTreeMap::new();
    for (user, load) in loads {
        *committed.entry(user).or_default() += load.await.unwrap();
    }
    let report = monitor.stop().await;
    report.assert_never_exceeded();

    for user in TENANTS {
        assert!(committed[user] > 0, "{user} was not served: {committed:?}");
        let before = before.get(user).copied().unwrap_or_default();
        let after = after.get(user).copied().unwrap_or_default();
        let opened = after.opened - before.opened;
        let recreated = opened - after.held.saturating_sub(before.held).min(opened);
        assert!(
            recreated * 60 < RECREATED_PER_MINUTE_BELOW * STEADY.as_secs(),
            "{user} recreated {recreated} server connections in {STEADY:?} of steady load \
             ({before:?} before, {after:?} after); allowed fewer than \
             {RECREATED_PER_MINUTE_BELOW} a minute"
        );
    }
}

#[tokio::test]
async fn tenants_competing_for_the_budget_keep_their_connections() {
    steady_load_recreates_no_connection(Load {
        clients_per_tenant: 10,
        think: Duration::ZERO,
    })
    .await;
}

#[tokio::test]
async fn tenants_within_the_budget_keep_their_connections_between_transactions() {
    steady_load_recreates_no_connection(Load {
        clients_per_tenant: 2,
        think: Duration::from_millis(20),
    })
    .await;
}
