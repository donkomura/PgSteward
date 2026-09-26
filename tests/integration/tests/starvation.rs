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
use tokio::time::Instant;
use tokio_postgres::{Client, NoTls};

const PENCIL_VERIFIER: &str = "SCRAM-SHA-256$4096:W22ZaJ0SNY7soEsUEjb6gQ==$\
WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=:wfPLwcE6nTWhTAmQ7tl2KeoiWGPlZqQxSrmfPwDl2dU=";
const TENANTS: [&str; 3] = ["alice", "bob", "carol"];
const CLIENTS_PER_TENANT: usize = 10;
const LOAD: Duration = Duration::from_secs(3);

#[derive(Debug, Default)]
struct Served {
    committed: usize,
    failed: Vec<String>,
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

async fn through(proxy: SocketAddr, user: &str) -> Result<Client, tokio_postgres::Error> {
    let conn_str = format!(
        "host=127.0.0.1 port={} user={user} dbname=postgres password=pencil",
        proxy.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

async fn transaction(client: &Client) -> Result<(), tokio_postgres::Error> {
    client.simple_query("BEGIN").await?;
    client.simple_query("SELECT pg_sleep(0.005)").await?;
    client.simple_query("COMMIT").await?;
    Ok(())
}

fn refusal(error: &tokio_postgres::Error) -> String {
    error.as_db_error().map_or_else(
        || error.to_string(),
        |db| format!("{}: {}", db.code().code(), db.message()),
    )
}

async fn same_load(client: Result<Client, tokio_postgres::Error>, until: Instant) -> Served {
    let mut served = Served::default();
    let client = match client {
        Ok(client) => client,
        Err(error) => {
            served.failed.push(refusal(&error));
            return served;
        }
    };
    while Instant::now() < until {
        match transaction(&client).await {
            Ok(()) => served.committed += 1,
            Err(error) => {
                served.failed.push(refusal(&error));
                break;
            }
        }
    }
    served
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
async fn three_tenants_sharing_one_budget_are_all_served() {
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
            wait_timeout: Duration::from_secs(1),
            ..ServeOptions::default()
        },
    )
    .await
    .unwrap();

    let budget = serving.budget(&InstanceId::new(&instance));
    assert_eq!(
        budget, 10,
        "16 max_connections - 3 superuser reserved - 2 foreign - 1 monitor, the max_db_connections of pgbouncer#131"
    );
    let monitor = CapMonitor::start(
        &rt,
        PgStatActivity::new(observer, "pgsteward-proxy"),
        budget as usize,
        Duration::from_millis(20),
    );

    let proxy = serving.local_addr();
    let connecting: Vec<_> = TENANTS
        .iter()
        .flat_map(|&user| {
            (0..CLIENTS_PER_TENANT).map(move |_| (user, tokio::spawn(through(proxy, user))))
        })
        .collect();
    let mut connected = Vec::with_capacity(connecting.len());
    for (user, client) in connecting {
        connected.push((user, client.await.unwrap()));
    }
    let until = Instant::now() + LOAD;
    let loads: Vec<_> = connected
        .into_iter()
        .map(|(user, client)| (user, tokio::spawn(same_load(client, until))))
        .collect();
    let mut per_tenant: Vec<(&str, Served)> = TENANTS
        .iter()
        .map(|&user| (user, Served::default()))
        .collect();
    for (user, load) in loads {
        let served = load.await.unwrap();
        let (_, total) = per_tenant
            .iter_mut()
            .find(|(tenant, _)| *tenant == user)
            .unwrap();
        total.committed += served.committed;
        total.failed.extend(served.failed);
    }

    let report = monitor.stop().await;
    report.assert_never_exceeded();

    for (user, served) in &per_tenant {
        assert!(
            served.failed.is_empty(),
            "{user} was turned away while sharing the budget: {:?}",
            served.failed
        );
    }
    let committed: usize = per_tenant.iter().map(|(_, served)| served.committed).sum();
    let fair_share = committed / TENANTS.len();
    for (user, served) in &per_tenant {
        assert!(
            served.committed * 2 >= fair_share,
            "{user} committed {} of {committed}, less than half of an equal share: {per_tenant:?}",
            served.committed
        );
    }
}
