use std::time::Duration;

use pgsteward_core::allocation::InstanceId;
use pgsteward_core::rt::tokio_rt::TokioRuntime;
use pgsteward_harness::cap::CapMonitor;
use pgsteward_harness::pg_stat_activity::PgStatActivity;
use pgsteward_node::config::{ClusterConfig, NodeConfig};
use pgsteward_node::serve::{ServeOptions, serve};
use testcontainers::core::{ExecCommand, Host};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::{Client, NoTls};

const PENCIL_VERIFIER: &str = "SCRAM-SHA-256$4096:W22ZaJ0SNY7soEsUEjb6gQ==$\
WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=:wfPLwcE6nTWhTAmQ7tl2KeoiWGPlZqQxSrmfPwDl2dU=";
const TENANT: &str = "bench";
const HOST: &str = "host.docker.internal";
const CLIENTS: usize = 200;
const THREADS: usize = 8;
const SECONDS: u64 = 5;

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

fn node_config() -> NodeConfig {
    NodeConfig::parse(&format!(
        r#"
[node]
role = "proxy"
listen = "0.0.0.0:0"
coordinator = "127.0.0.1:7432"
max_client_connections = 250

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

struct Run {
    exit_code: Option<i64>,
    stdout: String,
    stderr: String,
}

async fn shell(container: &ContainerAsync<Postgres>, script: String) -> Run {
    let mut exec = container
        .exec(ExecCommand::new(["sh", "-c", &script]))
        .await
        .unwrap();
    let stdout = String::from_utf8_lossy(&exec.stdout_to_vec().await.unwrap()).into_owned();
    let stderr = String::from_utf8_lossy(&exec.stderr_to_vec().await.unwrap()).into_owned();
    Run {
        exit_code: exec.exit_code().await.unwrap(),
        stdout,
        stderr,
    }
}

fn processed(stdout: &str) -> u64 {
    stdout
        .lines()
        .find_map(|line| line.strip_prefix("number of transactions actually processed: "))
        .and_then(|rest| rest.split('/').next())
        .and_then(|count| count.trim().parse().ok())
        .unwrap_or_else(|| panic!("pgbench reported no processed transactions:\n{stdout}"))
}

async fn pgbench_completes(protocol: &str) {
    let container = Postgres::default()
        .with_host_auth()
        .with_tag("16")
        .with_cmd(["postgres", "-c", "max_connections=26"])
        .with_host(HOST, Host::HostGateway)
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let admin = direct(port, "harness-admin").await;
    admin
        .simple_query(&format!("CREATE ROLE {TENANT} LOGIN"))
        .await
        .unwrap();
    admin
        .simple_query(&format!("CREATE DATABASE {TENANT} OWNER {TENANT}"))
        .await
        .unwrap();
    let init = shell(&container, format!("pgbench -i -q -U {TENANT} {TENANT}")).await;
    assert_eq!(
        init.exit_code,
        Some(0),
        "pgbench -i failed: {}",
        init.stderr
    );

    let observer = direct(port, "harness-observer").await;
    let instance = format!("127.0.0.1:{port}");
    let rt = TokioRuntime::new();
    let serving = serve(
        rt,
        node_config(),
        cluster_config(&instance),
        ServeOptions {
            margin: 0,
            wait_timeout: Duration::from_secs(60),
            ..ServeOptions::default()
        },
    )
    .await
    .unwrap();

    let budget = serving.budget(&InstanceId::new(&instance));
    assert_eq!(
        budget, 20,
        "26 max_connections - 3 superuser reserved - 2 foreign - 1 monitor, the default_pool_size of the comparison"
    );
    let monitor = CapMonitor::start(
        &rt,
        PgStatActivity::new(observer, "pgsteward-proxy"),
        budget as usize,
        Duration::from_millis(20),
    );

    let proxy = serving.local_addr().port();
    let run = shell(
        &container,
        format!(
            "PGPASSWORD=pencil pgbench -h {HOST} -p {proxy} -U {TENANT} \
             -M {protocol} -c {CLIENTS} -j {THREADS} -T {SECONDS} {TENANT}"
        ),
    )
    .await;

    let report = monitor.stop().await;
    report.assert_never_exceeded();

    assert_eq!(
        run.exit_code,
        Some(0),
        "pgbench -M {protocol} did not complete:\n{}\n{}",
        run.stdout,
        run.stderr
    );
    assert!(
        run.stdout
            .lines()
            .any(|line| line == format!("number of clients: {CLIENTS}")),
        "{}",
        run.stdout
    );
    assert!(
        run.stdout
            .lines()
            .any(|line| line.starts_with("number of failed transactions: 0 ")),
        "{}",
        run.stdout
    );
    assert!(processed(&run.stdout) > 0, "{}", run.stdout);
}

#[tokio::test]
async fn pgbench_completes_over_the_simple_query_protocol() {
    pgbench_completes("simple").await;
}

#[tokio::test]
async fn pgbench_completes_over_the_extended_query_protocol() {
    pgbench_completes("extended").await;
}
