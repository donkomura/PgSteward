use std::net::TcpListener;
use std::time::Duration;

use pgsteward_core::allocation::InstanceId;
use pgsteward_core::rt::tokio_rt::TokioRuntime;
use pgsteward_harness::cap::CapMonitor;
use pgsteward_harness::pg_stat_activity::PgStatActivity;
use pgsteward_node::config::{ClusterConfig, NodeConfig};
use pgsteward_node::serve::{ServeOptions, serve};
use testcontainers::core::wait::WaitFor;
use testcontainers::core::{ExecCommand, Host};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::{Client, NoTls};

const PENCIL_VERIFIER: &str = "SCRAM-SHA-256$4096:W22ZaJ0SNY7soEsUEjb6gQ==$\
WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=:wfPLwcE6nTWhTAmQ7tl2KeoiWGPlZqQxSrmfPwDl2dU=";
const TENANT: &str = "bench";
const HOST: &str = "host.docker.internal";
const CLIENTS: usize = 200;
const THREADS: usize = 8;
const SECONDS: u64 = 60;
const ROUNDS: usize = 5;
const POOL_SIZE: u32 = 20;
const PGBOUNCER_IMAGE: &str = "edoburu/pgbouncer";
const PGBOUNCER_TAG: &str = "v1.25.2-p0";

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

fn free_port() -> u16 {
    TcpListener::bind("0.0.0.0:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
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

async fn database() -> (ContainerAsync<Postgres>, u16, Client) {
    let container = Postgres::default()
        .with_host_auth()
        .with_tag("16")
        .with_cmd(["postgres", "-c", "max_connections=25"])
        .with_host(HOST, Host::HostGateway)
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let observer = direct(port, "harness-observer").await;
    observer
        .simple_query(&format!("CREATE ROLE {TENANT} LOGIN"))
        .await
        .unwrap();
    observer
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
    (container, port, observer)
}

fn tps(stdout: &str) -> f64 {
    stdout
        .lines()
        .find_map(|line| line.strip_prefix("tps = "))
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| panic!("pgbench reported no tps:\n{stdout}"))
}

async fn pgbench(container: &ContainerAsync<Postgres>, proxy: u16) -> f64 {
    let run = shell(
        container,
        format!(
            "PGPASSWORD=pencil pgbench -h {HOST} -p {proxy} -U {TENANT} \
             -c {CLIENTS} -j {THREADS} -T {SECONDS} {TENANT}"
        ),
    )
    .await;
    assert_eq!(
        run.exit_code,
        Some(0),
        "pgbench did not complete:\n{}\n{}",
        run.stdout,
        run.stderr
    );
    assert!(
        run.stdout
            .lines()
            .any(|line| line.starts_with("number of failed transactions: 0 ")),
        "{}",
        run.stdout
    );
    tps(&run.stdout)
}

fn pgbouncer_ini(database: u16, listen: u16) -> String {
    format!(
        r"[databases]
{TENANT} = host=127.0.0.1 port={database} dbname={TENANT} user={TENANT}

[pgbouncer]
listen_addr = 0.0.0.0
listen_port = {listen}
auth_type = scram-sha-256
auth_file = /etc/pgbouncer/userlist.txt
pool_mode = transaction
default_pool_size = {POOL_SIZE}
max_client_conn = 250
"
    )
}

async fn through_pgbouncer() -> f64 {
    let (container, port, _observer) = database().await;
    let listen = free_port();
    let _pgbouncer = GenericImage::new(PGBOUNCER_IMAGE, PGBOUNCER_TAG)
        .with_wait_for(WaitFor::message_on_stderr("process up"))
        .with_network("host")
        .with_copy_to(
            "/etc/pgbouncer/pgbouncer.ini",
            pgbouncer_ini(port, listen).into_bytes(),
        )
        .with_copy_to(
            "/etc/pgbouncer/userlist.txt",
            format!("\"{TENANT}\" \"{PENCIL_VERIFIER}\"\n").into_bytes(),
        )
        .start()
        .await
        .unwrap();
    pgbench(&container, listen).await
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

async fn through_pgsteward() -> f64 {
    let (container, port, observer) = database().await;
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
    assert_eq!(
        serving.budget(&InstanceId::new(&instance)),
        POOL_SIZE,
        "25 max_connections - 3 superuser reserved - 1 foreign - 1 monitor"
    );
    let monitor = CapMonitor::start(
        &rt,
        PgStatActivity::new(observer, "pgsteward-proxy"),
        usize::try_from(POOL_SIZE).unwrap(),
        Duration::from_millis(20),
    );
    let tps = pgbench(&container, serving.local_addr().port()).await;
    monitor.stop().await.assert_never_exceeded();
    tps
}

fn median(mut samples: Vec<f64>) -> f64 {
    samples.sort_by(f64::total_cmp);
    samples[samples.len() / 2]
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark; run with `cargo test --release -p pgsteward-bench -- --ignored --nocapture`"]
async fn throughput_is_at_least_pgbouncers_under_the_same_pool_size() {
    let mut pgbouncer = Vec::new();
    let mut pgsteward = Vec::new();
    for round in 1..=ROUNDS {
        pgbouncer.push(through_pgbouncer().await);
        pgsteward.push(through_pgsteward().await);
        println!(
            "round {round}: PgBouncer {:.1} tps, PgSteward {:.1} tps",
            pgbouncer[round - 1],
            pgsteward[round - 1]
        );
    }
    let pgbouncer = median(pgbouncer);
    let pgsteward = median(pgsteward);
    println!(
        "pgbench -c {CLIENTS} -j {THREADS} -T {SECONDS}, pool size {POOL_SIZE}, median of {ROUNDS}"
    );
    println!("PgBouncer {PGBOUNCER_TAG}: {pgbouncer:.1} tps");
    println!(
        "PgSteward: {pgsteward:.1} tps ({:.1}% of PgBouncer)",
        pgsteward / pgbouncer * 100.0
    );
    assert!(
        pgsteward >= pgbouncer,
        "PgSteward {pgsteward:.1} tps is below PgBouncer {pgbouncer:.1} tps"
    );
}
