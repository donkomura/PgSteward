use std::time::Duration;

use pgsteward_core::rt::tokio_rt::TokioRuntime;
use pgsteward_harness::cap::CapMonitor;
use pgsteward_harness::pg_stat_activity::PgStatActivity;
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::NoTls;

async fn connect(port: u16, application_name: &str) -> tokio_postgres::Client {
    let conn_str = format!(
        "host=127.0.0.1 port={port} user=postgres password=postgres dbname=postgres application_name={application_name}"
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

#[tokio::test]
async fn counts_only_connections_tagged_as_pgsteward() {
    let container = Postgres::default().with_tag("16").start().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();

    let observer_client = connect(port, "harness-observer").await;
    let observer = PgStatActivity::new(observer_client, "pgsteward-");

    let rt = TokioRuntime::new();
    let monitor = CapMonitor::start(&rt, observer, 2, Duration::from_millis(50));

    let held = [
        connect(port, "pgsteward-test-1").await,
        connect(port, "pgsteward-test-2").await,
    ];
    let _unrelated = connect(port, "psql-admin").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(held);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let report = monitor.stop().await;
    assert_eq!(report.peak(), 2);
    report.assert_never_exceeded();
}

#[tokio::test]
async fn reports_violation_when_more_pgsteward_connections_than_cap() {
    let container = Postgres::default().with_tag("16").start().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();

    let observer_client = connect(port, "harness-observer").await;
    let observer = PgStatActivity::new(observer_client, "pgsteward-");

    let rt = TokioRuntime::new();
    let monitor = CapMonitor::start(&rt, observer, 1, Duration::from_millis(50));

    let _held = [
        connect(port, "pgsteward-test-1").await,
        connect(port, "pgsteward-test-2").await,
    ];
    tokio::time::sleep(Duration::from_millis(300)).await;

    let report = monitor.stop().await;
    assert_eq!(report.peak(), 2);
    assert!(!report.violations().is_empty());
}
