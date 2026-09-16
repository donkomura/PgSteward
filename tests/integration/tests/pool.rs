use std::collections::BTreeSet;
use std::time::Duration;

use pgsteward_core::pool::{InstanceOpener, Pool, PoolLimits};
use pgsteward_core::rt::tokio_rt::TokioRuntime;
use pgsteward_core::server::{ApplicationName, ServerCredentials, SimpleQuery};
use pgsteward_harness::cap::CapMonitor;
use pgsteward_harness::pg_stat_activity::PgStatActivity;
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::{Client, NoTls};

const IDENTIFIER: &str = "pool";
const SLOTS: usize = 2;
const CLIENTS: usize = 8;

fn credentials() -> ServerCredentials {
    ServerCredentials {
        user: "postgres".to_owned(),
        database: "postgres".to_owned(),
        password: None,
    }
}

async fn observer(port: u16) -> Client {
    let conn_str = format!(
        "host=127.0.0.1 port={port} user=postgres dbname=postgres application_name=harness-observer"
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

#[tokio::test]
async fn the_pool_serves_more_clients_than_it_has_slots_without_exceeding_them() {
    let container = Postgres::default()
        .with_host_auth()
        .with_tag("16")
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let application_name = ApplicationName::new(IDENTIFIER);
    let rt = TokioRuntime::new();
    let monitor = CapMonitor::start(
        &rt,
        PgStatActivity::new(observer(port).await, application_name.to_string()),
        SLOTS,
        Duration::from_millis(20),
    );

    let pool = Pool::new(
        InstanceOpener::new(
            rt,
            format!("127.0.0.1:{port}"),
            credentials(),
            application_name,
        ),
        rt,
        PoolLimits {
            slots: SLOTS,
            wait_timeout: Duration::from_secs(5),
        },
    );

    let clients: Vec<_> = (0..CLIENTS)
        .map(|_| {
            let pool = pool.clone();
            tokio::spawn(async move {
                let mut assigned = pool.acquire().await.unwrap();
                let rows = assigned
                    .simple_query("SELECT pg_backend_pid()")
                    .await
                    .unwrap();
                rows[0][0].clone().unwrap()
            })
        })
        .collect();
    let mut backends = BTreeSet::new();
    for client in clients {
        backends.insert(client.await.unwrap());
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let report = monitor.stop().await;
    report.assert_never_exceeded();
    assert_eq!(report.peak(), SLOTS);
    assert!(
        backends.len() <= SLOTS,
        "{CLIENTS} clients ran on {} server connections: {backends:?}",
        backends.len()
    );
    assert_eq!(pool.stats().actual(), SLOTS);
    assert_eq!(pool.stats().in_use, 0);
}
