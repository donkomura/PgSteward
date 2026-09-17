use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::time::Duration;

use pgsteward_core::auth::TrustAll;
use pgsteward_core::pool::{InstanceOpener, Pool, PoolLimits};
use pgsteward_core::relay::{Welcome, transaction_mode};
use pgsteward_core::rt::tokio_rt::TokioRuntime;
use pgsteward_core::rt::{Net, Spawner};
use pgsteward_core::server::{ApplicationName, ServerCredentials};
use pgsteward_core::session::{Accepted, accept};
use pgsteward_harness::cap::CapMonitor;
use pgsteward_harness::pg_stat_activity::PgStatActivity;
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

const IDENTIFIER: &str = "transaction-mode";
const SLOTS: usize = 2;
const CLIENTS: usize = 8;

fn credentials() -> ServerCredentials {
    ServerCredentials {
        user: "postgres".to_owned(),
        database: "postgres".to_owned(),
        password: None,
    }
}

async fn start_proxy(
    rt: &TokioRuntime,
    server_addr: String,
    application_name: ApplicationName,
) -> SocketAddr {
    let listener = rt.bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let serve_rt = *rt;
    let pool = Pool::new(
        InstanceOpener::new(serve_rt, server_addr, credentials(), application_name),
        serve_rt,
        PoolLimits {
            slots: SLOTS,
            wait_timeout: Duration::from_secs(10),
        },
    );
    let welcome = Welcome::default();
    rt.spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let pool = pool.clone();
            let welcome = welcome.clone();
            serve_rt.spawn(async move {
                let Accepted::Session(session) = accept(stream, TrustAll).await.unwrap() else {
                    panic!("expected an authenticated session");
                };
                transaction_mode(session, &pool, &welcome).await.unwrap();
            });
        }
    });
    addr
}

async fn connect_through(addr: SocketAddr) -> Client {
    let conn_str = format!(
        "host=127.0.0.1 port={} user=postgres dbname=postgres",
        addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
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

async fn backend_pid(client: &Client) -> String {
    let rows = client
        .simple_query("SELECT pg_backend_pid()")
        .await
        .unwrap();
    rows.iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(row.get(0).expect("a backend pid").to_owned()),
            _ => None,
        })
        .expect("a row")
}

#[tokio::test]
async fn more_clients_than_slots_run_their_transactions_without_exceeding_them() {
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
    let proxy = start_proxy(&rt, format!("127.0.0.1:{port}"), application_name).await;

    let clients: Vec<_> = (0..CLIENTS)
        .map(|_| {
            tokio::spawn(async move {
                let client = connect_through(proxy).await;
                client.simple_query("BEGIN").await.unwrap();
                let pid = backend_pid(&client).await;
                tokio::time::sleep(Duration::from_millis(20)).await;
                assert_eq!(
                    backend_pid(&client).await,
                    pid,
                    "a transaction must stay on the server connection it started on"
                );
                client.simple_query("COMMIT").await.unwrap();
                pid
            })
        })
        .collect();
    let mut backends = BTreeSet::new();
    for client in clients {
        backends.insert(client.await.unwrap());
    }

    let report = monitor.stop().await;
    report.assert_never_exceeded();
    assert_eq!(report.peak(), SLOTS);
    assert!(
        backends.len() <= SLOTS,
        "{CLIENTS} clients ran on {} server connections: {backends:?}",
        backends.len()
    );
}
