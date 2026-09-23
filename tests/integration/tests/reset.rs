use std::net::SocketAddr;
use std::time::Duration;

use pgsteward_core::allocation::InstanceId;
use pgsteward_core::auth::TrustAll;
use pgsteward_core::cancel::{CancelRegistry, ProxyTag};
use pgsteward_core::pool::{InstanceOpener, Pool, PoolLimits};
use pgsteward_core::relay::{Welcome, transaction_mode};
use pgsteward_core::rt::tokio_rt::TokioRuntime;
use pgsteward_core::rt::{Net, Spawner};
use pgsteward_core::server::{ApplicationName, ServerCredentials};
use pgsteward_core::session::{Accepted, accept};
use pgsteward_harness::server_tls;
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

const IDENTIFIER: &str = "state-reset";
const SLOTS: usize = 1;
const WAIT_TIMEOUT: Duration = Duration::from_secs(10);

/// The four traces a client can leave on a server connection, each with the
/// query that would find it and the answer that says it is gone.
const TRACES: [(&str, &str, &str); 4] = [
    (
        "SET search_path TO leaked",
        "SHOW search_path",
        "\"$user\", public",
    ),
    (
        "CREATE TEMP TABLE leaked (n int)",
        "SELECT count(*) FROM pg_tables WHERE tablename = 'leaked'",
        "0",
    ),
    (
        "PREPARE leaked AS SELECT 1",
        "SELECT count(*) FROM pg_prepared_statements",
        "0",
    ),
    (
        "SELECT pg_advisory_lock(42)",
        "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory'",
        "0",
    ),
];

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
        InstanceOpener::new(
            serve_rt,
            server_addr,
            credentials(),
            application_name,
            server_tls(),
        ),
        serve_rt,
        PoolLimits {
            slots: SLOTS,
            wait_timeout: WAIT_TIMEOUT,
        },
    );
    let welcome = Welcome::default();
    let cancels = CancelRegistry::new(ProxyTag::new(1).unwrap());
    let instance = InstanceId::new("postgres");
    rt.spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let pool = pool.clone();
            let welcome = welcome.clone();
            let cancels = cancels.clone();
            let instance = instance.clone();
            serve_rt.spawn(async move {
                let Accepted::Session(session) = accept(stream, TrustAll, None).await.unwrap()
                else {
                    panic!("expected an authenticated session");
                };
                transaction_mode(session, &pool, &welcome, &cancels, &instance)
                    .await
                    .unwrap();
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

async fn single_value(client: &Client, sql: &str) -> String {
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

#[tokio::test]
async fn nothing_a_client_leaves_on_a_connection_reaches_the_client_that_gets_it_next() {
    let container = Postgres::default()
        .with_host_auth()
        .with_tag("16")
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let rt = TokioRuntime::new();
    let proxy = start_proxy(
        &rt,
        format!("127.0.0.1:{port}"),
        ApplicationName::new(IDENTIFIER),
    )
    .await;

    let first = connect_through(proxy).await;
    let backend = single_value(&first, "SELECT pg_backend_pid()").await;
    for (leave, _, _) in TRACES {
        first.simple_query(leave).await.unwrap();
    }

    let second = connect_through(proxy).await;

    assert_eq!(
        single_value(&second, "SELECT pg_backend_pid()").await,
        backend,
        "the only slot must hand the same server connection to the next client"
    );
    for (leave, find, gone) in TRACES {
        assert_eq!(
            single_value(&second, find).await,
            gone,
            "the trace of `{leave}` outlived the reset"
        );
    }
}
