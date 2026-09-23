use std::net::SocketAddr;

use pgsteward_core::auth::TrustAll;
use pgsteward_core::relay::session_mode;
use pgsteward_core::rt::tokio_rt::TokioRuntime;
use pgsteward_core::rt::{Net, Spawner};
use pgsteward_core::server::{ApplicationName, ServerCredentials, connect};
use pgsteward_core::session::{Accepted, accept};
use pgsteward_harness::server_tls;
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, NoTls};

const IDENTIFIER: &str = "session-mode";

fn credentials() -> ServerCredentials {
    ServerCredentials {
        user: "postgres".to_owned(),
        database: "postgres".to_owned(),
        password: None,
    }
}

async fn start_proxy(rt: &TokioRuntime, server_addr: String) -> SocketAddr {
    let listener = rt.bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let serve_rt = *rt;
    rt.spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let server_addr = server_addr.clone();
            serve_rt.spawn(async move {
                let Accepted::Session(session) = accept(stream, TrustAll, None).await.unwrap()
                else {
                    panic!("expected an authenticated session");
                };
                let server = connect(
                    &serve_rt,
                    &server_addr,
                    &credentials(),
                    &ApplicationName::new(IDENTIFIER),
                    &server_tls(),
                )
                .await
                .unwrap();
                session_mode(session, server).await.unwrap();
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
        "host=127.0.0.1 port={port} user=postgres password=postgres dbname=postgres application_name=harness-observer"
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

async fn count_named(client: &Client, application_name: &str) -> i64 {
    client
        .query_one(
            "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1",
            &[&application_name],
        )
        .await
        .unwrap()
        .get(0)
}

#[tokio::test]
async fn a_session_keeps_its_state_on_one_server_connection() {
    let container = Postgres::default()
        .with_host_auth()
        .with_tag("16")
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let observer = observer(port).await;
    let rt = TokioRuntime::new();
    let proxy = start_proxy(&rt, format!("127.0.0.1:{port}")).await;

    let client = connect_through(proxy).await;
    let one: i32 = client.query_one("SELECT 1", &[]).await.unwrap().get(0);
    assert_eq!(one, 1);

    client
        .simple_query("CREATE TEMP TABLE probe (value int)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO probe VALUES (7)")
        .await
        .unwrap();
    let value: i32 = client
        .query_one("SELECT value FROM probe", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        value, 7,
        "a session must see the temporary table it created, so it must stay on one server connection"
    );

    assert_eq!(
        count_named(&observer, &ApplicationName::new(IDENTIFIER).to_string()).await,
        1,
        "one client session must hold exactly one server connection"
    );
}

#[tokio::test]
async fn an_error_from_the_server_reaches_the_client_and_the_session_survives() {
    let container = Postgres::default()
        .with_host_auth()
        .with_tag("16")
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let rt = TokioRuntime::new();
    let proxy = start_proxy(&rt, format!("127.0.0.1:{port}")).await;

    let client = connect_through(proxy).await;
    let error = client
        .query_one("SELECT * FROM missing", &[])
        .await
        .expect_err("the server refuses a query on a missing table");
    assert_eq!(error.code(), Some(&SqlState::UNDEFINED_TABLE));

    let one: i32 = client.query_one("SELECT 1", &[]).await.unwrap().get(0);
    assert_eq!(one, 1);
}
