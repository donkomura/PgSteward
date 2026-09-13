use std::time::Duration;

use pgsteward_core::rt::tokio_rt::TokioRuntime;
use pgsteward_core::server::{
    ApplicationName, ConnectError, HandshakeError, ServerCredentials, connect,
};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::NoTls;

async fn observer(port: u16) -> tokio_postgres::Client {
    let conn_str = format!(
        "host=127.0.0.1 port={port} user=postgres password=postgres dbname=postgres application_name=harness-observer"
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

async fn count_named(client: &tokio_postgres::Client, application_name: &str) -> i64 {
    client
        .query_one(
            "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1",
            &[&application_name],
        )
        .await
        .unwrap()
        .get(0)
}

async fn wait_for_count(client: &tokio_postgres::Client, application_name: &str, expected: i64) {
    for _ in 0..50 {
        if count_named(client, application_name).await == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!(
        "expected {expected} connection(s) named {application_name}, saw {}",
        count_named(client, application_name).await
    );
}

fn credentials(database: &str) -> ServerCredentials {
    ServerCredentials {
        user: "postgres".to_owned(),
        database: database.to_owned(),
        password: None,
    }
}

#[tokio::test]
async fn handshake_with_trust_auth_tags_the_connection_and_keeps_the_startup_parameters() {
    let container = Postgres::default()
        .with_host_auth()
        .with_tag("16")
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let observer = observer(port).await;

    let conn = connect(
        &TokioRuntime::new(),
        &format!("127.0.0.1:{port}"),
        &credentials("postgres"),
        &ApplicationName::new("integration"),
    )
    .await
    .unwrap();

    assert!(
        conn.parameter("server_version")
            .is_some_and(|version| version.starts_with("16")),
        "{:?}",
        conn.parameters()
    );
    assert!(conn.backend_key().process_id > 0);
    wait_for_count(&observer, "pgsteward-integration", 1).await;

    conn.terminate().await.unwrap();
    wait_for_count(&observer, "pgsteward-integration", 0).await;
}

#[tokio::test]
async fn unknown_database_is_refused_with_the_server_sqlstate() {
    let container = Postgres::default()
        .with_host_auth()
        .with_tag("16")
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();

    let err = connect(
        &TokioRuntime::new(),
        &format!("127.0.0.1:{port}"),
        &credentials("no_such_database"),
        &ApplicationName::new("integration"),
    )
    .await
    .unwrap_err();

    match err {
        ConnectError::Handshake(HandshakeError::Server { code, message }) => {
            assert_eq!(code, "3D000");
            assert!(message.contains("no_such_database"), "{message}");
        }
        other => panic!("expected a server error, got {other:?}"),
    }
}
