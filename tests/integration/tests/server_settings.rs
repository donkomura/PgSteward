use std::time::Duration;

use pgsteward_core::budget::ServerLimits;
use pgsteward_core::inspect::{
    count_foreign_connections, read_server_limits, read_timeout_settings,
};
use pgsteward_core::rt::tokio_rt::TokioRuntime;
use pgsteward_core::server::{ApplicationName, ServerCredentials, connect};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::NoTls;

fn credentials() -> ServerCredentials {
    ServerCredentials {
        user: "postgres".to_owned(),
        database: "postgres".to_owned(),
        password: None,
    }
}

async fn client(port: u16, application_name: &str) -> tokio_postgres::Client {
    let conn_str = format!(
        "host=127.0.0.1 port={port} user=postgres dbname=postgres application_name={application_name}"
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

#[tokio::test]
async fn the_server_limits_and_the_timeout_settings_are_read_as_the_server_reports_them() {
    let container = Postgres::default()
        .with_host_auth()
        .with_tag("16")
        .with_cmd([
            "postgres",
            "-c",
            "max_connections=123",
            "-c",
            "superuser_reserved_connections=5",
            "-c",
            "tcp_keepalives_idle=45",
            "-c",
            "tcp_user_timeout=20s",
        ])
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();

    let mut conn = connect(
        &TokioRuntime::new(),
        &format!("127.0.0.1:{port}"),
        &credentials(),
        &ApplicationName::new("integration"),
    )
    .await
    .unwrap();

    let limits = read_server_limits(&mut conn).await.unwrap();
    assert_eq!(
        limits,
        ServerLimits {
            max_connections: 123,
            superuser_reserved_connections: 5,
            reserved_connections: 0,
        }
    );

    let timeouts = read_timeout_settings(&mut conn).await.unwrap();
    assert_eq!(timeouts.tcp_keepalives_idle, Duration::from_secs(45));
    assert_eq!(timeouts.tcp_user_timeout, Duration::from_secs(20));

    conn.terminate().await.unwrap();
}

#[tokio::test]
async fn the_foreign_connections_leave_out_the_ones_this_system_opened() {
    let container = Postgres::default()
        .with_host_auth()
        .with_tag("16")
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();

    let mut conn = connect(
        &TokioRuntime::new(),
        &format!("127.0.0.1:{port}"),
        &credentials(),
        &ApplicationName::new("integration"),
    )
    .await
    .unwrap();

    let alone = count_foreign_connections(&mut conn).await.unwrap();

    let _held = [client(port, "psql").await, client(port, "monitoring").await];
    let mut crowded = 0;
    for _ in 0..50 {
        crowded = count_foreign_connections(&mut conn).await.unwrap();
        if crowded == alone + 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert_eq!(crowded, alone + 2);
    conn.terminate().await.unwrap();
}
