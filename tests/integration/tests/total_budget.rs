use std::time::Duration;

use pgsteward_core::budget::{BudgetChange, InstanceBudget};
use pgsteward_core::inspect::{count_foreign_connections, read_server_limits};
use pgsteward_core::rt::Instant;
use pgsteward_core::rt::tokio_rt::TokioRuntime;
use pgsteward_core::server::{ApplicationName, ServerCredentials, connect};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::NoTls;

const MARGIN: u32 = 10;

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
async fn the_total_budget_shrinks_by_the_foreign_connections_the_server_reports() {
    let container = Postgres::default()
        .with_host_auth()
        .with_tag("16")
        .with_cmd([
            "postgres",
            "-c",
            "max_connections=123",
            "-c",
            "superuser_reserved_connections=5",
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
    let mut budget = InstanceBudget::new(limits, MARGIN, Duration::from_secs(60));

    let alone = count_foreign_connections(&mut conn).await.unwrap();
    budget.observe(Instant::now(), alone);
    let before = budget.current().total();
    assert_eq!(before, 123 - 5 - alone - MARGIN);

    let _held = [client(port, "psql").await, client(port, "monitoring").await];
    let mut crowded = alone;
    for _ in 0..50 {
        crowded = count_foreign_connections(&mut conn).await.unwrap();
        if crowded == alone + 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(crowded, alone + 2);

    assert_eq!(
        budget.observe(Instant::now(), crowded),
        BudgetChange::Shrank {
            from: before,
            to: before - 2,
        }
    );
    assert_eq!(budget.current().inputs().foreign_peak, crowded);

    conn.terminate().await.unwrap();
}
