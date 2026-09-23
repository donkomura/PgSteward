use std::sync::{Arc, Mutex};

use pgsteward_core::rt::turmoil_rt::TurmoilRuntime;
use pgsteward_core::server::{
    ApplicationName, ConnectError, ServerCredentials, SimpleQuery, connect,
};
use pgsteward_core::tls::{ServerTls, SslMode};
use pgsteward_harness::fake_postgres::{FakePostgres, FakePostgresStats};

const IDENTIFIER: &str = "fake-postgres";

fn credentials() -> ServerCredentials {
    ServerCredentials {
        user: "postgres".to_owned(),
        database: "postgres".to_owned(),
        password: None,
    }
}

fn start_db(sim: &mut turmoil::Sim<'_>, stats: FakePostgresStats) {
    sim.host("db", move || {
        let stats = stats.clone();
        async move {
            let rt = TurmoilRuntime::new();
            FakePostgres::start(&rt, "0.0.0.0:5432", stats).await?;
            std::future::pending::<()>().await;
            Ok(())
        }
    });
}

#[test]
fn the_fake_postgres_answers_the_startup_packet() {
    let stats = FakePostgresStats::default();
    let parameters: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let mut sim = turmoil::Builder::new().build();
    start_db(&mut sim, stats.clone());

    let seen = Arc::clone(&parameters);
    sim.client("app", async move {
        let rt = TurmoilRuntime::new();
        let server = connect(
            &rt,
            "db:5432",
            &credentials(),
            &ApplicationName::new(IDENTIFIER),
        )
        .await?;
        assert_ne!(
            server.backend_key().process_id,
            0,
            "the handshake must carry a BackendKeyData"
        );
        *seen.lock().unwrap() = server
            .parameters()
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        Ok(())
    });

    sim.run().unwrap();

    let parameters = parameters.lock().unwrap();
    assert!(
        parameters.iter().any(|(name, _)| name == "server_version"),
        "the handshake must carry the server version: {parameters:?}"
    );
    assert_eq!(stats.accepted(), 1);
}

#[test]
fn every_server_connection_reports_its_own_backend() {
    let stats = FakePostgresStats::default();
    let backends: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let mut sim = turmoil::Builder::new().build();
    start_db(&mut sim, stats.clone());

    let reported = Arc::clone(&backends);
    sim.client("app", async move {
        let rt = TurmoilRuntime::new();
        let application_name = ApplicationName::new(IDENTIFIER);
        for _ in 0..2 {
            let mut server = connect(&rt, "db:5432", &credentials(), &application_name).await?;
            let rows = server
                .simple_query("SELECT pg_backend_pid()")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            reported
                .lock()
                .unwrap()
                .push(rows[0][0].clone().expect("a backend identifier"));
        }
        Ok(())
    });

    sim.run().unwrap();

    let backends = backends.lock().unwrap();
    assert_eq!(backends.len(), 2);
    assert_ne!(
        backends[0], backends[1],
        "two server connections must not report the same backend: {backends:?}"
    );
}

#[test]
fn the_fake_postgres_refuses_encryption_and_answers_the_startup_packet_after_it() {
    let stats = FakePostgresStats::default();
    let mut sim = turmoil::Builder::new().build();
    start_db(&mut sim, stats.clone());

    sim.client("app", async move {
        let rt = TurmoilRuntime::new();
        let tls = ServerTls::new(SslMode::Prefer, None).unwrap();
        let server = connect(
            &rt,
            "db:5432",
            &credentials(),
            &ApplicationName::new(IDENTIFIER),
            &tls,
        )
        .await?;
        assert!(
            server.parameter("server_version").is_some(),
            "the startup goes on in the clear after the refusal"
        );
        Ok(())
    });

    sim.run().unwrap();

    assert_eq!(stats.accepted(), 1);
}

#[test]
fn a_connection_that_demands_encryption_does_not_reach_the_fake_postgres_startup() {
    let stats = FakePostgresStats::default();
    let mut sim = turmoil::Builder::new().build();
    start_db(&mut sim, stats.clone());

    sim.client("app", async move {
        let rt = TurmoilRuntime::new();
        let tls = ServerTls::new(SslMode::Require, None).unwrap();
        let error = connect(
            &rt,
            "db:5432",
            &credentials(),
            &ApplicationName::new(IDENTIFIER),
            &tls,
        )
        .await
        .expect_err("the fake PostgreSQL does not encrypt");
        assert!(
            matches!(error, ConnectError::EncryptionRefused),
            "expected the refusal to be reported, got {error}"
        );
        Ok(())
    });

    sim.run().unwrap();
}
