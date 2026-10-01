use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::BytesMut;
use pgsteward_core::budget::ServerLimits;
use pgsteward_core::inspect::{count_foreign_connections, read_server_limits};
use pgsteward_core::rt::{Clock, Net, turmoil_rt::TurmoilRuntime};
use pgsteward_core::server::{
    ApplicationName, ConnectError, QueryError, ServerCredentials, SimpleQuery, connect,
};
use pgsteward_core::tls::{ServerTls, SslMode};
use pgsteward_harness::fake_postgres::{FakePostgres, FakePostgresStats};
use pgsteward_protocol::backend::sqlstate;
use pgsteward_protocol::framing::decode_frame;
use pgsteward_protocol::startup::{
    ProtocolVersion, StartupMessage, StartupRequest, encode_startup,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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

fn start_db_with_limits(
    sim: &mut turmoil::Sim<'_>,
    stats: FakePostgresStats,
    limits: ServerLimits,
) {
    sim.host("db", move || {
        let stats = stats.clone();
        async move {
            let rt = TurmoilRuntime::new();
            FakePostgres::start_with_limits(&rt, "0.0.0.0:5432", stats, limits).await?;
            std::future::pending::<()>().await;
            Ok(())
        }
    });
}

async fn monitor(rt: &TurmoilRuntime) -> Result<impl SimpleQuery, ConnectError> {
    connect(
        rt,
        "db:5432",
        &credentials(),
        &ApplicationName::new(IDENTIFIER),
        &ServerTls::disabled(),
    )
    .await
}

/// Logs in the way a client of the database that is not this system does,
/// with whatever `application_name` it sets, or none.
async fn open_foreign(
    rt: &TurmoilRuntime,
    application_name: Option<&str>,
) -> turmoil::Result<<TurmoilRuntime as Net>::Stream> {
    let mut stream = rt.connect("db:5432").await?;
    let mut parameters = vec![
        ("user".to_owned(), "postgres".to_owned()),
        ("database".to_owned(), "postgres".to_owned()),
    ];
    if let Some(name) = application_name {
        parameters.push(("application_name".to_owned(), name.to_owned()));
    }
    let mut out = BytesMut::new();
    encode_startup(
        &StartupRequest::Startup(StartupMessage::new(ProtocolVersion::V3_0, parameters)),
        &mut out,
    );
    stream.write_all(&out).await?;
    let mut buf = BytesMut::new();
    loop {
        while let Some(frame) = decode_frame(&mut buf, 1 << 20)? {
            if frame.tag == b'Z' {
                return Ok(stream);
            }
        }
        assert!(
            stream.read_buf(&mut buf).await? > 0,
            "the fake PostgreSQL closed during the startup"
        );
    }
}

#[test]
fn the_fake_postgres_shows_the_limits_it_was_started_with() {
    let limits = ServerLimits {
        max_connections: 20,
        superuser_reserved_connections: 3,
        reserved_connections: 2,
    };
    let read: Arc<Mutex<Option<ServerLimits>>> = Arc::new(Mutex::new(None));
    let mut sim = turmoil::Builder::new().build();
    start_db_with_limits(&mut sim, FakePostgresStats::default(), limits);

    let shown = Arc::clone(&read);
    sim.client("app", async move {
        let rt = TurmoilRuntime::new();
        let mut server = monitor(&rt).await?;
        *shown.lock().unwrap() = Some(read_server_limits(&mut server).await?);
        Ok(())
    });

    sim.run().unwrap();

    assert_eq!(*read.lock().unwrap(), Some(limits));
}

#[test]
fn the_fake_postgres_shows_postgresql_defaults_unless_told_otherwise() {
    let read: Arc<Mutex<Option<ServerLimits>>> = Arc::new(Mutex::new(None));
    let mut sim = turmoil::Builder::new().build();
    start_db(&mut sim, FakePostgresStats::default());

    let shown = Arc::clone(&read);
    sim.client("app", async move {
        let rt = TurmoilRuntime::new();
        let mut server = monitor(&rt).await?;
        *shown.lock().unwrap() = Some(read_server_limits(&mut server).await?);
        Ok(())
    });

    sim.run().unwrap();

    assert_eq!(
        *read.lock().unwrap(),
        Some(ServerLimits {
            max_connections: 100,
            superuser_reserved_connections: 3,
            reserved_connections: 0,
        })
    );
}

#[test]
fn the_fake_postgres_refuses_to_show_a_setting_it_does_not_know() {
    let mut sim = turmoil::Builder::new().build();
    start_db(&mut sim, FakePostgresStats::default());

    sim.client("app", async move {
        let rt = TurmoilRuntime::new();
        let mut server = monitor(&rt).await?;
        let error = server
            .simple_query("SHOW no_such_setting")
            .await
            .expect_err("an unknown setting must be refused");
        assert!(
            matches!(&error, QueryError::Server { code, .. } if code == sqlstate::UNDEFINED_OBJECT),
            "expected undefined_object, got {error}"
        );
        assert!(
            server.simple_query("SELECT 1").await.is_ok(),
            "the connection must stay usable after the refusal"
        );
        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn the_fake_postgres_counts_only_the_connections_this_system_did_not_open() {
    let counts: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));
    let mut sim = turmoil::Builder::new().build();
    start_db(&mut sim, FakePostgresStats::default());

    let counted = Arc::clone(&counts);
    sim.client("app", async move {
        let rt = TurmoilRuntime::new();
        let mut server = monitor(&rt).await?;
        let _ours = monitor(&rt).await?;
        let psql = open_foreign(&rt, Some("psql")).await?;
        let _unnamed = open_foreign(&rt, None).await?;
        let before = count_foreign_connections(&mut server).await?;
        counted.lock().unwrap().push(before);

        drop(psql);
        let mut after = None;
        for _ in 0..100 {
            let count = count_foreign_connections(&mut server).await?;
            if count < 2 {
                after = Some(count);
                break;
            }
            rt.sleep(Duration::from_millis(1)).await;
        }
        counted
            .lock()
            .unwrap()
            .push(after.expect("the closed foreign connection must stop being counted"));
        Ok(())
    });

    sim.run().unwrap();

    assert_eq!(*counts.lock().unwrap(), vec![2, 1]);
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
            &ServerTls::disabled(),
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
            let mut server = connect(
                &rt,
                "db:5432",
                &credentials(),
                &application_name,
                &ServerTls::disabled(),
            )
            .await?;
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
