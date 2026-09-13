use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use pgsteward_core::rt::{Net, Spawner, turmoil_rt::TurmoilRuntime};
use pgsteward_core::session::{Accepted, accept};
use pgsteward_harness::fake_postgres::{FakePostgres, FakePostgresStats};
use pgsteward_protocol::framing::decode_frame;
use pgsteward_protocol::startup::{
    ProtocolVersion, StartupMessage, StartupRequest, encode_startup,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MAX_FRAME: usize = 1 << 20;

#[test]
fn authenticating_a_client_opens_no_server_connection() {
    let stats = FakePostgresStats::default();
    let tenants: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let mut sim = turmoil::Builder::new().build();

    let db_stats = stats.clone();
    sim.host("db", move || {
        let stats = db_stats.clone();
        async move {
            let rt = TurmoilRuntime::new();
            FakePostgres::start(&rt, "0.0.0.0:5432", stats).await?;
            std::future::pending::<()>().await;
            Ok(())
        }
    });

    let proxy_tenants = Arc::clone(&tenants);
    sim.host("proxy", move || {
        let tenants = Arc::clone(&proxy_tenants);
        async move {
            let rt = TurmoilRuntime::new();
            let listener = rt.bind("0.0.0.0:6432").await?;
            loop {
                let (stream, _) = listener.accept().await?;
                let tenants = Arc::clone(&tenants);
                rt.spawn(async move {
                    if let Ok(Accepted::Session(session)) = accept(stream).await {
                        tenants.lock().unwrap().push(session.tenant().to_string());
                    }
                });
            }
        }
    });

    sim.client("app", async move {
        let rt = TurmoilRuntime::new();
        let mut stream = rt.connect("proxy:6432").await?;
        let mut out = BytesMut::new();
        encode_startup(
            &StartupRequest::Startup(StartupMessage::new(
                ProtocolVersion::V3_0,
                vec![
                    ("user".to_owned(), "app_web".to_owned()),
                    ("database".to_owned(), "shop".to_owned()),
                ],
            )),
            &mut out,
        );
        stream.write_all(&out).await?;
        stream.flush().await?;

        let mut buf = BytesMut::new();
        let frame = loop {
            if let Some(frame) = decode_frame(&mut buf, MAX_FRAME).unwrap() {
                break frame;
            }
            assert!(stream.read_buf(&mut buf).await? > 0, "the proxy closed");
        };
        assert_eq!(frame.tag, b'R');
        Ok(())
    });

    sim.run().unwrap();

    assert_eq!(tenants.lock().unwrap().as_slice(), ["app_web@shop"]);
    assert_eq!(
        stats.accepted(),
        0,
        "accepting and authenticating a client must not open a server connection"
    );
}
