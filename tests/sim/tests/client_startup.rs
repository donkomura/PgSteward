use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use pgsteward_core::admission::ClientLimit;
use pgsteward_core::auth::{AuthMethod, Credentials, TrustAll};
use pgsteward_core::rt::{Net, Spawner, turmoil_rt::TurmoilRuntime};
use pgsteward_core::scram::{DEFAULT_ITERATIONS, ScramVerifier};
use pgsteward_core::session::Accepted;
use pgsteward_core::tenant::TenantId;
use pgsteward_harness::fake_postgres::{FakePostgres, FakePostgresStats};
use pgsteward_protocol::framing::{Frame, decode_frame, encode_frame};
use pgsteward_protocol::startup::{
    ProtocolVersion, StartupMessage, StartupRequest, encode_startup,
};
use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MAX_FRAME: usize = 1 << 20;
const SALT: &[u8] = b"pgsteward-salt-1";
const MANY_CLIENTS: usize = 16;
const TOO_MANY_CONNECTIONS: &str = pgsteward_protocol::backend::sqlstate::TOO_MANY_CONNECTIONS;

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

    start_proxy(&mut sim, Arc::clone(&tenants), TrustAll);

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

#[derive(Debug, Clone)]
struct OneUser {
    user: String,
    password: String,
}

impl Credentials for OneUser {
    fn method(&self, tenant: &TenantId) -> Option<AuthMethod> {
        (tenant.user() == self.user).then(|| {
            AuthMethod::ScramSha256(ScramVerifier::from_password(
                &self.password,
                SALT,
                DEFAULT_ITERATIONS,
            ))
        })
    }
}

fn start_proxy<C: Credentials + Clone + Send + 'static>(
    sim: &mut turmoil::Sim<'_>,
    tenants: Arc<Mutex<Vec<String>>>,
    credentials: C,
) {
    start_proxy_with_limit(sim, tenants, credentials, MANY_CLIENTS);
}

fn start_proxy_with_limit<C: Credentials + Clone + Send + 'static>(
    sim: &mut turmoil::Sim<'_>,
    tenants: Arc<Mutex<Vec<String>>>,
    credentials: C,
    max_client_connections: usize,
) {
    sim.host("proxy", move || {
        let tenants = Arc::clone(&tenants);
        let credentials = credentials.clone();
        async move {
            let rt = TurmoilRuntime::new();
            let limit = ClientLimit::new(max_client_connections);
            let listener = rt.bind("0.0.0.0:6432").await?;
            loop {
                let (stream, _) = listener.accept().await?;
                let tenants = Arc::clone(&tenants);
                let credentials = credentials.clone();
                let limit = limit.clone();
                rt.spawn(async move {
                    let Ok((_admitted, accepted)) = limit.accept(stream, credentials, None).await
                    else {
                        return;
                    };
                    if let Accepted::Session(session) = accepted {
                        tenants.lock().unwrap().push(session.tenant().to_string());
                    }
                    std::future::pending::<()>().await;
                });
            }
        }
    });
}

#[test]
fn proving_a_password_opens_no_server_connection() {
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

    start_proxy(
        &mut sim,
        Arc::clone(&tenants),
        OneUser {
            user: "app_web".to_owned(),
            password: "secret".to_owned(),
        },
    );

    sim.client("app", async move {
        let rt = TurmoilRuntime::new();
        let mut stream = rt.connect("proxy:6432").await?;
        let mut client = SaslClient::new(&mut stream);
        client.send_startup().await?;
        client.prove("secret").await?;
        Ok(())
    });

    sim.run().unwrap();

    assert_eq!(tenants.lock().unwrap().as_slice(), ["app_web@shop"]);
    assert_eq!(
        stats.accepted(),
        0,
        "proving a password must not open a server connection"
    );
}

struct SaslClient<'a, S> {
    stream: &'a mut S,
    buf: BytesMut,
}

impl<'a, S: AsyncReadExt + AsyncWriteExt + Unpin> SaslClient<'a, S> {
    fn new(stream: &'a mut S) -> Self {
        Self {
            stream,
            buf: BytesMut::new(),
        }
    }

    async fn send(&mut self, bytes: &[u8]) -> turmoil::Result {
        self.stream.write_all(bytes).await?;
        self.stream.flush().await?;
        Ok(())
    }

    async fn send_startup(&mut self) -> turmoil::Result {
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
        self.send(&out).await
    }

    async fn read_frame(&mut self) -> turmoil::Result<Frame> {
        loop {
            if let Some(frame) = decode_frame(&mut self.buf, MAX_FRAME).unwrap() {
                return Ok(frame);
            }
            assert!(
                self.stream.read_buf(&mut self.buf).await? > 0,
                "the proxy closed"
            );
        }
    }

    async fn read_message(&mut self) -> turmoil::Result<Message> {
        let frame = self.read_frame().await?;
        let mut bytes = BytesMut::new();
        encode_frame(frame.tag, &frame.body, &mut bytes);
        Ok(Message::parse(&mut bytes).unwrap().unwrap())
    }

    async fn prove(&mut self, password: &str) -> turmoil::Result {
        let mut scram = ScramSha256::new(password.as_bytes(), ChannelBinding::unsupported());
        assert!(matches!(
            self.read_message().await?,
            Message::AuthenticationSasl(_)
        ));
        let mut out = BytesMut::new();
        frontend::sasl_initial_response("SCRAM-SHA-256", scram.message(), &mut out).unwrap();
        self.send(&out).await?;

        let Message::AuthenticationSaslContinue(body) = self.read_message().await? else {
            panic!("expected an AuthenticationSASLContinue");
        };
        scram.update(body.data()).unwrap();
        let mut out = BytesMut::new();
        frontend::sasl_response(scram.message(), &mut out).unwrap();
        self.send(&out).await?;

        let Message::AuthenticationSaslFinal(body) = self.read_message().await? else {
            panic!("expected an AuthenticationSASLFinal");
        };
        scram.finish(body.data()).unwrap();
        assert!(matches!(
            self.read_message().await?,
            Message::AuthenticationOk
        ));
        Ok(())
    }
}

#[test]
fn a_client_over_the_node_limit_is_refused_and_opens_no_server_connection() {
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

    start_proxy_with_limit(&mut sim, Arc::clone(&tenants), TrustAll, 1);

    sim.client("app", async move {
        let rt = TurmoilRuntime::new();
        let mut held = rt.connect("proxy:6432").await?;
        let mut client = SaslClient::new(&mut held);
        client.send_startup().await?;
        assert!(matches!(
            client.read_message().await?,
            Message::AuthenticationOk
        ));

        let mut refused = rt.connect("proxy:6432").await?;
        let mut client = SaslClient::new(&mut refused);
        client.send_startup().await?;
        let refusal = client.read_frame().await?;
        assert_eq!(refusal.tag, b'E');
        assert!(
            refusal
                .body
                .windows(TOO_MANY_CONNECTIONS.len())
                .any(|field| field == TOO_MANY_CONNECTIONS.as_bytes()),
            "the refusal must carry the too_many_connections SQLSTATE"
        );
        Ok(())
    });

    sim.run().unwrap();

    assert_eq!(tenants.lock().unwrap().as_slice(), ["app_web@shop"]);
    assert_eq!(
        stats.accepted(),
        0,
        "refusing a client must not open a server connection"
    );
}
