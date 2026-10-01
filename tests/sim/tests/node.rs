use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use pgsteward_core::allocation::InstanceId;
use pgsteward_core::budget::ServerLimits;
use pgsteward_core::rt::{Clock, Net, Spawner, turmoil_rt::TurmoilRuntime};
use pgsteward_harness::cap::{CapMonitor, CapReport};
use pgsteward_harness::fake_postgres::{FakePostgres, FakePostgresStats};
use pgsteward_node::config::{ClusterConfig, NodeConfig};
use pgsteward_node::serve::{ServeOptions, serve};
use pgsteward_protocol::framing::{Frame, decode_frame, encode_frame};
use pgsteward_protocol::startup::{
    ProtocolVersion, StartupMessage, StartupRequest, encode_startup,
};
use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const PENCIL_VERIFIER: &str = "SCRAM-SHA-256$4096:W22ZaJ0SNY7soEsUEjb6gQ==$\
WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=:wfPLwcE6nTWhTAmQ7tl2KeoiWGPlZqQxSrmfPwDl2dU=";
const MAX_FRAME: usize = 1 << 20;
const POLL: Duration = Duration::from_millis(1);
const SEEDS: u64 = 10;
const CLIENTS: usize = 20;
const TRANSACTIONS: usize = 5;
const LIMITS: ServerLimits = ServerLimits {
    max_connections: 12,
    superuser_reserved_connections: 3,
    reserved_connections: 0,
};
const OBSERVER: usize = 1;
const BUDGET: u32 = 8;

fn node_config() -> NodeConfig {
    NodeConfig::parse(&format!(
        r#"
[node]
role = "proxy"
listen = "0.0.0.0:6432"
coordinator = "coordinator:7432"
max_client_connections = 100

[monitor]
user = "postgres"

[client."alice"]
verifier = "{PENCIL_VERIFIER}"
"#
    ))
    .unwrap()
}

fn cluster_config() -> ClusterConfig {
    ClusterConfig::parse(
        r#"
[cluster]
pool_mode = "transaction"
grant_ttl = "10s"
arbitration_interval = "10ms"

[[instance]]
name = "db"

[tenant."*"]
instances = ["db"]
"#,
    )
    .unwrap()
}

fn options() -> ServeOptions {
    ServeOptions {
        margin: 0,
        observe_interval: Duration::from_millis(100),
        ..ServeOptions::default()
    }
}

fn start_db(sim: &mut turmoil::Sim<'_>, stats: FakePostgresStats) {
    sim.host("db", move || {
        let stats = stats.clone();
        async move {
            let rt = TurmoilRuntime::new();
            FakePostgres::start_with_limits(&rt, "0.0.0.0:5432", stats, LIMITS).await?;
            std::future::pending::<()>().await;
            Ok(())
        }
    });
}

fn start_node(sim: &mut turmoil::Sim<'_>, budget: Arc<Mutex<Option<u32>>>) {
    sim.host("node", move || {
        let budget = Arc::clone(&budget);
        async move {
            let rt = TurmoilRuntime::new();
            let serving = serve(rt, node_config(), cluster_config(), options()).await?;
            *budget.lock().unwrap() = Some(serving.budget(&InstanceId::new("db")));
            std::future::pending::<()>().await;
            drop(serving);
            Ok(())
        }
    });
}

struct WireClient<S> {
    stream: S,
    buf: BytesMut,
}

impl<S: AsyncRead + AsyncWrite + Unpin> WireClient<S> {
    async fn login(stream: S, user: &str, password: &str) -> turmoil::Result<Self> {
        let mut client = Self {
            stream,
            buf: BytesMut::new(),
        };
        let mut out = BytesMut::new();
        encode_startup(
            &StartupRequest::Startup(StartupMessage::new(
                ProtocolVersion::V3_0,
                vec![
                    ("user".to_owned(), user.to_owned()),
                    ("database".to_owned(), "postgres".to_owned()),
                ],
            )),
            &mut out,
        );
        client.write(&out).await?;
        client.prove(password).await?;
        loop {
            if let Message::ReadyForQuery(body) = client.read_message().await? {
                assert_eq!(body.status(), b'I');
                return Ok(client);
            }
        }
    }

    async fn prove(&mut self, password: &str) -> turmoil::Result {
        let mut scram = ScramSha256::new(password.as_bytes(), ChannelBinding::unsupported());
        assert!(matches!(
            self.read_message().await?,
            Message::AuthenticationSasl(_)
        ));
        let mut out = BytesMut::new();
        frontend::sasl_initial_response("SCRAM-SHA-256", scram.message(), &mut out)?;
        self.write(&out).await?;

        let Message::AuthenticationSaslContinue(body) = self.read_message().await? else {
            panic!("expected an AuthenticationSASLContinue");
        };
        scram.update(body.data())?;
        let mut out = BytesMut::new();
        frontend::sasl_response(scram.message(), &mut out)?;
        self.write(&out).await?;

        let Message::AuthenticationSaslFinal(body) = self.read_message().await? else {
            panic!("expected an AuthenticationSASLFinal");
        };
        scram.finish(body.data())?;
        assert!(matches!(
            self.read_message().await?,
            Message::AuthenticationOk
        ));
        Ok(())
    }

    async fn write(&mut self, bytes: &[u8]) -> turmoil::Result {
        self.stream.write_all(bytes).await?;
        self.stream.flush().await?;
        Ok(())
    }

    async fn read_frame(&mut self) -> turmoil::Result<Frame> {
        loop {
            if let Some(frame) = decode_frame(&mut self.buf, MAX_FRAME)? {
                return Ok(frame);
            }
            assert!(
                self.stream.read_buf(&mut self.buf).await? > 0,
                "the node closed"
            );
        }
    }

    async fn read_message(&mut self) -> turmoil::Result<Message> {
        let frame = self.read_frame().await?;
        let mut bytes = BytesMut::new();
        encode_frame(frame.tag, &frame.body, &mut bytes);
        Ok(Message::parse(&mut bytes)?.expect("a whole message"))
    }

    async fn query(&mut self, sql: &str) -> turmoil::Result<u8> {
        let mut body = BytesMut::new();
        body.put_slice(sql.as_bytes());
        body.put_u8(0);
        let mut out = BytesMut::new();
        encode_frame(b'Q', &body, &mut out);
        self.write(&out).await?;
        loop {
            match self.read_message().await? {
                Message::ReadyForQuery(body) => return Ok(body.status()),
                Message::ErrorResponse(_) => panic!("the node refused {sql:?}"),
                _ => {}
            }
        }
    }
}

/// The node listens only once it has derived the total budget, so a client
/// that starts with it is refused until then.
async fn connect_when_listening(
    rt: TurmoilRuntime,
) -> turmoil::Result<<TurmoilRuntime as Net>::Stream> {
    loop {
        match rt.connect("node:6432").await {
            Ok(stream) => return Ok(stream),
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
                rt.sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

async fn run_transactions(rt: TurmoilRuntime) -> turmoil::Result {
    let stream = connect_when_listening(rt).await?;
    let mut client = WireClient::login(stream, "alice", "pencil").await?;
    for _ in 0..TRANSACTIONS {
        assert_eq!(client.query("BEGIN").await?, b'T');
        assert_eq!(client.query("SELECT 1").await?, b'T');
        assert_eq!(client.query("COMMIT").await?, b'I');
    }
    Ok(())
}

#[test]
fn a_node_on_turmoil_serves_its_clients_within_the_budget_it_derives() {
    for seed in 0..SEEDS {
        serves_within_the_budget(seed);
    }
}

fn serves_within_the_budget(seed: u64) {
    let stats = FakePostgresStats::default();
    let budget: Arc<Mutex<Option<u32>>> = Arc::new(Mutex::new(None));
    let report: Arc<Mutex<Option<CapReport>>> = Arc::new(Mutex::new(None));
    let mut sim = turmoil::Builder::new()
        .rng_seed(seed)
        .simulation_duration(Duration::from_secs(60))
        .build();
    start_db(&mut sim, stats.clone());
    start_node(&mut sim, Arc::clone(&budget));

    let observed = stats.clone();
    let capped = Arc::clone(&report);
    sim.client("app", async move {
        let rt = TurmoilRuntime::new();
        let monitor = CapMonitor::start(&rt, observed, BUDGET as usize + OBSERVER, POLL);
        let clients: Vec<_> = (0..CLIENTS)
            .map(|_| {
                rt.spawn(async move {
                    run_transactions(rt)
                        .await
                        .map_err(|error| error.to_string())
                })
            })
            .collect();
        for client in clients {
            client.await??;
        }
        *capped.lock().unwrap() = Some(monitor.stop().await);
        Ok(())
    });

    sim.run().unwrap();

    assert_eq!(
        *budget.lock().unwrap(),
        Some(BUDGET),
        "seed {seed}: the node must derive the total budget from what the fake PostgreSQL shows"
    );
    let report = report.lock().unwrap().take().expect("a cap report");
    report.assert_never_exceeded();
    assert!(
        report.peak() > OBSERVER,
        "seed {seed}: the clients must have been served over server connections"
    );
}
