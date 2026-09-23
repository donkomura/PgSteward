use std::net::SocketAddr;
use std::time::Duration;

use bytes::{BufMut, BytesMut};
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
use pgsteward_protocol::framing::{Frame, decode_frame, encode_frame};
use pgsteward_protocol::startup::{
    ProtocolVersion, StartupMessage, StartupRequest, encode_startup,
};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

const IDENTIFIER: &str = "copy";
const SLOTS: usize = 1;
const MAX_FRAME: usize = 1 << 20;
const WAIT_TIMEOUT: Duration = Duration::from_secs(10);
const ROWS: &str = "1\n2\n3\n";

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

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = BytesMut::new();
    encode_frame(tag, body, &mut out);
    out.to_vec()
}

fn query_frame(sql: &str) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_slice(sql.as_bytes());
    body.put_u8(0);
    frame(b'Q', &body)
}

/// A client that speaks frames itself: `tokio_postgres` drives a copy to
/// completion in one call, and this test has to stop halfway through one.
struct RawClient<S> {
    stream: S,
    buf: BytesMut,
}

impl<S: AsyncRead + AsyncWrite + Unpin> RawClient<S> {
    async fn start(stream: S) -> Self {
        let mut client = Self {
            stream,
            buf: BytesMut::new(),
        };
        let mut out = BytesMut::new();
        encode_startup(
            &StartupRequest::Startup(StartupMessage::new(
                ProtocolVersion::V3_0,
                vec![
                    ("user".to_owned(), "postgres".to_owned()),
                    ("database".to_owned(), "postgres".to_owned()),
                ],
            )),
            &mut out,
        );
        client.send(&out).await;
        client.read_until(b'Z').await;
        client
    }

    async fn send(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).await.unwrap();
        self.stream.flush().await.unwrap();
    }

    async fn read_frame(&mut self) -> Frame {
        loop {
            if let Some(frame) = decode_frame(&mut self.buf, MAX_FRAME).unwrap() {
                return frame;
            }
            let read = self.stream.read_buf(&mut self.buf).await.unwrap();
            assert!(read > 0, "the proxy closed before answering");
        }
    }

    async fn read_until(&mut self, tag: u8) -> Frame {
        loop {
            let frame = self.read_frame().await;
            assert_ne!(frame.tag, b'E', "the server answered with an error");
            if frame.tag == tag {
                return frame;
            }
        }
    }

    async fn read_copy_out(&mut self) -> String {
        let mut rows = String::new();
        loop {
            let frame = self.read_frame().await;
            match frame.tag {
                b'H' => {}
                b'd' => rows.push_str(str::from_utf8(&frame.body).unwrap()),
                b'c' => return rows,
                other => panic!("unexpected {:?} during a copy out", other as char),
            }
        }
    }
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
async fn a_copy_holds_its_slot_until_the_stream_ends_and_the_rows_reach_the_table() {
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

    let setup = connect_through(proxy).await;
    setup
        .simple_query("CREATE TABLE copied (n int)")
        .await
        .unwrap();

    let mut copier = RawClient::start(rt.connect(&proxy.to_string()).await.unwrap()).await;
    copier.send(&query_frame("COPY copied FROM STDIN")).await;
    copier.read_until(b'G').await;
    copier.send(&frame(b'd', ROWS.as_bytes())).await;

    let mut waiting = tokio::spawn(async move {
        connect_through(proxy)
            .await
            .simple_query("SELECT 1")
            .await
            .unwrap();
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(500), &mut waiting)
            .await
            .is_err(),
        "the only slot must stay with the copy while the client is still streaming"
    );

    copier.send(&frame(b'c', &[])).await;
    assert_eq!(copier.read_until(b'Z').await.body.as_ref(), b"I");

    tokio::time::timeout(WAIT_TIMEOUT, waiting)
        .await
        .expect("the slot must reach the waiting client once the copy ends")
        .unwrap();

    assert_eq!(single_value(&setup, "SELECT sum(n) FROM copied").await, "6");

    copier.send(&query_frame("COPY copied TO STDOUT")).await;
    assert_eq!(copier.read_copy_out().await, ROWS);
    assert_eq!(copier.read_until(b'Z').await.body.as_ref(), b"I");
}
