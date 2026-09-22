use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use fallible_iterator::FallibleIterator;
use pgsteward_core::auth::TrustAll;
use pgsteward_core::pool::{OpenServer, Pool, PoolLimits};
use pgsteward_core::relay::{
    Boundary, RelayError, Welcome, serve_assignment, session_mode, transaction_mode,
};
use pgsteward_core::rt::tokio_rt::TokioRuntime;
use pgsteward_core::server::{ApplicationName, ConnectError, ServerConnection, ServerCredentials};
use pgsteward_core::session::{Accepted, ClientSession, accept};
use pgsteward_protocol::framing::{Frame, decode_frame, decode_startup_frame, encode_frame};
use pgsteward_protocol::startup::{
    CancelKey, ProtocolVersion, StartupMessage, StartupRequest, decode_startup, encode_startup,
};
use postgres_protocol::message::backend::{ErrorResponseBody, Message};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, duplex};
use tokio::task::JoinHandle;

const MAX_FRAME: usize = 1 << 20;
const DUPLEX_CAPACITY: usize = 64 * 1024;
const WAIT_TIMEOUT: Duration = Duration::from_secs(5);
const POLL: Duration = Duration::from_millis(1);
const ATTEMPTS: usize = 1000;

const BACKEND_KEY: CancelKey = CancelKey {
    process_id: 4242,
    secret_key: 987_654_321,
};

struct Client {
    stream: DuplexStream,
    buf: BytesMut,
}

impl Client {
    fn new(stream: DuplexStream) -> Self {
        Self {
            stream,
            buf: BytesMut::new(),
        }
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

    async fn read_message(&mut self) -> Message {
        let frame = self.read_frame().await;
        let mut bytes = BytesMut::new();
        encode_frame(frame.tag, &frame.body, &mut bytes);
        Message::parse(&mut bytes).unwrap().unwrap()
    }

    async fn read_greeting(&mut self) -> (Vec<(String, String)>, CancelKey) {
        let mut parameters = Vec::new();
        let mut key = None;
        loop {
            match self.read_message().await {
                Message::ParameterStatus(body) => parameters.push((
                    body.name().unwrap().to_owned(),
                    body.value().unwrap().to_owned(),
                )),
                Message::BackendKeyData(body) => {
                    key = Some(CancelKey {
                        process_id: body.process_id(),
                        secret_key: body.secret_key(),
                    });
                }
                Message::ReadyForQuery(body) => {
                    assert_eq!(body.status(), b'I');
                    return (
                        parameters,
                        key.expect("a BackendKeyData before ReadyForQuery"),
                    );
                }
                _ => panic!("unexpected message before ReadyForQuery"),
            }
        }
    }

    async fn read_to_end(&mut self) -> Vec<u8> {
        let mut rest = self.buf.to_vec();
        self.stream.read_to_end(&mut rest).await.unwrap();
        rest
    }
}

struct Backend {
    stream: DuplexStream,
    buf: BytesMut,
}

impl Backend {
    fn new(stream: DuplexStream) -> Self {
        Self {
            stream,
            buf: BytesMut::new(),
        }
    }

    async fn read_startup(&mut self) {
        loop {
            if let Some(body) = decode_startup_frame(&mut self.buf, MAX_FRAME).unwrap() {
                match decode_startup(&body).unwrap() {
                    StartupRequest::Startup(_) => return,
                    other => panic!("expected a StartupMessage, got {other:?}"),
                }
            }
            let read = self.stream.read_buf(&mut self.buf).await.unwrap();
            assert!(read > 0, "the proxy closed before sending a startup packet");
        }
    }

    async fn read_frame(&mut self) -> Frame {
        loop {
            if let Some(frame) = decode_frame(&mut self.buf, MAX_FRAME).unwrap() {
                return frame;
            }
            let read = self.stream.read_buf(&mut self.buf).await.unwrap();
            assert!(read > 0, "the proxy closed before sending a frame");
        }
    }

    async fn read_to_end(&mut self) -> Vec<u8> {
        let mut rest = self.buf.to_vec();
        self.stream.read_to_end(&mut rest).await.unwrap();
        rest
    }

    async fn send(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).await.unwrap();
        self.stream.flush().await.unwrap();
    }
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

fn terminate_frame() -> Vec<u8> {
    frame(b'X', &[])
}

fn parameter_status(name: &str, value: &str) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_slice(name.as_bytes());
    body.put_u8(0);
    body.put_slice(value.as_bytes());
    body.put_u8(0);
    frame(b'S', &body)
}

fn startup_tail() -> Vec<u8> {
    let mut authentication = BytesMut::new();
    authentication.put_i32(0);
    let mut key = BytesMut::new();
    key.put_i32(BACKEND_KEY.process_id);
    key.put_i32(BACKEND_KEY.secret_key);
    [
        frame(b'R', &authentication),
        parameter_status("server_version", "16.4"),
        parameter_status("client_encoding", "UTF8"),
        frame(b'K', &key),
        frame(b'Z', b"I"),
    ]
    .concat()
}

fn row_description() -> Vec<u8> {
    let mut description = BytesMut::new();
    description.put_i16(1);
    description.put_slice(b"?column?\0");
    description.put_i32(0);
    description.put_i16(0);
    description.put_i32(23);
    description.put_i16(4);
    description.put_i32(-1);
    description.put_i16(0);
    frame(b'T', &description)
}

fn data_row() -> Vec<u8> {
    let mut row = BytesMut::new();
    row.put_i16(1);
    row.put_i32(1);
    row.put_slice(b"1");
    frame(b'D', &row)
}

fn select_one_result() -> Vec<u8> {
    [
        row_description(),
        data_row(),
        frame(b'C', b"SELECT 1\0"),
        frame(b'Z', b"I"),
    ]
    .concat()
}

fn error_response(sqlstate: &str, message: &str) -> Vec<u8> {
    let mut body = BytesMut::new();
    for (field, value) in [(b'S', "ERROR"), (b'C', sqlstate), (b'M', message)] {
        body.put_u8(field);
        body.put_slice(value.as_bytes());
        body.put_u8(0);
    }
    body.put_u8(0);
    frame(b'E', &body)
}

fn error_fields(body: &ErrorResponseBody) -> Vec<(u8, String)> {
    let mut collected = Vec::new();
    let mut fields = body.fields();
    while let Some(field) = fields.next().unwrap() {
        collected.push((
            field.type_(),
            String::from_utf8(field.value_bytes().to_vec()).unwrap(),
        ));
    }
    collected
}

async fn client_session(pipelined: &[u8]) -> (Client, ClientSession<DuplexStream>) {
    let (client_stream, session_stream) = duplex(DUPLEX_CAPACITY);
    let mut client = Client::new(client_stream);
    let accepting = tokio::spawn(accept(session_stream, TrustAll));

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
    out.put_slice(pipelined);
    client.send(&out).await;

    let Accepted::Session(session) = accepting.await.unwrap().unwrap() else {
        panic!("expected an authenticated session");
    };
    assert!(matches!(
        client.read_message().await,
        Message::AuthenticationOk
    ));
    (client, session)
}

async fn server_connection() -> (Backend, ServerConnection<DuplexStream>) {
    let (proxy_stream, backend_stream) = duplex(DUPLEX_CAPACITY);
    let mut backend = Backend::new(backend_stream);
    let connecting = tokio::spawn(async move {
        let credentials = ServerCredentials {
            user: "app_web".to_owned(),
            database: "shop".to_owned(),
            password: None,
        };
        ServerConnection::handshake(proxy_stream, &credentials, &ApplicationName::new("node-1"))
            .await
            .unwrap()
    });

    backend.read_startup().await;
    backend.send(&startup_tail()).await;
    (backend, connecting.await.unwrap())
}

#[tokio::test]
async fn the_client_is_greeted_with_the_server_parameters_the_backend_key_and_ready_for_query() {
    let (mut client, session) = client_session(&[]).await;
    let (_backend, server) = server_connection().await;
    tokio::spawn(session_mode(session, server));

    let (parameters, key) = client.read_greeting().await;
    assert_eq!(
        parameters,
        vec![
            ("client_encoding".to_owned(), "UTF8".to_owned()),
            ("server_version".to_owned(), "16.4".to_owned()),
        ]
    );
    assert_eq!(key, BACKEND_KEY);
}

#[tokio::test]
async fn a_simple_query_and_its_result_cross_unchanged() {
    let (mut client, session) = client_session(&[]).await;
    let (mut backend, server) = server_connection().await;
    tokio::spawn(session_mode(session, server));
    client.read_greeting().await;

    client.send(&query_frame("SELECT 1")).await;
    let query = backend.read_frame().await;
    assert_eq!(query.tag, b'Q');
    assert_eq!(query.body.as_ref(), b"SELECT 1\0");

    backend.send(&select_one_result()).await;
    let Message::RowDescription(_) = client.read_message().await else {
        panic!("expected a RowDescription");
    };
    let Message::DataRow(body) = client.read_message().await else {
        panic!("expected a DataRow");
    };
    let mut ranges = body.ranges();
    let range = ranges
        .next()
        .unwrap()
        .expect("one column in the row")
        .expect("a non-null value");
    assert_eq!(&body.buffer()[range], b"1");
    let Message::CommandComplete(body) = client.read_message().await else {
        panic!("expected a CommandComplete");
    };
    assert_eq!(body.tag().unwrap(), "SELECT 1");
    let Message::ReadyForQuery(body) = client.read_message().await else {
        panic!("expected a ReadyForQuery");
    };
    assert_eq!(body.status(), b'I');
}

#[tokio::test]
async fn a_query_sent_before_the_relay_started_reaches_the_server() {
    let (mut client, session) = client_session(&query_frame("SELECT 1")).await;
    let (mut backend, server) = server_connection().await;
    tokio::spawn(session_mode(session, server));
    client.read_greeting().await;

    let query = backend.read_frame().await;
    assert_eq!(query.tag, b'Q');
    assert_eq!(query.body.as_ref(), b"SELECT 1\0");
}

#[tokio::test]
async fn an_error_from_the_server_reaches_the_client_with_its_sqlstate() {
    let (mut client, session) = client_session(&[]).await;
    let (mut backend, server) = server_connection().await;
    tokio::spawn(session_mode(session, server));
    client.read_greeting().await;

    client.send(&query_frame("SELECT * FROM missing")).await;
    backend.read_frame().await;
    backend
        .send(
            &[
                error_response("42P01", "relation \"missing\" does not exist"),
                frame(b'Z', b"I"),
            ]
            .concat(),
        )
        .await;

    let Message::ErrorResponse(body) = client.read_message().await else {
        panic!("expected an ErrorResponse");
    };
    assert_eq!(
        error_fields(&body),
        vec![
            (b'S', "ERROR".to_owned()),
            (b'C', "42P01".to_owned()),
            (b'M', "relation \"missing\" does not exist".to_owned()),
        ]
    );
    let Message::ReadyForQuery(body) = client.read_message().await else {
        panic!("expected a ReadyForQuery");
    };
    assert_eq!(body.status(), b'I');
}

#[tokio::test]
async fn terminating_the_client_ends_the_session_on_the_server() {
    let (mut client, session) = client_session(&[]).await;
    let (mut backend, server) = server_connection().await;
    let relay = tokio::spawn(session_mode(session, server));
    client.read_greeting().await;

    client.send(&terminate_frame()).await;
    client.stream.shutdown().await.unwrap();
    assert_eq!(backend.read_to_end().await, terminate_frame());

    drop(backend);
    relay.await.unwrap().unwrap();
}

#[tokio::test]
async fn a_server_that_closes_closes_the_client_connection() {
    let (mut client, session) = client_session(&[]).await;
    let (backend, server) = server_connection().await;
    let relay = tokio::spawn(session_mode(session, server));
    client.read_greeting().await;

    drop(backend);
    assert!(client.read_to_end().await.is_empty());

    drop(client);
    relay.await.unwrap().unwrap();
}

type Assignment = (
    Result<Boundary, RelayError>,
    BytesMut,
    ServerConnection<DuplexStream>,
);

fn client_link() -> (Client, DuplexStream) {
    let (app, proxy) = duplex(DUPLEX_CAPACITY);
    (Client::new(app), proxy)
}

fn spawn_assignment(
    mut client: DuplexStream,
    mut pending: BytesMut,
    mut server: ServerConnection<DuplexStream>,
) -> JoinHandle<Assignment> {
    tokio::spawn(async move {
        let boundary = serve_assignment(&mut client, &mut pending, &mut server).await;
        (boundary, pending, server)
    })
}

fn command_complete(tag: &str, status: u8) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_slice(tag.as_bytes());
    body.put_u8(0);
    [frame(b'C', &body), frame(b'Z', &[status])].concat()
}

async fn expect_query(backend: &mut Backend, sql: &str) {
    let query = backend.read_frame().await;
    assert_eq!(query.tag, b'Q');
    assert_eq!(
        query.body.as_ref(),
        [sql.as_bytes(), b"\0"].concat().as_slice()
    );
}

async fn expect_complete(client: &mut Client) {
    assert!(matches!(
        client.read_message().await,
        Message::CommandComplete(_)
    ));
}

async fn expect_ready(client: &mut Client, status: u8) {
    let Message::ReadyForQuery(body) = client.read_message().await else {
        panic!("expected a ReadyForQuery");
    };
    assert_eq!(body.status(), status);
}

#[tokio::test]
async fn a_simple_query_releases_the_assignment_when_the_server_reports_idle() {
    let (mut client, proxy) = client_link();
    let (mut backend, server) = server_connection().await;
    let assignment = spawn_assignment(proxy, BytesMut::new(), server);

    client.send(&query_frame("SELECT 1")).await;
    expect_query(&mut backend, "SELECT 1").await;
    backend.send(&select_one_result()).await;

    let (boundary, pending, _server) = assignment.await.unwrap();
    assert_eq!(boundary.unwrap(), Boundary::Released);
    assert!(pending.is_empty());
    assert!(matches!(
        client.read_message().await,
        Message::RowDescription(_)
    ));
    assert!(matches!(client.read_message().await, Message::DataRow(_)));
    expect_complete(&mut client).await;
    expect_ready(&mut client, b'I').await;
}

#[tokio::test]
async fn an_open_transaction_holds_the_assignment_until_it_ends() {
    let (mut client, proxy) = client_link();
    let (mut backend, server) = server_connection().await;
    let assignment = spawn_assignment(proxy, BytesMut::new(), server);

    client.send(&query_frame("BEGIN")).await;
    expect_query(&mut backend, "BEGIN").await;
    backend.send(&command_complete("BEGIN", b'T')).await;
    expect_complete(&mut client).await;
    expect_ready(&mut client, b'T').await;
    tokio::task::yield_now().await;
    assert!(
        !assignment.is_finished(),
        "an open transaction must hold the assignment"
    );

    client.send(&query_frame("COMMIT")).await;
    expect_query(&mut backend, "COMMIT").await;
    backend.send(&command_complete("COMMIT", b'I')).await;
    expect_complete(&mut client).await;
    expect_ready(&mut client, b'I').await;

    let (boundary, _pending, _server) = assignment.await.unwrap();
    assert_eq!(boundary.unwrap(), Boundary::Released);
}

#[tokio::test]
async fn a_failed_transaction_holds_the_assignment_until_it_is_rolled_back() {
    let (mut client, proxy) = client_link();
    let (mut backend, server) = server_connection().await;
    let assignment = spawn_assignment(proxy, BytesMut::new(), server);

    client.send(&query_frame("SELECT * FROM missing")).await;
    expect_query(&mut backend, "SELECT * FROM missing").await;
    backend
        .send(
            &[
                error_response("42P01", "relation \"missing\" does not exist"),
                frame(b'Z', b"E"),
            ]
            .concat(),
        )
        .await;
    assert!(matches!(
        client.read_message().await,
        Message::ErrorResponse(_)
    ));
    expect_ready(&mut client, b'E').await;
    tokio::task::yield_now().await;
    assert!(
        !assignment.is_finished(),
        "a failed transaction must hold the assignment"
    );

    client.send(&query_frame("ROLLBACK")).await;
    expect_query(&mut backend, "ROLLBACK").await;
    backend.send(&command_complete("ROLLBACK", b'I')).await;
    expect_complete(&mut client).await;
    expect_ready(&mut client, b'I').await;

    let (boundary, _pending, _server) = assignment.await.unwrap();
    assert_eq!(boundary.unwrap(), Boundary::Released);
}

#[tokio::test]
async fn pipelined_queries_hold_the_assignment_until_the_last_one_is_answered() {
    let (mut client, proxy) = client_link();
    let (mut backend, server) = server_connection().await;
    let assignment = spawn_assignment(proxy, BytesMut::new(), server);

    client
        .send(&[query_frame("SELECT 1"), query_frame("SELECT 2")].concat())
        .await;
    expect_query(&mut backend, "SELECT 1").await;
    expect_query(&mut backend, "SELECT 2").await;
    backend.send(&command_complete("SELECT 1", b'I')).await;
    backend.send(&command_complete("SELECT 2", b'I')).await;

    let (boundary, _pending, _server) = assignment.await.unwrap();
    assert_eq!(boundary.unwrap(), Boundary::Released);
    for _ in 0..2 {
        expect_complete(&mut client).await;
        expect_ready(&mut client, b'I').await;
    }
}

#[tokio::test]
async fn a_released_assignment_leaves_the_server_connection_to_the_next_client() {
    let (mut first, proxy) = client_link();
    let (mut backend, server) = server_connection().await;
    let assignment = spawn_assignment(proxy, BytesMut::new(), server);

    first.send(&query_frame("SELECT 1")).await;
    expect_query(&mut backend, "SELECT 1").await;
    backend.send(&command_complete("SELECT 1", b'I')).await;
    let (boundary, _pending, server) = assignment.await.unwrap();
    assert_eq!(boundary.unwrap(), Boundary::Released);

    let (mut second, proxy) = client_link();
    let assignment = spawn_assignment(proxy, BytesMut::new(), server);
    second.send(&query_frame("SELECT 2")).await;
    expect_query(&mut backend, "SELECT 2").await;
    backend.send(&command_complete("SELECT 2", b'I')).await;

    let (boundary, _pending, _server) = assignment.await.unwrap();
    assert_eq!(boundary.unwrap(), Boundary::Released);
    expect_complete(&mut second).await;
    expect_ready(&mut second, b'I').await;
}

#[tokio::test]
async fn terminate_ends_the_assignment_without_reaching_the_server() {
    let (mut client, proxy) = client_link();
    let (mut backend, server) = server_connection().await;
    let assignment = spawn_assignment(proxy, BytesMut::new(), server);

    client.send(&terminate_frame()).await;

    let (boundary, _pending, server) = assignment.await.unwrap();
    assert_eq!(
        boundary.unwrap(),
        Boundary::ClientClosed { may_release: true }
    );
    drop(server);
    assert!(
        backend.read_to_end().await.is_empty(),
        "Terminate must not reach a server connection that goes back to the pool"
    );
}

#[tokio::test]
async fn a_client_that_leaves_mid_transaction_leaves_the_assignment_unreleasable() {
    let (mut client, proxy) = client_link();
    let (mut backend, server) = server_connection().await;
    let assignment = spawn_assignment(proxy, BytesMut::new(), server);

    client.send(&query_frame("BEGIN")).await;
    expect_query(&mut backend, "BEGIN").await;
    backend.send(&command_complete("BEGIN", b'T')).await;
    expect_complete(&mut client).await;
    expect_ready(&mut client, b'T').await;
    drop(client);

    let (boundary, _pending, _server) = assignment.await.unwrap();
    assert_eq!(
        boundary.unwrap(),
        Boundary::ClientClosed { may_release: false }
    );
}

#[tokio::test]
async fn a_server_that_closes_mid_request_fails_the_assignment() {
    let (mut client, proxy) = client_link();
    let (mut backend, server) = server_connection().await;
    let assignment = spawn_assignment(proxy, BytesMut::new(), server);

    client.send(&query_frame("SELECT 1")).await;
    expect_query(&mut backend, "SELECT 1").await;
    drop(backend);

    let (boundary, _pending, _server) = assignment.await.unwrap();
    assert!(matches!(boundary.unwrap_err(), RelayError::ServerClosed));
}

#[tokio::test]
async fn an_unknown_client_message_fails_the_assignment_before_it_reaches_the_server() {
    let (mut client, proxy) = client_link();
    let (mut backend, server) = server_connection().await;
    let assignment = spawn_assignment(proxy, BytesMut::new(), server);

    client.send(&frame(b'!', b"")).await;

    let (boundary, _pending, server) = assignment.await.unwrap();
    assert!(matches!(boundary.unwrap_err(), RelayError::Message(_)));
    drop(server);
    assert!(backend.read_to_end().await.is_empty());
}

fn parse_frame(statement: &str, sql: &str) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_slice(statement.as_bytes());
    body.put_u8(0);
    body.put_slice(sql.as_bytes());
    body.put_u8(0);
    body.put_i16(0);
    frame(b'P', &body)
}

fn bind_frame(portal: &str, statement: &str) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_slice(portal.as_bytes());
    body.put_u8(0);
    body.put_slice(statement.as_bytes());
    body.put_u8(0);
    body.put_i16(0);
    body.put_i16(0);
    body.put_i16(0);
    frame(b'B', &body)
}

fn execute_frame(portal: &str) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_slice(portal.as_bytes());
    body.put_u8(0);
    body.put_i32(0);
    frame(b'E', &body)
}

fn flush_frame() -> Vec<u8> {
    frame(b'H', &[])
}

fn sync_frame() -> Vec<u8> {
    frame(b'S', &[])
}

fn extended_select_one() -> Vec<u8> {
    [
        parse_frame("", "SELECT 1"),
        bind_frame("", ""),
        execute_frame(""),
    ]
    .concat()
}

fn extended_select_one_result() -> Vec<u8> {
    [
        frame(b'1', &[]),
        frame(b'2', &[]),
        row_description(),
        data_row(),
        frame(b'C', b"SELECT 1\0"),
    ]
    .concat()
}

async fn expect_backend_frames(backend: &mut Backend, tags: &[u8]) {
    for &tag in tags {
        assert_eq!(backend.read_frame().await.tag, tag);
    }
}

async fn expect_client_frames(client: &mut Client, tags: &[u8]) {
    for &tag in tags {
        assert_eq!(client.read_frame().await.tag, tag);
    }
}

#[tokio::test]
async fn an_answered_flush_holds_the_assignment_until_the_sync_is_answered() {
    let (mut client, proxy) = client_link();
    let (mut backend, server) = server_connection().await;
    let assignment = spawn_assignment(proxy, BytesMut::new(), server);

    client
        .send(&[extended_select_one(), flush_frame()].concat())
        .await;
    expect_backend_frames(&mut backend, b"PBEH").await;
    backend.send(&extended_select_one_result()).await;
    expect_client_frames(&mut client, b"12TDC").await;
    tokio::task::yield_now().await;
    assert!(
        !assignment.is_finished(),
        "an extended query window must hold the assignment until its Sync is answered"
    );

    client.send(&sync_frame()).await;
    expect_backend_frames(&mut backend, b"S").await;
    backend.send(&frame(b'Z', b"I")).await;

    let (boundary, _pending, _server) = assignment.await.unwrap();
    assert_eq!(boundary.unwrap(), Boundary::Released);
    expect_ready(&mut client, b'I').await;
}

#[tokio::test]
async fn a_client_that_leaves_before_its_sync_leaves_the_assignment_unreleasable() {
    let (mut client, proxy) = client_link();
    let (mut backend, server) = server_connection().await;
    let assignment = spawn_assignment(proxy, BytesMut::new(), server);

    client
        .send(&[extended_select_one(), flush_frame()].concat())
        .await;
    expect_backend_frames(&mut backend, b"PBEH").await;
    backend.send(&extended_select_one_result()).await;
    expect_client_frames(&mut client, b"12TDC").await;
    drop(client);

    let (boundary, _pending, _server) = assignment.await.unwrap();
    assert_eq!(
        boundary.unwrap(),
        Boundary::ClientClosed { may_release: false }
    );
}

#[tokio::test]
async fn a_parse_pipelined_after_the_sync_holds_the_assignment_past_the_ready_for_query() {
    let (mut client, proxy) = client_link();
    let (mut backend, server) = server_connection().await;
    let assignment = spawn_assignment(proxy, BytesMut::new(), server);

    client
        .send(
            &[
                extended_select_one(),
                sync_frame(),
                parse_frame("", "SELECT 2"),
            ]
            .concat(),
        )
        .await;
    expect_backend_frames(&mut backend, b"PBESP").await;
    backend
        .send(&[extended_select_one_result(), frame(b'Z', b"I")].concat())
        .await;
    expect_client_frames(&mut client, b"12TDCZ").await;
    tokio::task::yield_now().await;
    assert!(
        !assignment.is_finished(),
        "a request pipelined after the Sync must hold the assignment"
    );

    client
        .send(&[bind_frame("", ""), execute_frame(""), sync_frame()].concat())
        .await;
    expect_backend_frames(&mut backend, b"BES").await;
    backend
        .send(&[extended_select_one_result(), frame(b'Z', b"I")].concat())
        .await;

    let (boundary, _pending, _server) = assignment.await.unwrap();
    assert_eq!(boundary.unwrap(), Boundary::Released);
}

fn copy_response(tag: u8, columns: i16) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_u8(0);
    body.put_i16(columns);
    for _ in 0..columns {
        body.put_i16(0);
    }
    frame(tag, &body)
}

fn copy_data_frame(row: &str) -> Vec<u8> {
    frame(b'd', row.as_bytes())
}

fn copy_done_frame() -> Vec<u8> {
    frame(b'c', &[])
}

fn copy_fail_frame(reason: &str) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_slice(reason.as_bytes());
    body.put_u8(0);
    frame(b'f', &body)
}

#[tokio::test]
async fn a_copy_out_holds_the_assignment_until_the_stream_is_answered() {
    let (mut client, proxy) = client_link();
    let (mut backend, server) = server_connection().await;
    let assignment = spawn_assignment(proxy, BytesMut::new(), server);

    client.send(&query_frame("COPY t TO STDOUT")).await;
    expect_query(&mut backend, "COPY t TO STDOUT").await;
    backend
        .send(
            &[
                copy_response(b'H', 1),
                copy_data_frame("1\n"),
                copy_data_frame("2\n"),
            ]
            .concat(),
        )
        .await;
    expect_client_frames(&mut client, b"Hdd").await;
    tokio::task::yield_now().await;
    assert!(
        !assignment.is_finished(),
        "a copy stream must hold the assignment until its ReadyForQuery"
    );

    backend
        .send(&[copy_done_frame(), command_complete("COPY 2", b'I')].concat())
        .await;

    let (boundary, _pending, _server) = assignment.await.unwrap();
    assert_eq!(boundary.unwrap(), Boundary::Released);
    expect_client_frames(&mut client, b"c").await;
    expect_complete(&mut client).await;
    expect_ready(&mut client, b'I').await;
}

#[tokio::test]
async fn a_copy_in_holds_the_assignment_while_the_client_streams_its_rows() {
    let (mut client, proxy) = client_link();
    let (mut backend, server) = server_connection().await;
    let assignment = spawn_assignment(proxy, BytesMut::new(), server);

    client.send(&query_frame("COPY t FROM STDIN")).await;
    expect_query(&mut backend, "COPY t FROM STDIN").await;
    backend.send(&copy_response(b'G', 1)).await;
    expect_client_frames(&mut client, b"G").await;

    client
        .send(&[copy_data_frame("1\n"), copy_data_frame("2\n")].concat())
        .await;
    expect_backend_frames(&mut backend, b"dd").await;
    tokio::task::yield_now().await;
    assert!(
        !assignment.is_finished(),
        "a client still streaming rows must keep the assignment"
    );

    client.send(&copy_done_frame()).await;
    expect_backend_frames(&mut backend, b"c").await;
    backend.send(&command_complete("COPY 2", b'I')).await;

    let (boundary, _pending, _server) = assignment.await.unwrap();
    assert_eq!(boundary.unwrap(), Boundary::Released);
    expect_complete(&mut client).await;
    expect_ready(&mut client, b'I').await;
}

#[tokio::test]
async fn a_client_that_leaves_mid_copy_in_leaves_the_assignment_unreleasable() {
    let (mut client, proxy) = client_link();
    let (mut backend, server) = server_connection().await;
    let assignment = spawn_assignment(proxy, BytesMut::new(), server);

    client.send(&query_frame("COPY t FROM STDIN")).await;
    expect_query(&mut backend, "COPY t FROM STDIN").await;
    backend.send(&copy_response(b'G', 1)).await;
    expect_client_frames(&mut client, b"G").await;
    client.send(&copy_data_frame("1\n")).await;
    expect_backend_frames(&mut backend, b"d").await;
    drop(client);

    let (boundary, _pending, _server) = assignment.await.unwrap();
    assert_eq!(
        boundary.unwrap(),
        Boundary::ClientClosed { may_release: false }
    );
}

#[tokio::test]
async fn a_copy_in_that_fails_inside_a_transaction_holds_the_assignment_until_the_rollback() {
    let (mut client, proxy) = client_link();
    let (mut backend, server) = server_connection().await;
    let assignment = spawn_assignment(proxy, BytesMut::new(), server);

    client.send(&query_frame("BEGIN")).await;
    expect_query(&mut backend, "BEGIN").await;
    backend.send(&command_complete("BEGIN", b'T')).await;
    expect_complete(&mut client).await;
    expect_ready(&mut client, b'T').await;

    client.send(&query_frame("COPY t FROM STDIN")).await;
    expect_query(&mut backend, "COPY t FROM STDIN").await;
    backend.send(&copy_response(b'G', 1)).await;
    expect_client_frames(&mut client, b"G").await;
    client
        .send(&[copy_data_frame("oops\n"), copy_fail_frame("bad input")].concat())
        .await;
    expect_backend_frames(&mut backend, b"df").await;
    backend
        .send(
            &[
                error_response("22P02", "invalid input syntax for type integer"),
                frame(b'Z', b"E"),
            ]
            .concat(),
        )
        .await;
    expect_client_frames(&mut client, b"EZ").await;
    tokio::task::yield_now().await;
    assert!(
        !assignment.is_finished(),
        "a failed copy leaves the transaction open, so the assignment stays"
    );

    client.send(&query_frame("ROLLBACK")).await;
    expect_query(&mut backend, "ROLLBACK").await;
    backend.send(&command_complete("ROLLBACK", b'I')).await;

    let (boundary, _pending, _server) = assignment.await.unwrap();
    assert_eq!(boundary.unwrap(), Boundary::Released);
}

struct Backends {
    opened: Arc<Mutex<Vec<Backend>>>,
}

impl Backends {
    fn new() -> Self {
        Self {
            opened: Arc::new(Mutex::new(Vec::new())),
        }
    }

    async fn accept(&self) -> Backend {
        for _ in 0..ATTEMPTS {
            let taken = self.opened.lock().unwrap().pop();
            if let Some(backend) = taken {
                return backend;
            }
            tokio::time::sleep(POLL).await;
        }
        panic!("the pool opened no server connection");
    }
}

impl Clone for Backends {
    fn clone(&self) -> Self {
        Self {
            opened: Arc::clone(&self.opened),
        }
    }
}

impl OpenServer for Backends {
    type Connection = ServerConnection<DuplexStream>;

    async fn open(&self) -> Result<Self::Connection, ConnectError> {
        let (backend, server) = server_connection().await;
        self.opened.lock().unwrap().push(backend);
        Ok(server)
    }
}

fn pool_of(backends: Backends, slots: usize) -> Pool<Backends, TokioRuntime> {
    Pool::new(
        backends,
        TokioRuntime::new(),
        PoolLimits {
            slots,
            wait_timeout: WAIT_TIMEOUT,
        },
    )
}

fn spawn_transaction_mode(
    session: ClientSession<DuplexStream>,
    pool: Pool<Backends, TokioRuntime>,
    welcome: Welcome,
) -> JoinHandle<Result<(), RelayError>> {
    tokio::spawn(async move { transaction_mode(session, &pool, &welcome).await })
}

async fn wait_until(what: &str, mut reached: impl FnMut() -> bool) {
    for _ in 0..ATTEMPTS {
        if reached() {
            return;
        }
        tokio::time::sleep(POLL).await;
    }
    panic!("{what}");
}

#[tokio::test]
async fn a_client_that_leaves_while_it_waits_for_a_slot_gives_up_its_place() {
    let backends = Backends::new();
    let pool = pool_of(backends.clone(), 1);
    let welcome = Welcome::default();

    let (mut holder, session) = client_session(&[]).await;
    let holding = spawn_transaction_mode(session, pool.clone(), welcome.clone());
    holder.read_greeting().await;
    let mut backend = backends.accept().await;
    holder.send(&query_frame("BEGIN")).await;
    expect_query(&mut backend, "BEGIN").await;
    backend.send(&command_complete("BEGIN", b'T')).await;
    expect_complete(&mut holder).await;
    expect_ready(&mut holder, b'T').await;

    let (mut leaving, session) = client_session(&[]).await;
    let waiting = spawn_transaction_mode(session, pool.clone(), welcome.clone());
    leaving.read_greeting().await;
    leaving.send(&query_frame("SELECT 1")).await;
    wait_until("the second client never reached the queue", || {
        pool.stats().waiting == 1
    })
    .await;

    drop(leaving);

    wait_until(
        "a client that went away kept its place in the queue",
        || pool.stats().waiting == 0,
    )
    .await;
    waiting.await.unwrap().unwrap();

    holder.send(&query_frame("COMMIT")).await;
    expect_query(&mut backend, "COMMIT").await;
    backend.send(&command_complete("COMMIT", b'I')).await;
    expect_query(&mut backend, "DISCARD ALL").await;
    backend.send(&command_complete("DISCARD ALL", b'I')).await;
    wait_until("the connection never went back to the pool", || {
        pool.stats().idle == 1
    })
    .await;

    drop(holder);
    holding.await.unwrap().unwrap();
}
