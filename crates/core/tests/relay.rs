use bytes::{BufMut, BytesMut};
use fallible_iterator::FallibleIterator;
use pgsteward_core::auth::TrustAll;
use pgsteward_core::relay::session_mode;
use pgsteward_core::server::{ApplicationName, ServerConnection, ServerCredentials};
use pgsteward_core::session::{Accepted, ClientSession, accept};
use pgsteward_protocol::framing::{Frame, decode_frame, decode_startup_frame, encode_frame};
use pgsteward_protocol::startup::{
    CancelKey, ProtocolVersion, StartupMessage, StartupRequest, decode_startup, encode_startup,
};
use postgres_protocol::message::backend::{ErrorResponseBody, Message};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, duplex};

const MAX_FRAME: usize = 1 << 20;
const DUPLEX_CAPACITY: usize = 64 * 1024;

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

fn select_one_result() -> Vec<u8> {
    let mut description = BytesMut::new();
    description.put_i16(1);
    description.put_slice(b"?column?\0");
    description.put_i32(0);
    description.put_i16(0);
    description.put_i32(23);
    description.put_i16(4);
    description.put_i32(-1);
    description.put_i16(0);
    let mut row = BytesMut::new();
    row.put_i16(1);
    row.put_i32(1);
    row.put_slice(b"1");
    [
        frame(b'T', &description),
        frame(b'D', &row),
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
