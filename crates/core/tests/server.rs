use bytes::{BufMut, BytesMut};
use pgsteward_core::rt::tokio_rt::TokioRuntime;
use pgsteward_core::server::{
    ApplicationName, ConnectError, HandshakeError, ServerConnection, ServerCredentials, connect,
};
use pgsteward_protocol::framing::{Frame, decode_frame, decode_startup_frame, encode_frame};
use pgsteward_protocol::startup::{
    CancelKey, ProtocolVersion, StartupMessage, StartupRequest, decode_startup,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

const MAX_FRAME: usize = 1 << 20;

fn credentials(password: Option<&str>) -> ServerCredentials {
    ServerCredentials {
        user: "app_web".to_owned(),
        database: "shop".to_owned(),
        password: password.map(str::to_owned),
    }
}

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = BytesMut::new();
    encode_frame(tag, body, &mut out);
    out.to_vec()
}

fn authentication(code: i32, rest: &[u8]) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i32(code);
    body.put_slice(rest);
    frame(b'R', &body)
}

fn authentication_ok() -> Vec<u8> {
    authentication(0, &[])
}

fn parameter_status(name: &str, value: &str) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_slice(name.as_bytes());
    body.put_u8(0);
    body.put_slice(value.as_bytes());
    body.put_u8(0);
    frame(b'S', &body)
}

fn backend_key_data(process_id: i32, secret_key: i32) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i32(process_id);
    body.put_i32(secret_key);
    frame(b'K', &body)
}

fn ready_for_query(status: u8) -> Vec<u8> {
    frame(b'Z', &[status])
}

fn error_response(sqlstate: &str, message: &str) -> Vec<u8> {
    let mut body = BytesMut::new();
    for (field, value) in [(b'S', "FATAL"), (b'C', sqlstate), (b'M', message)] {
        body.put_u8(field);
        body.put_slice(value.as_bytes());
        body.put_u8(0);
    }
    body.put_u8(0);
    frame(b'E', &body)
}

fn notice_response(message: &str) -> Vec<u8> {
    let mut body = BytesMut::new();
    for (field, value) in [(b'S', "NOTICE"), (b'C', "00000"), (b'M', message)] {
        body.put_u8(field);
        body.put_slice(value.as_bytes());
        body.put_u8(0);
    }
    body.put_u8(0);
    frame(b'N', &body)
}

fn successful_startup_tail() -> Vec<u8> {
    [
        authentication_ok(),
        parameter_status("server_version", "16.4"),
        parameter_status("client_encoding", "UTF8"),
        backend_key_data(4242, 987_654_321),
        ready_for_query(b'I'),
    ]
    .concat()
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

    async fn read_startup(&mut self) -> StartupMessage {
        loop {
            if let Some(body) = decode_startup_frame(&mut self.buf, MAX_FRAME).unwrap() {
                match decode_startup(&body).unwrap() {
                    StartupRequest::Startup(message) => return message,
                    other => panic!("expected a StartupMessage, got {other:?}"),
                }
            }
            let n = self.stream.read_buf(&mut self.buf).await.unwrap();
            assert!(n > 0, "client closed before sending a startup packet");
        }
    }

    async fn read_frame(&mut self) -> Frame {
        loop {
            if let Some(frame) = decode_frame(&mut self.buf, MAX_FRAME).unwrap() {
                return frame;
            }
            let n = self.stream.read_buf(&mut self.buf).await.unwrap();
            assert!(n > 0, "client closed before sending a frame");
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

fn pair() -> (DuplexStream, Backend) {
    let (client, server) = tokio::io::duplex(1 << 16);
    (client, Backend::new(server))
}

#[tokio::test]
async fn handshake_sends_user_database_and_a_tagged_application_name() {
    let (client, mut backend) = pair();
    let server = tokio::spawn(async move {
        let startup = backend.read_startup().await;
        backend.send(&successful_startup_tail()).await;
        startup
    });

    let conn =
        ServerConnection::handshake(client, &credentials(None), &ApplicationName::new("node-1"))
            .await
            .unwrap();

    let startup = server.await.unwrap();
    assert_eq!(startup.version(), ProtocolVersion::V3_0);
    assert_eq!(startup.user(), Some("app_web"));
    assert_eq!(startup.database(), Some("shop"));
    assert_eq!(
        startup.parameter("application_name"),
        Some("pgsteward-node-1")
    );
    assert_eq!(conn.parameter("server_version"), Some("16.4"));
    assert_eq!(conn.parameter("client_encoding"), Some("UTF8"));
    assert_eq!(conn.parameters().len(), 2);
    assert_eq!(
        conn.backend_key(),
        CancelKey {
            process_id: 4242,
            secret_key: 987_654_321
        }
    );
}

#[tokio::test]
async fn handshake_answers_md5_with_the_salted_hash() {
    let (client, mut backend) = pair();
    let server = tokio::spawn(async move {
        backend.read_startup().await;
        backend.send(&authentication(5, &[1, 2, 3, 4])).await;
        let password = backend.read_frame().await;
        backend.send(&successful_startup_tail()).await;
        password
    });

    ServerConnection::handshake(
        client,
        &credentials(Some("secret")),
        &ApplicationName::new("node-1"),
    )
    .await
    .unwrap();

    let password = server.await.unwrap();
    assert_eq!(password.tag, b'p');
    assert_eq!(
        password.body.as_ref(),
        b"md53e2532b4448fb5de380fc851a83eeae2\0"
    );
}

#[tokio::test]
async fn md5_without_a_configured_password_is_refused_and_nothing_more_is_sent() {
    let (client, mut backend) = pair();
    let server = tokio::spawn(async move {
        backend.read_startup().await;
        backend.send(&authentication(5, &[1, 2, 3, 4])).await;
        backend.read_to_end().await
    });

    let err =
        ServerConnection::handshake(client, &credentials(None), &ApplicationName::new("node-1"))
            .await
            .unwrap_err();

    assert!(
        matches!(err, HandshakeError::PasswordRequired(_)),
        "{err:?}"
    );
    assert!(err.to_string().contains("password"), "{err}");
    assert!(server.await.unwrap().is_empty());
}

#[tokio::test]
async fn cleartext_password_authentication_is_not_supported() {
    let (client, mut backend) = pair();
    let server = tokio::spawn(async move {
        backend.read_startup().await;
        backend.send(&authentication(3, &[])).await;
        backend.read_to_end().await
    });

    let err = ServerConnection::handshake(
        client,
        &credentials(Some("secret")),
        &ApplicationName::new("node-1"),
    )
    .await
    .unwrap_err();

    assert!(
        matches!(err, HandshakeError::UnsupportedAuthentication(_)),
        "{err:?}"
    );
    assert!(err.to_string().contains("cleartext"), "{err}");
    assert!(server.await.unwrap().is_empty());
}

#[tokio::test]
async fn server_error_during_startup_is_reported_with_its_sqlstate() {
    let (client, mut backend) = pair();
    tokio::spawn(async move {
        backend.read_startup().await;
        backend
            .send(&error_response("3D000", "database \"shop\" does not exist"))
            .await;
    });

    let err =
        ServerConnection::handshake(client, &credentials(None), &ApplicationName::new("node-1"))
            .await
            .unwrap_err();

    match err {
        HandshakeError::Server { code, message } => {
            assert_eq!(code, "3D000");
            assert_eq!(message, "database \"shop\" does not exist");
        }
        other => panic!("expected a server error, got {other:?}"),
    }
}

#[tokio::test]
async fn notices_during_startup_are_ignored() {
    let (client, mut backend) = pair();
    tokio::spawn(async move {
        backend.read_startup().await;
        backend.send(&notice_response("welcome")).await;
        backend.send(&successful_startup_tail()).await;
    });

    let conn =
        ServerConnection::handshake(client, &credentials(None), &ApplicationName::new("node-1"))
            .await
            .unwrap();
    assert_eq!(conn.parameters().len(), 2);
}

#[tokio::test]
async fn connection_closed_before_ready_for_query_is_an_error() {
    let (client, mut backend) = pair();
    tokio::spawn(async move {
        backend.read_startup().await;
        backend.send(&authentication_ok()).await;
    });

    let err =
        ServerConnection::handshake(client, &credentials(None), &ApplicationName::new("node-1"))
            .await
            .unwrap_err();
    assert!(matches!(err, HandshakeError::ConnectionClosed), "{err:?}");
}

#[tokio::test]
async fn ready_for_query_before_authentication_ok_is_a_protocol_violation() {
    let (client, mut backend) = pair();
    tokio::spawn(async move {
        backend.read_startup().await;
        backend.send(&backend_key_data(1, 2)).await;
        backend.send(&ready_for_query(b'I')).await;
    });

    let err =
        ServerConnection::handshake(client, &credentials(None), &ApplicationName::new("node-1"))
            .await
            .unwrap_err();
    assert!(
        matches!(err, HandshakeError::UnexpectedMessage(_)),
        "{err:?}"
    );
}

#[tokio::test]
async fn ready_for_query_without_backend_key_data_is_a_protocol_violation() {
    let (client, mut backend) = pair();
    tokio::spawn(async move {
        backend.read_startup().await;
        backend.send(&authentication_ok()).await;
        backend.send(&ready_for_query(b'I')).await;
    });

    let err =
        ServerConnection::handshake(client, &credentials(None), &ApplicationName::new("node-1"))
            .await
            .unwrap_err();
    assert!(
        matches!(err, HandshakeError::UnexpectedMessage(_)),
        "{err:?}"
    );
}

#[tokio::test]
async fn ready_for_query_outside_idle_is_a_protocol_violation() {
    let (client, mut backend) = pair();
    tokio::spawn(async move {
        backend.read_startup().await;
        backend.send(&authentication_ok()).await;
        backend.send(&backend_key_data(1, 2)).await;
        backend.send(&ready_for_query(b'T')).await;
    });

    let err =
        ServerConnection::handshake(client, &credentials(None), &ApplicationName::new("node-1"))
            .await
            .unwrap_err();
    assert!(
        matches!(err, HandshakeError::UnexpectedMessage(_)),
        "{err:?}"
    );
}

#[tokio::test]
async fn data_before_ready_for_query_is_a_protocol_violation() {
    let (client, mut backend) = pair();
    tokio::spawn(async move {
        backend.read_startup().await;
        backend.send(&authentication_ok()).await;
        backend.send(&frame(b'C', b"SELECT 1\0")).await;
    });

    let err =
        ServerConnection::handshake(client, &credentials(None), &ApplicationName::new("node-1"))
            .await
            .unwrap_err();
    assert!(
        matches!(err, HandshakeError::UnexpectedMessage(_)),
        "{err:?}"
    );
}

#[tokio::test]
async fn terminate_sends_the_terminate_message_and_closes() {
    let (client, mut backend) = pair();
    let server = tokio::spawn(async move {
        backend.read_startup().await;
        backend.send(&successful_startup_tail()).await;
        let frame = backend.read_frame().await;
        let rest = backend.read_to_end().await;
        (frame, rest)
    });

    let conn =
        ServerConnection::handshake(client, &credentials(None), &ApplicationName::new("node-1"))
            .await
            .unwrap();
    conn.terminate().await.unwrap();

    let (frame, rest) = server.await.unwrap();
    assert_eq!(frame.tag, b'X');
    assert!(frame.body.is_empty());
    assert!(rest.is_empty());
}

#[tokio::test]
async fn connect_reports_an_unreachable_server() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let err = connect(
        &TokioRuntime::new(),
        &addr.to_string(),
        &credentials(None),
        &ApplicationName::new("node-1"),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, ConnectError::Unreachable(_)), "{err:?}");
    assert!(err.to_string().contains(&addr.to_string()), "{err}");
}

#[tokio::test]
async fn connect_runs_the_handshake_over_the_runtime_network() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = BytesMut::new();
        loop {
            if decode_startup_frame(&mut buf, MAX_FRAME).unwrap().is_some() {
                break;
            }
            stream.read_buf(&mut buf).await.unwrap();
        }
        stream.write_all(&successful_startup_tail()).await.unwrap();
        stream.flush().await.unwrap();
        let mut sink = Vec::new();
        let _ = stream.read_to_end(&mut sink).await;
    });

    let conn = connect(
        &TokioRuntime::new(),
        &addr.to_string(),
        &credentials(None),
        &ApplicationName::new("node-1"),
    )
    .await
    .unwrap();
    assert_eq!(conn.parameter("server_version"), Some("16.4"));
}

#[test]
fn application_name_carries_the_prefix_the_observer_filters_on() {
    let name = ApplicationName::new("node-1");
    assert_eq!(name.as_str(), "pgsteward-node-1");
    assert_eq!(name.to_string(), "pgsteward-node-1");
    assert!(name.as_str().starts_with(ApplicationName::PREFIX));
    assert_eq!(ApplicationName::PREFIX, "pgsteward-");
}

#[test]
fn credentials_debug_output_redacts_the_password() {
    let text = format!("{:?}", credentials(Some("hunter2")));
    assert!(text.contains("app_web"), "{text}");
    assert!(!text.contains("hunter2"), "{text}");
}
