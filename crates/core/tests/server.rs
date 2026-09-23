use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use bytes::{BufMut, BytesMut};
use hmac::{Hmac, KeyInit, Mac};
use pgsteward_core::rt::tokio_rt::TokioRuntime;
use pgsteward_core::server::{
    ApplicationName, ConnectError, HandshakeError, QueryError, ServerConnection, ServerCredentials,
    SimpleQuery, connect,
};
use pgsteward_core::tls::ServerTls;
use pgsteward_protocol::framing::{Frame, decode_frame, decode_startup_frame, encode_frame};
use pgsteward_protocol::startup::{
    CancelKey, ProtocolVersion, StartupMessage, StartupRequest, decode_startup,
};
use sha2::{Digest, Sha256};
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

const SCRAM_SALT: &[u8] = b"pgsteward-salt16";
const SCRAM_ITERATIONS: u32 = 4096;
const SERVER_NONCE: &str = "3rfcNHYJY1ZVvWVs7j";
const FORGED_SIGNATURE: &str = "v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

fn authentication_sasl(mechanisms: &[&str]) -> Vec<u8> {
    let mut rest = BytesMut::new();
    for mechanism in mechanisms {
        rest.put_slice(mechanism.as_bytes());
        rest.put_u8(0);
    }
    rest.put_u8(0);
    authentication(10, &rest)
}

fn authentication_sasl_continue(data: &str) -> Vec<u8> {
    authentication(11, data.as_bytes())
}

fn authentication_sasl_final(data: &str) -> Vec<u8> {
    authentication(12, data.as_bytes())
}

fn split_sasl_initial_response(body: &[u8]) -> (String, String) {
    let nul = body.iter().position(|byte| *byte == 0).unwrap();
    let mechanism = String::from_utf8(body[..nul].to_vec()).unwrap();
    let rest = &body[nul + 1..];
    let declared = i32::from_be_bytes(rest[..4].try_into().unwrap());
    let data = String::from_utf8(rest[4..].to_vec()).unwrap();
    assert_eq!(usize::try_from(declared).unwrap(), data.len());
    (mechanism, data)
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().into()
}

fn salted_password(password: &str) -> [u8; 32] {
    let mut block = Vec::from(SCRAM_SALT);
    block.extend_from_slice(&1i32.to_be_bytes());
    let mut previous = hmac_sha256(password.as_bytes(), &block);
    let mut salted = previous;
    for _ in 1..SCRAM_ITERATIONS {
        previous = hmac_sha256(password.as_bytes(), &previous);
        for (byte, next) in salted.iter_mut().zip(previous) {
            *byte ^= next;
        }
    }
    salted
}

struct ScramExchange {
    client_first_bare: String,
    server_first: String,
}

impl ScramExchange {
    fn start(client_first: &str) -> Self {
        let client_first_bare = client_first
            .strip_prefix("n,,")
            .expect("a client without channel binding announces it with the n,, header")
            .to_owned();
        let client_nonce = client_first_bare.split(",r=").nth(1).unwrap().to_owned();
        let server_first = format!(
            "r={client_nonce}{SERVER_NONCE},s={},i={SCRAM_ITERATIONS}",
            STANDARD.encode(SCRAM_SALT)
        );
        Self {
            client_first_bare,
            server_first,
        }
    }

    fn finish(&self, client_final: &str, password: &str) -> String {
        let (without_proof, proof) = client_final.rsplit_once(",p=").unwrap();
        let auth_message = format!(
            "{},{},{without_proof}",
            self.client_first_bare, self.server_first
        );
        let salted = salted_password(password);

        let client_key = hmac_sha256(&salted, b"Client Key");
        let stored_key: [u8; 32] = Sha256::digest(client_key).into();
        let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());
        let expected: Vec<u8> = client_key
            .iter()
            .zip(client_signature)
            .map(|(key, signature)| key ^ signature)
            .collect();
        assert_eq!(STANDARD.decode(proof).unwrap(), expected, "client proof");

        let server_key = hmac_sha256(&salted, b"Server Key");
        format!(
            "v={}",
            STANDARD.encode(hmac_sha256(&server_key, auth_message.as_bytes()))
        )
    }
}

struct ScramTranscript {
    mechanism: String,
    client_first: String,
    client_final: String,
}

async fn scram_up_to_the_client_proof(
    backend: &mut Backend,
    mechanisms: &[&str],
    password: &str,
) -> (ScramTranscript, String) {
    backend.send(&authentication_sasl(mechanisms)).await;

    let initial = backend.read_frame().await;
    assert_eq!(initial.tag, b'p');
    let (mechanism, client_first) = split_sasl_initial_response(initial.body.as_ref());
    let exchange = ScramExchange::start(&client_first);
    backend
        .send(&authentication_sasl_continue(&exchange.server_first))
        .await;

    let response = backend.read_frame().await;
    assert_eq!(response.tag, b'p');
    let client_final = String::from_utf8(response.body.to_vec()).unwrap();
    let server_final = exchange.finish(&client_final, password);
    (
        ScramTranscript {
            mechanism,
            client_first,
            client_final,
        },
        server_final,
    )
}

async fn run_scram(backend: &mut Backend, mechanisms: &[&str], password: &str) -> ScramTranscript {
    let (transcript, server_final) =
        scram_up_to_the_client_proof(backend, mechanisms, password).await;
    backend
        .send(&authentication_sasl_final(&server_final))
        .await;
    backend.send(&successful_startup_tail()).await;
    transcript
}

#[tokio::test]
async fn handshake_completes_the_scram_sha_256_exchange() {
    let (client, mut backend) = pair();
    let server = tokio::spawn(async move {
        backend.read_startup().await;
        run_scram(&mut backend, &["SCRAM-SHA-256"], "secret").await
    });

    let conn = ServerConnection::handshake(
        client,
        &credentials(Some("secret")),
        &ApplicationName::new("node-1"),
    )
    .await
    .unwrap();
    assert_eq!(conn.parameter("server_version"), Some("16.4"));

    let transcript = server.await.unwrap();
    assert_eq!(transcript.mechanism, "SCRAM-SHA-256");
    assert!(
        transcript.client_first.starts_with("n,,n=,r="),
        "{}",
        transcript.client_first
    );
    assert!(
        transcript.client_final.starts_with("c=biws,r="),
        "{}",
        transcript.client_final
    );
}

#[tokio::test]
async fn channel_binding_is_declined_while_the_db_side_runs_without_tls() {
    let (client, mut backend) = pair();
    let server = tokio::spawn(async move {
        backend.read_startup().await;
        run_scram(
            &mut backend,
            &["SCRAM-SHA-256", "SCRAM-SHA-256-PLUS"],
            "secret",
        )
        .await
    });

    ServerConnection::handshake(
        client,
        &credentials(Some("secret")),
        &ApplicationName::new("node-1"),
    )
    .await
    .unwrap();

    let transcript = server.await.unwrap();
    assert_eq!(transcript.mechanism, "SCRAM-SHA-256");
    assert!(
        transcript.client_first.starts_with("n,,"),
        "{}",
        transcript.client_first
    );
}

#[tokio::test]
async fn a_forged_server_signature_fails_the_handshake() {
    let (client, mut backend) = pair();
    let server = tokio::spawn(async move {
        backend.read_startup().await;
        scram_up_to_the_client_proof(&mut backend, &["SCRAM-SHA-256"], "secret").await;
        backend
            .send(&authentication_sasl_final(FORGED_SIGNATURE))
            .await;
        backend.read_to_end().await
    });

    let err = ServerConnection::handshake(
        client,
        &credentials(Some("secret")),
        &ApplicationName::new("node-1"),
    )
    .await
    .unwrap_err();

    assert!(matches!(err, HandshakeError::Scram(_)), "{err:?}");
    assert!(server.await.unwrap().is_empty());
}

#[tokio::test]
async fn authentication_ok_before_the_scram_proof_is_a_protocol_violation() {
    let (client, mut backend) = pair();
    tokio::spawn(async move {
        backend.read_startup().await;
        backend.send(&authentication_sasl(&["SCRAM-SHA-256"])).await;
        backend.read_frame().await;
        backend.send(&successful_startup_tail()).await;
    });

    let err = ServerConnection::handshake(
        client,
        &credentials(Some("secret")),
        &ApplicationName::new("node-1"),
    )
    .await
    .unwrap_err();

    assert!(
        matches!(err, HandshakeError::UnexpectedMessage(_)),
        "{err:?}"
    );
}

#[tokio::test]
async fn scram_without_a_configured_password_is_refused_and_nothing_more_is_sent() {
    let (client, mut backend) = pair();
    let server = tokio::spawn(async move {
        backend.read_startup().await;
        backend.send(&authentication_sasl(&["SCRAM-SHA-256"])).await;
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
    assert!(err.to_string().contains("SCRAM-SHA-256"), "{err}");
    assert!(server.await.unwrap().is_empty());
}

#[tokio::test]
async fn sasl_without_scram_sha_256_is_not_supported() {
    let (client, mut backend) = pair();
    let server = tokio::spawn(async move {
        backend.read_startup().await;
        backend
            .send(&authentication_sasl(&["SCRAM-SHA-256-PLUS"]))
            .await;
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
    assert!(err.to_string().contains("SCRAM-SHA-256"), "{err}");
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
        &ServerTls::disabled(),
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
        &ServerTls::disabled(),
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

fn row_description(names: &[&str]) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i16(i16::try_from(names.len()).unwrap());
    for name in names {
        body.put_slice(name.as_bytes());
        body.put_u8(0);
        body.put_i32(0);
        body.put_i16(0);
        body.put_i32(25);
        body.put_i16(-1);
        body.put_i32(-1);
        body.put_i16(0);
    }
    frame(b'T', &body)
}

fn data_row(values: &[Option<&str>]) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i16(i16::try_from(values.len()).unwrap());
    for value in values {
        match value {
            Some(text) => {
                body.put_i32(i32::try_from(text.len()).unwrap());
                body.put_slice(text.as_bytes());
            }
            None => body.put_i32(-1),
        }
    }
    frame(b'D', &body)
}

fn command_complete(tag: &str) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_slice(tag.as_bytes());
    body.put_u8(0);
    frame(b'C', &body)
}

#[tokio::test]
async fn simple_query_returns_every_row_of_the_result() {
    let (client, mut backend) = pair();
    let server = tokio::spawn(async move {
        backend.read_startup().await;
        backend.send(&successful_startup_tail()).await;
        let query = backend.read_frame().await;
        backend
            .send(
                &[
                    row_description(&["name", "setting"]),
                    data_row(&[Some("max_connections"), Some("200")]),
                    data_row(&[Some("reserved_connections"), None]),
                    command_complete("SELECT 2"),
                    ready_for_query(b'I'),
                ]
                .concat(),
            )
            .await;
        query
    });

    let mut conn =
        ServerConnection::handshake(client, &credentials(None), &ApplicationName::new("node-1"))
            .await
            .unwrap();
    let rows = conn
        .simple_query("SELECT name, setting FROM pg_settings")
        .await
        .unwrap();

    let query = server.await.unwrap();
    assert_eq!(query.tag, b'Q');
    assert_eq!(
        query.body.as_ref(),
        b"SELECT name, setting FROM pg_settings\0"
    );
    assert_eq!(
        rows,
        vec![
            vec![Some("max_connections".to_owned()), Some("200".to_owned())],
            vec![Some("reserved_connections".to_owned()), None],
        ]
    );
}

#[tokio::test]
async fn simple_query_reports_the_server_error_and_leaves_the_connection_usable() {
    let (client, mut backend) = pair();
    tokio::spawn(async move {
        backend.read_startup().await;
        backend.send(&successful_startup_tail()).await;
        backend.read_frame().await;
        backend
            .send(
                &[
                    error_response("42704", "unrecognized configuration parameter \"nope\""),
                    ready_for_query(b'I'),
                ]
                .concat(),
            )
            .await;
        backend.read_frame().await;
        backend
            .send(
                &[
                    row_description(&["setting"]),
                    data_row(&[Some("200")]),
                    command_complete("SHOW"),
                    ready_for_query(b'I'),
                ]
                .concat(),
            )
            .await;
    });

    let mut conn =
        ServerConnection::handshake(client, &credentials(None), &ApplicationName::new("node-1"))
            .await
            .unwrap();

    let err = conn.simple_query("SHOW nope").await.unwrap_err();
    match err {
        QueryError::Server { code, message } => {
            assert_eq!(code, "42704");
            assert!(message.contains("nope"), "{message}");
        }
        other => panic!("expected a server error, got {other:?}"),
    }

    let rows = conn.simple_query("SHOW max_connections").await.unwrap();
    assert_eq!(rows, vec![vec![Some("200".to_owned())]]);
}

#[tokio::test]
async fn simple_query_keeps_a_parameter_status_sent_while_it_runs() {
    let (client, mut backend) = pair();
    tokio::spawn(async move {
        backend.read_startup().await;
        backend.send(&successful_startup_tail()).await;
        backend.read_frame().await;
        backend
            .send(
                &[
                    parameter_status("TimeZone", "UTC"),
                    notice_response("a notice in the middle of a result"),
                    row_description(&["set_config"]),
                    data_row(&[Some("UTC")]),
                    command_complete("SELECT 1"),
                    ready_for_query(b'I'),
                ]
                .concat(),
            )
            .await;
    });

    let mut conn =
        ServerConnection::handshake(client, &credentials(None), &ApplicationName::new("node-1"))
            .await
            .unwrap();
    conn.simple_query("SELECT set_config('TimeZone', 'UTC', false)")
        .await
        .unwrap();

    assert_eq!(conn.parameter("TimeZone"), Some("UTC"));
}
