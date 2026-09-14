use bytes::{BufMut, BytesMut};
use fallible_iterator::FallibleIterator;
use pgsteward_core::auth::{AuthMethod, Credentials, TrustAll};
use pgsteward_core::scram::{DEFAULT_ITERATIONS, ScramVerifier};
use pgsteward_core::session::{AcceptError, Accepted, ClientSession, accept};
use pgsteward_core::tenant::TenantId;
use pgsteward_protocol::backend::sqlstate;
use pgsteward_protocol::framing::{Frame, decode_frame, encode_frame};
use pgsteward_protocol::startup::{
    CancelKey, ProtocolVersion, StartupMessage, StartupRequest, encode_startup,
};
use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256};
use postgres_protocol::message::backend::{ErrorResponseBody, Message};
use postgres_protocol::message::frontend;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, duplex};

const SALT: &[u8] = b"pgsteward-salt-1";

const MAX_FRAME: usize = 1 << 20;
const DUPLEX_CAPACITY: usize = 64 * 1024;

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

    async fn send_startup(&mut self, request: &StartupRequest) {
        let mut out = BytesMut::new();
        encode_startup(request, &mut out);
        self.send(&out).await;
    }

    async fn fill(&mut self) {
        let read = self.stream.read_buf(&mut self.buf).await.unwrap();
        assert!(read > 0, "the session closed before answering");
    }

    async fn read_byte(&mut self) -> u8 {
        while self.buf.is_empty() {
            self.fill().await;
        }
        self.buf.split_to(1)[0]
    }

    async fn read_frame(&mut self) -> Frame {
        loop {
            if let Some(frame) = decode_frame(&mut self.buf, MAX_FRAME).unwrap() {
                return frame;
            }
            self.fill().await;
        }
    }

    async fn read_error(&mut self) -> Vec<(u8, String)> {
        let Message::ErrorResponse(body) = self.read_message().await else {
            panic!("expected an ErrorResponse");
        };
        error_fields(&body)
    }

    async fn read_message(&mut self) -> Message {
        let frame = self.read_frame().await;
        let mut bytes = BytesMut::new();
        encode_frame(frame.tag, &frame.body, &mut bytes);
        Message::parse(&mut bytes).unwrap().unwrap()
    }

    async fn read_offered_mechanisms(&mut self) -> Vec<String> {
        let Message::AuthenticationSasl(body) = self.read_message().await else {
            panic!("expected an AuthenticationSASL");
        };
        body.mechanisms()
            .map(|mechanism| Ok(mechanism.to_owned()))
            .collect()
            .unwrap()
    }

    async fn send_sasl_initial_response(&mut self, mechanism: &str, data: &[u8]) {
        let mut out = BytesMut::new();
        frontend::sasl_initial_response(mechanism, data, &mut out).unwrap();
        self.send(&out).await;
    }

    async fn prove(&mut self, password: &str) -> ScramSha256 {
        let mut scram = ScramSha256::new(password.as_bytes(), ChannelBinding::unsupported());
        assert_eq!(self.read_offered_mechanisms().await, vec!["SCRAM-SHA-256"]);
        self.send_sasl_initial_response("SCRAM-SHA-256", scram.message())
            .await;

        let Message::AuthenticationSaslContinue(body) = self.read_message().await else {
            panic!("expected an AuthenticationSASLContinue");
        };
        scram.update(body.data()).unwrap();
        let mut out = BytesMut::new();
        frontend::sasl_response(scram.message(), &mut out).unwrap();
        self.send(&out).await;
        scram
    }

    async fn finish(&mut self, scram: &mut ScramSha256) {
        let Message::AuthenticationSaslFinal(body) = self.read_message().await else {
            panic!("expected an AuthenticationSASLFinal");
        };
        scram.finish(body.data()).unwrap();
        assert!(matches!(
            self.read_message().await,
            Message::AuthenticationOk
        ));
    }
}

#[derive(Debug, Clone)]
struct OneUser {
    user: String,
    password: String,
}

impl OneUser {
    fn new(user: &str, password: &str) -> Self {
        Self {
            user: user.to_owned(),
            password: password.to_owned(),
        }
    }
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

fn field(fields: &[(u8, String)], wanted: u8) -> &str {
    let Some((_, value)) = fields.iter().find(|(kind, _)| *kind == wanted) else {
        panic!("no field {:?} in {fields:?}", wanted as char);
    };
    value
}

fn startup(parameters: &[(&str, &str)]) -> StartupRequest {
    startup_with_version(ProtocolVersion::V3_0, parameters)
}

fn startup_with_version(version: ProtocolVersion, parameters: &[(&str, &str)]) -> StartupRequest {
    StartupRequest::Startup(StartupMessage::new(
        version,
        parameters
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect(),
    ))
}

fn query_frame(sql: &str) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_slice(sql.as_bytes());
    body.put_u8(0);
    let mut out = BytesMut::new();
    encode_frame(b'Q', &body, &mut out);
    out.to_vec()
}

async fn accepted_session(
    client_steps: impl AsyncFnOnce(&mut Client),
) -> ClientSession<DuplexStream> {
    let (client_stream, session_stream) = duplex(DUPLEX_CAPACITY);
    let mut client = Client::new(client_stream);
    let session = tokio::spawn(accept(session_stream, TrustAll));
    client_steps(&mut client).await;
    let accepted = session.await.unwrap().unwrap();
    let Accepted::Session(session) = accepted else {
        panic!("expected an authenticated session");
    };
    assert_eq!(client.read_frame().await.tag, b'R');
    session
}

async fn authenticated_session(
    credentials: impl Credentials + Send + 'static,
    client_steps: impl AsyncFnOnce(&mut Client),
) -> ClientSession<DuplexStream> {
    let (client_stream, session_stream) = duplex(DUPLEX_CAPACITY);
    let mut client = Client::new(client_stream);
    let session = tokio::spawn(accept(session_stream, credentials));
    client_steps(&mut client).await;
    let accepted = session.await.unwrap().unwrap();
    let Accepted::Session(session) = accepted else {
        panic!("expected an authenticated session");
    };
    session
}

async fn refused_by(
    credentials: impl Credentials + Send + 'static,
    client_steps: impl AsyncFnOnce(&mut Client),
) -> (AcceptError, Vec<(u8, String)>) {
    let (client_stream, session_stream) = duplex(DUPLEX_CAPACITY);
    let mut client = Client::new(client_stream);
    let session = tokio::spawn(accept(session_stream, credentials));
    client_steps(&mut client).await;
    let error = session.await.unwrap().expect_err("the client is refused");
    let fields = client.read_error().await;
    (error, fields)
}

#[tokio::test]
async fn a_startup_message_resolves_the_tenant_and_answers_authentication_ok() {
    let session = accepted_session(async |client| {
        client
            .send_startup(&startup(&[("user", "app_web"), ("database", "shop")]))
            .await;
    })
    .await;

    assert_eq!(session.tenant(), &TenantId::new("app_web", "shop"));
    assert_eq!(session.parameter("user"), Some("app_web"));
}

#[tokio::test]
async fn the_database_defaults_to_the_user_name() {
    let session = accepted_session(async |client| {
        client.send_startup(&startup(&[("user", "app_web")])).await;
    })
    .await;

    assert_eq!(session.tenant(), &TenantId::new("app_web", "app_web"));
}

#[tokio::test]
async fn an_ssl_request_is_refused_with_a_single_byte_and_startup_continues() {
    let session = accepted_session(async |client| {
        client.send_startup(&StartupRequest::Ssl).await;
        assert_eq!(client.read_byte().await, b'N');
        client.send_startup(&startup(&[("user", "app_web")])).await;
    })
    .await;

    assert_eq!(session.tenant().user(), "app_web");
}

#[tokio::test]
async fn a_gssenc_request_is_refused_before_an_ssl_request_is_refused() {
    let session = accepted_session(async |client| {
        client.send_startup(&StartupRequest::GssEnc).await;
        assert_eq!(client.read_byte().await, b'N');
        client.send_startup(&StartupRequest::Ssl).await;
        assert_eq!(client.read_byte().await, b'N');
        client.send_startup(&startup(&[("user", "app_web")])).await;
    })
    .await;

    assert_eq!(session.tenant().user(), "app_web");
}

#[tokio::test]
async fn bytes_sent_after_the_startup_packet_are_kept_for_the_session() {
    let session = accepted_session(async |client| {
        client.send_startup(&startup(&[("user", "app_web")])).await;
        client.send(&query_frame("SELECT 1")).await;
    })
    .await;

    let (_, pending) = session.into_parts();
    assert_eq!(&pending[..], &query_frame("SELECT 1")[..]);
}

#[tokio::test]
async fn a_cancel_request_is_returned_without_resolving_a_tenant() {
    let (client_stream, session_stream) = duplex(DUPLEX_CAPACITY);
    let mut client = Client::new(client_stream);
    let key = CancelKey {
        process_id: 4242,
        secret_key: 987_654_321,
    };
    client.send_startup(&StartupRequest::Cancel(key)).await;

    let accepted = accept(session_stream, TrustAll).await.unwrap();
    assert!(matches!(accepted, Accepted::Cancel(returned) if returned == key));
}

async fn refused(client_steps: impl AsyncFnOnce(&mut Client)) -> (AcceptError, Vec<(u8, String)>) {
    refused_by(TrustAll, client_steps).await
}

#[tokio::test]
async fn a_startup_message_without_a_user_is_refused_with_a_fatal_error() {
    let (error, fields) = refused(async |client| {
        client.send_startup(&startup(&[("database", "shop")])).await;
    })
    .await;

    assert!(matches!(error, AcceptError::Tenant(_)), "{error:?}");
    assert_eq!(field(&fields, b'S'), "FATAL");
    assert_eq!(
        field(&fields, b'C'),
        sqlstate::INVALID_AUTHORIZATION_SPECIFICATION
    );
}

#[tokio::test]
async fn an_unsupported_protocol_version_is_refused_as_an_unsupported_feature() {
    let (error, fields) = refused(async |client| {
        client
            .send_startup(&startup_with_version(
                ProtocolVersion { major: 2, minor: 0 },
                &[("user", "app_web")],
            ))
            .await;
    })
    .await;

    assert!(matches!(error, AcceptError::Startup(_)), "{error:?}");
    assert_eq!(field(&fields, b'C'), sqlstate::FEATURE_NOT_SUPPORTED);
    assert!(field(&fields, b'M').contains("3.0"), "{fields:?}");
}

#[tokio::test]
async fn a_repeated_ssl_request_is_a_protocol_violation() {
    let (error, fields) = refused(async |client| {
        client.send_startup(&StartupRequest::Ssl).await;
        assert_eq!(client.read_byte().await, b'N');
        client.send_startup(&StartupRequest::Ssl).await;
    })
    .await;

    assert!(
        matches!(error, AcceptError::RepeatedEncryptionRequest(_)),
        "{error:?}"
    );
    assert_eq!(field(&fields, b'C'), sqlstate::PROTOCOL_VIOLATION);
}

#[tokio::test]
async fn a_startup_packet_over_the_limit_is_refused_before_it_is_read() {
    let (error, fields) = refused(async |client| {
        let mut oversized = BytesMut::new();
        oversized.put_i32(10_001);
        client.send(&oversized).await;
    })
    .await;

    assert!(matches!(error, AcceptError::Frame(_)), "{error:?}");
    assert_eq!(field(&fields, b'C'), sqlstate::PROTOCOL_VIOLATION);
}

#[tokio::test]
async fn a_client_that_closes_before_the_startup_packet_is_not_an_error_response() {
    let (client_stream, session_stream) = duplex(DUPLEX_CAPACITY);
    drop(client_stream);

    let error = accept(session_stream, TrustAll)
        .await
        .expect_err("no startup packet");
    assert!(matches!(error, AcceptError::ConnectionClosed), "{error:?}");
}

#[tokio::test]
async fn a_client_that_proves_its_password_is_answered_authentication_ok() {
    let session = authenticated_session(OneUser::new("app_web", "secret"), async |client| {
        client
            .send_startup(&startup(&[("user", "app_web"), ("database", "shop")]))
            .await;
        let mut scram = client.prove("secret").await;
        client.finish(&mut scram).await;
    })
    .await;

    assert_eq!(session.tenant(), &TenantId::new("app_web", "shop"));
}

#[tokio::test]
async fn a_wrong_password_is_refused_with_the_sqlstate_postgres_uses() {
    let (error, fields) = refused_by(OneUser::new("app_web", "secret"), async |client| {
        client.send_startup(&startup(&[("user", "app_web")])).await;
        client.prove("wrong").await;
    })
    .await;

    assert!(
        matches!(error, AcceptError::AuthenticationFailed(ref user) if user == "app_web"),
        "{error:?}"
    );
    assert_eq!(field(&fields, b'S'), "FATAL");
    assert_eq!(field(&fields, b'C'), sqlstate::INVALID_PASSWORD);
}

#[tokio::test]
async fn a_user_without_credentials_is_refused_exactly_as_a_wrong_password_is() {
    let (error, fields) = refused_by(OneUser::new("app_web", "secret"), async |client| {
        client.send_startup(&startup(&[("user", "nobody")])).await;
        client.prove("secret").await;
    })
    .await;

    assert!(
        matches!(error, AcceptError::AuthenticationFailed(ref user) if user == "nobody"),
        "{error:?}"
    );
    assert_eq!(field(&fields, b'C'), sqlstate::INVALID_PASSWORD);
    assert_eq!(
        field(&fields, b'M'),
        "password authentication failed for user \"nobody\""
    );
}

#[tokio::test]
async fn a_mechanism_this_node_does_not_offer_is_refused() {
    let (error, fields) = refused_by(OneUser::new("app_web", "secret"), async |client| {
        client.send_startup(&startup(&[("user", "app_web")])).await;
        assert_eq!(
            client.read_offered_mechanisms().await,
            vec!["SCRAM-SHA-256"]
        );
        client
            .send_sasl_initial_response("SCRAM-SHA-256-PLUS", b"p=tls-server-end-point,,n=,r=abc")
            .await;
    })
    .await;

    assert!(
        matches!(error, AcceptError::UnsupportedMechanism(ref asked) if asked == "SCRAM-SHA-256-PLUS"),
        "{error:?}"
    );
    assert_eq!(field(&fields, b'C'), sqlstate::FEATURE_NOT_SUPPORTED);
}

#[tokio::test]
async fn a_query_sent_where_a_sasl_response_belongs_is_a_protocol_violation() {
    let (error, fields) = refused_by(OneUser::new("app_web", "secret"), async |client| {
        client.send_startup(&startup(&[("user", "app_web")])).await;
        assert_eq!(
            client.read_offered_mechanisms().await,
            vec!["SCRAM-SHA-256"]
        );
        client.send(&query_frame("SELECT 1")).await;
    })
    .await;

    assert!(
        matches!(error, AcceptError::UnexpectedMessage(b'Q')),
        "{error:?}"
    );
    assert_eq!(field(&fields, b'C'), sqlstate::PROTOCOL_VIOLATION);
}

#[tokio::test]
async fn a_client_that_closes_during_authentication_is_not_an_error_response() {
    let (client_stream, session_stream) = duplex(DUPLEX_CAPACITY);
    let mut client = Client::new(client_stream);
    let session = tokio::spawn(accept(session_stream, OneUser::new("app_web", "secret")));
    client.send_startup(&startup(&[("user", "app_web")])).await;
    assert_eq!(
        client.read_offered_mechanisms().await,
        vec!["SCRAM-SHA-256"]
    );
    drop(client);

    let error = session.await.unwrap().expect_err("no SASL response");
    assert!(matches!(error, AcceptError::ConnectionClosed), "{error:?}");
}
