use std::sync::Arc;

use bytes::BytesMut;
use pgsteward_core::auth::TrustAll;
use pgsteward_core::session::{AcceptError, Accepted, accept};
use pgsteward_core::tenant::TenantId;
use pgsteward_core::tls::ClientTls;
use pgsteward_protocol::framing::{Frame, decode_frame};
use pgsteward_protocol::startup::{
    CancelKey, ProtocolVersion, StartupMessage, StartupRequest, encode_startup,
};
use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, duplex};
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};
use tokio_rustls::rustls::{ClientConfig, RootCertStore, crypto::ring};

const DUPLEX_CAPACITY: usize = 64 * 1024;
const MAX_FRAME: usize = 1 << 20;
const SERVER_NAME: &str = "pgsteward.test";

struct Authority {
    cert_pem: String,
    key_pem: String,
    root: CertificateDer<'static>,
}

fn authority() -> Authority {
    let root_key = KeyPair::generate().unwrap();
    let mut root_params = CertificateParams::new(Vec::new()).unwrap();
    root_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let root = root_params.self_signed(&root_key).unwrap();
    let issuer = Issuer::new(root_params, root_key);

    let key = KeyPair::generate().unwrap();
    let params = CertificateParams::new(vec![SERVER_NAME.to_owned()]).unwrap();
    let cert = params.signed_by(&key, &issuer).unwrap();

    Authority {
        cert_pem: cert.pem(),
        key_pem: key.serialize_pem(),
        root: root.der().clone(),
    }
}

impl Authority {
    fn node(&self) -> ClientTls {
        ClientTls::from_pem(self.cert_pem.as_bytes(), self.key_pem.as_bytes()).unwrap()
    }

    fn connector(&self) -> TlsConnector {
        let mut roots = RootCertStore::empty();
        roots.add(self.root.clone()).unwrap();
        let config = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        TlsConnector::from(Arc::new(config))
    }
}

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

    async fn send_startup(&mut self, request: &StartupRequest) {
        let mut out = BytesMut::new();
        encode_startup(request, &mut out);
        self.stream.write_all(&out).await.unwrap();
        self.stream.flush().await.unwrap();
    }

    async fn read_byte(&mut self) -> u8 {
        while self.buf.is_empty() {
            let read = self.stream.read_buf(&mut self.buf).await.unwrap();
            assert!(read > 0, "the session closed before answering");
        }
        self.buf.split_to(1)[0]
    }

    async fn handshake(self, authority: &Authority) -> Encrypted {
        assert!(self.buf.is_empty(), "the node sent more than the one byte");
        let name = ServerName::try_from(SERVER_NAME).unwrap();
        let stream = authority
            .connector()
            .connect(name, self.stream)
            .await
            .unwrap();
        Encrypted {
            stream,
            buf: BytesMut::new(),
        }
    }
}

struct Encrypted {
    stream: TlsStream<DuplexStream>,
    buf: BytesMut,
}

impl Encrypted {
    async fn send_startup(&mut self, request: &StartupRequest) {
        let mut out = BytesMut::new();
        encode_startup(request, &mut out);
        self.stream.write_all(&out).await.unwrap();
        self.stream.flush().await.unwrap();
    }

    async fn read_frame(&mut self) -> Frame {
        loop {
            if let Some(frame) = decode_frame(&mut self.buf, MAX_FRAME).unwrap() {
                return frame;
            }
            let read = self.stream.read_buf(&mut self.buf).await.unwrap();
            assert!(read > 0, "the session closed before answering");
        }
    }
}

fn startup(parameters: &[(&str, &str)]) -> StartupRequest {
    StartupRequest::Startup(StartupMessage::new(
        ProtocolVersion::V3_0,
        parameters
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect(),
    ))
}

#[tokio::test]
async fn a_client_that_asks_for_tls_finishes_its_startup_over_the_encrypted_connection() {
    let authority = authority();
    let tls = authority.node();
    let (client_stream, session_stream) = duplex(DUPLEX_CAPACITY);
    let session = tokio::spawn(async move { accept(session_stream, TrustAll, Some(&tls)).await });

    let mut client = Client::new(client_stream);
    client.send_startup(&StartupRequest::Ssl).await;
    assert_eq!(client.read_byte().await, b'S');
    let mut client = client.handshake(&authority).await;
    client
        .send_startup(&startup(&[("user", "app_web"), ("database", "shop")]))
        .await;

    let accepted = session.await.unwrap().unwrap();
    let Accepted::Session(session) = accepted else {
        panic!("expected an authenticated session");
    };
    assert_eq!(session.tenant(), &TenantId::new("app_web", "shop"));
    assert_eq!(client.read_frame().await.tag, b'R');
}

#[tokio::test]
async fn a_cancel_request_arrives_over_the_encrypted_connection_too() {
    let authority = authority();
    let tls = authority.node();
    let (client_stream, session_stream) = duplex(DUPLEX_CAPACITY);
    let session = tokio::spawn(async move { accept(session_stream, TrustAll, Some(&tls)).await });

    let key = CancelKey {
        process_id: 4242,
        secret_key: 987_654_321,
    };
    let mut client = Client::new(client_stream);
    client.send_startup(&StartupRequest::Ssl).await;
    assert_eq!(client.read_byte().await, b'S');
    let mut client = client.handshake(&authority).await;
    client.send_startup(&StartupRequest::Cancel(key)).await;

    let accepted = session.await.unwrap().unwrap();
    assert!(matches!(accepted, Accepted::Cancel(returned) if returned == key));
}

#[tokio::test]
async fn unencrypted_bytes_sent_with_the_ssl_request_are_refused_without_a_reply() {
    let authority = authority();
    let tls = authority.node();
    let (client_stream, session_stream) = duplex(DUPLEX_CAPACITY);
    let session = tokio::spawn(async move { accept(session_stream, TrustAll, Some(&tls)).await });

    let mut client = Client::new(client_stream);
    let mut out = BytesMut::new();
    encode_startup(&StartupRequest::Ssl, &mut out);
    encode_startup(&startup(&[("user", "app_web")]), &mut out);
    client.stream.write_all(&out).await.unwrap();
    client.stream.flush().await.unwrap();

    let error = session.await.unwrap().expect_err("the client is refused");
    assert!(
        matches!(error, AcceptError::UnencryptedData),
        "expected the startup packet sent in the clear to be refused, got {error}"
    );
    assert_eq!(client.read_byte().await, b'S');
    let mut buf = BytesMut::new();
    assert_eq!(
        client.stream.read_buf(&mut buf).await.unwrap(),
        0,
        "nothing the node says after the S can be read by a client that is waiting for TLS"
    );
}

#[tokio::test]
async fn a_second_ssl_request_inside_the_tls_connection_is_a_protocol_violation() {
    let authority = authority();
    let tls = authority.node();
    let (client_stream, session_stream) = duplex(DUPLEX_CAPACITY);
    let session = tokio::spawn(async move { accept(session_stream, TrustAll, Some(&tls)).await });

    let mut client = Client::new(client_stream);
    client.send_startup(&StartupRequest::Ssl).await;
    assert_eq!(client.read_byte().await, b'S');
    let mut client = client.handshake(&authority).await;
    client.send_startup(&StartupRequest::Ssl).await;

    let error = session.await.unwrap().expect_err("the client is refused");
    assert!(
        matches!(error, AcceptError::RepeatedEncryptionRequest(_)),
        "expected the repeated request to be refused, got {error}"
    );
    assert_eq!(client.read_frame().await.tag, b'E');
}

#[tokio::test]
async fn a_gssenc_request_is_refused_and_the_tls_request_after_it_is_accepted() {
    let authority = authority();
    let tls = authority.node();
    let (client_stream, session_stream) = duplex(DUPLEX_CAPACITY);
    let session = tokio::spawn(async move { accept(session_stream, TrustAll, Some(&tls)).await });

    let mut client = Client::new(client_stream);
    client.send_startup(&StartupRequest::GssEnc).await;
    assert_eq!(client.read_byte().await, b'N');
    client.send_startup(&StartupRequest::Ssl).await;
    assert_eq!(client.read_byte().await, b'S');
    let mut client = client.handshake(&authority).await;
    client.send_startup(&startup(&[("user", "app_web")])).await;

    let accepted = session.await.unwrap().unwrap();
    let Accepted::Session(session) = accepted else {
        panic!("expected an authenticated session");
    };
    assert_eq!(session.tenant().user(), "app_web");
}
