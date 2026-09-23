use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use pgsteward_core::server::{
    ApplicationName, ConnectError, ServerConnection, ServerCredentials, request_encryption,
};
use pgsteward_core::tls::{ServerTls, SslMode, TlsError};
use pgsteward_protocol::backend::{
    encode_backend_key_data, encode_parameter_status, encode_ready_for_query,
};
use pgsteward_protocol::framing::{decode_startup_frame, encode_frame};
use pgsteward_protocol::message::TransactionStatus;
use pgsteward_protocol::startup::{CancelKey, StartupRequest, decode_startup};
use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, duplex};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::{ServerConfig, crypto::ring};

const DUPLEX_CAPACITY: usize = 64 * 1024;
const MAX_FRAME: usize = 1 << 20;
const SERVER_NAME: &str = "db-primary.internal";
const ANOTHER_NAME: &str = "db-replica-1.internal";

struct Authority {
    certificate: String,
    key: String,
    root: String,
}

fn authority(name: &str) -> Authority {
    let root_key = KeyPair::generate().unwrap();
    let mut root_params = CertificateParams::new(Vec::new()).unwrap();
    root_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let root = root_params.self_signed(&root_key).unwrap();
    let issuer = Issuer::new(root_params, root_key);

    let key = KeyPair::generate().unwrap();
    let params = CertificateParams::new(vec![name.to_owned()]).unwrap();
    let cert = params.signed_by(&key, &issuer).unwrap();

    Authority {
        certificate: cert.pem(),
        key: key.serialize_pem(),
        root: root.pem(),
    }
}

impl Authority {
    fn acceptor(&self) -> TlsAcceptor {
        let chain = CertificateDer::pem_slice_iter(self.certificate.as_bytes())
            .collect::<Result<Vec<CertificateDer<'static>>, _>>()
            .unwrap();
        let key = PrivateKeyDer::from_pem_slice(self.key.as_bytes()).unwrap();
        let config = ServerConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .unwrap();
        TlsAcceptor::from(Arc::new(config))
    }

    fn root(&self) -> &[u8] {
        self.root.as_bytes()
    }
}

fn credentials() -> ServerCredentials {
    ServerCredentials {
        user: "app_web".to_owned(),
        database: "shop".to_owned(),
        password: None,
    }
}

fn successful_startup_tail() -> BytesMut {
    let mut out = BytesMut::new();
    let mut authentication = BytesMut::new();
    authentication.put_i32(0);
    encode_frame(b'R', &authentication, &mut out);
    encode_parameter_status("server_version", "16.4", &mut out);
    encode_backend_key_data(
        CancelKey {
            process_id: 4242,
            secret_key: 24,
        },
        &mut out,
    );
    encode_ready_for_query(TransactionStatus::Idle, &mut out);
    out
}

async fn read_startup_request<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
) -> StartupRequest {
    loop {
        if let Some(body) = decode_startup_frame(buf, MAX_FRAME).unwrap() {
            return decode_startup(&body).unwrap();
        }
        let read = stream.read_buf(buf).await.unwrap();
        assert!(read > 0, "the connection closed before a startup packet");
    }
}

/// The server side of the `SSLRequest` exchange: read the request and answer with
/// the one byte, leaving the stream where the startup packet follows.
async fn answer_encryption_request(stream: &mut DuplexStream, answer: u8) {
    let mut buf = BytesMut::new();
    let request = read_startup_request(stream, &mut buf).await;
    assert!(
        matches!(request, StartupRequest::Ssl),
        "expected an SSLRequest, got {request:?}"
    );
    assert!(buf.is_empty(), "the client sent more than the SSLRequest");
    stream.write_all(&[answer]).await.unwrap();
    stream.flush().await.unwrap();
}

async fn greet<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S) -> StartupRequest {
    let mut buf = BytesMut::new();
    let request = read_startup_request(stream, &mut buf).await;
    stream.write_all(&successful_startup_tail()).await.unwrap();
    stream.flush().await.unwrap();
    request
}

async fn greet_encrypted(acceptor: TlsAcceptor, stream: DuplexStream) -> StartupRequest {
    let mut stream = acceptor.accept(stream).await.unwrap();
    greet(&mut stream).await
}

async fn finish_handshake<S: AsyncRead + AsyncWrite + Unpin>(stream: S) -> ServerConnection<S> {
    ServerConnection::handshake(stream, &credentials(), &ApplicationName::new("node-1"))
        .await
        .unwrap()
}

#[tokio::test]
async fn disable_sends_the_startup_packet_without_asking_for_encryption() {
    let (client, mut db) = duplex(DUPLEX_CAPACITY);
    let server = tokio::spawn(async move { greet(&mut db).await });

    let stream = request_encryption(client, &ServerTls::disabled(), SERVER_NAME)
        .await
        .unwrap();
    assert!(!stream.is_encrypted());
    let connection = finish_handshake(stream).await;

    assert_eq!(connection.parameter("server_version"), Some("16.4"));
    assert!(
        matches!(server.await.unwrap(), StartupRequest::Startup(_)),
        "the first packet a disabled connection sends is the startup packet"
    );
}

#[tokio::test]
async fn prefer_carries_on_in_the_clear_when_the_server_refuses_encryption() {
    let (client, mut db) = duplex(DUPLEX_CAPACITY);
    let server = tokio::spawn(async move {
        answer_encryption_request(&mut db, b'N').await;
        greet(&mut db).await
    });

    let tls = ServerTls::new(SslMode::Prefer, None).unwrap();
    let stream = request_encryption(client, &tls, SERVER_NAME).await.unwrap();
    assert!(!stream.is_encrypted());
    let connection = finish_handshake(stream).await;

    assert_eq!(connection.parameter("server_version"), Some("16.4"));
    assert!(matches!(server.await.unwrap(), StartupRequest::Startup(_)));
}

#[tokio::test]
async fn require_refuses_a_server_that_does_not_encrypt() {
    let (client, mut db) = duplex(DUPLEX_CAPACITY);
    let server = tokio::spawn(async move {
        answer_encryption_request(&mut db, b'N').await;
        db
    });

    let tls = ServerTls::new(SslMode::Require, None).unwrap();
    let error = request_encryption(client, &tls, SERVER_NAME)
        .await
        .expect_err("a server that answers N cannot serve `require`");

    assert!(
        matches!(error, ConnectError::EncryptionRefused),
        "expected the refusal to be reported, got {error}"
    );
    drop(server.await.unwrap());
}

#[tokio::test]
async fn require_takes_a_certificate_no_authority_this_node_knows_signed() {
    let authority = authority(SERVER_NAME);
    let acceptor = authority.acceptor();
    let (client, mut db) = duplex(DUPLEX_CAPACITY);
    let server = tokio::spawn(async move {
        answer_encryption_request(&mut db, b'S').await;
        greet_encrypted(acceptor, db).await
    });

    let tls = ServerTls::new(SslMode::Require, None).unwrap();
    let stream = request_encryption(client, &tls, SERVER_NAME).await.unwrap();
    assert!(stream.is_encrypted());
    let connection = finish_handshake(stream).await;

    assert_eq!(connection.parameter("server_version"), Some("16.4"));
    assert_eq!(connection.backend_key().process_id, 4242);
    assert!(matches!(server.await.unwrap(), StartupRequest::Startup(_)));
}

#[tokio::test]
async fn verify_full_takes_a_certificate_its_root_signed_for_that_name() {
    let authority = authority(SERVER_NAME);
    let acceptor = authority.acceptor();
    let (client, mut db) = duplex(DUPLEX_CAPACITY);
    let server = tokio::spawn(async move {
        answer_encryption_request(&mut db, b'S').await;
        greet_encrypted(acceptor, db).await
    });

    let tls = ServerTls::new(SslMode::VerifyFull, Some(authority.root())).unwrap();
    let stream = request_encryption(client, &tls, SERVER_NAME).await.unwrap();
    assert!(stream.is_encrypted());
    let connection = finish_handshake(stream).await;

    assert_eq!(connection.parameter("server_version"), Some("16.4"));
    assert!(matches!(server.await.unwrap(), StartupRequest::Startup(_)));
}

#[tokio::test]
async fn verify_full_refuses_a_certificate_issued_for_another_name() {
    let authority = authority(ANOTHER_NAME);
    let acceptor = authority.acceptor();
    let (client, mut db) = duplex(DUPLEX_CAPACITY);
    tokio::spawn(async move {
        answer_encryption_request(&mut db, b'S').await;
        let _ = acceptor.accept(db).await;
    });

    let tls = ServerTls::new(SslMode::VerifyFull, Some(authority.root())).unwrap();
    let error = request_encryption(client, &tls, SERVER_NAME)
        .await
        .expect_err("the certificate does not name this instance");

    assert!(
        matches!(error, ConnectError::Encryption(_)),
        "expected the handshake to fail, got {error}"
    );
}

#[tokio::test]
async fn verify_ca_takes_a_certificate_its_root_signed_for_another_name() {
    let authority = authority(ANOTHER_NAME);
    let acceptor = authority.acceptor();
    let (client, mut db) = duplex(DUPLEX_CAPACITY);
    let server = tokio::spawn(async move {
        answer_encryption_request(&mut db, b'S').await;
        greet_encrypted(acceptor, db).await
    });

    let tls = ServerTls::new(SslMode::VerifyCa, Some(authority.root())).unwrap();
    let stream = request_encryption(client, &tls, SERVER_NAME).await.unwrap();
    assert!(stream.is_encrypted());
    let connection = finish_handshake(stream).await;

    assert_eq!(connection.parameter("server_version"), Some("16.4"));
    assert!(matches!(server.await.unwrap(), StartupRequest::Startup(_)));
}

#[tokio::test]
async fn verify_ca_refuses_a_certificate_another_authority_signed() {
    let known = authority(SERVER_NAME);
    let acceptor = authority(SERVER_NAME).acceptor();
    let (client, mut db) = duplex(DUPLEX_CAPACITY);
    tokio::spawn(async move {
        answer_encryption_request(&mut db, b'S').await;
        let _ = acceptor.accept(db).await;
    });

    let tls = ServerTls::new(SslMode::VerifyCa, Some(known.root())).unwrap();
    let error = request_encryption(client, &tls, SERVER_NAME)
        .await
        .expect_err("no authority this node knows signed the certificate");

    assert!(
        matches!(error, ConnectError::Encryption(_)),
        "expected the handshake to fail, got {error}"
    );
}

#[tokio::test]
async fn an_answer_that_is_neither_yes_nor_no_is_refused() {
    let (client, mut db) = duplex(DUPLEX_CAPACITY);
    tokio::spawn(async move {
        answer_encryption_request(&mut db, b'E').await;
        let mut sink = Vec::new();
        let _ = db.read_to_end(&mut sink).await;
    });

    let tls = ServerTls::new(SslMode::Prefer, None).unwrap();
    let error = request_encryption(client, &tls, SERVER_NAME)
        .await
        .expect_err("only S and N answer an SSLRequest");

    assert!(
        matches!(error, ConnectError::EncryptionAnswer(b'E')),
        "expected the answer to be reported, got {error}"
    );
}

#[test]
fn a_verifying_mode_needs_a_root_certificate() {
    for mode in [SslMode::VerifyCa, SslMode::VerifyFull] {
        let error = ServerTls::new(mode, None)
            .expect_err("a mode that verifies cannot work without a root certificate");
        assert!(
            matches!(error, TlsError::NoRootCertificate),
            "expected {mode:?} to ask for a root certificate, got {error}"
        );
    }
}

#[test]
fn a_root_certificate_that_is_not_a_certificate_is_refused() {
    let error = ServerTls::new(SslMode::VerifyFull, Some(b"not a certificate"))
        .expect_err("the root certificate is read at startup");

    assert!(
        matches!(error, TlsError::RootCertificate(_)),
        "expected the root certificate to be reported, got {error}"
    );
}

#[test]
fn the_mode_a_connection_runs_under_is_the_one_it_was_built_with() {
    assert_eq!(ServerTls::disabled().mode(), SslMode::Disable);
    assert_eq!(
        ServerTls::new(SslMode::Require, None).unwrap().mode(),
        SslMode::Require
    );
    assert_eq!(SslMode::default(), SslMode::Prefer);
}
