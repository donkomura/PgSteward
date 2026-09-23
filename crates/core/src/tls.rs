use std::fmt;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use tokio_rustls::rustls::client::{
    VerifierBuilderError, WebPkiServerVerifier, verify_server_cert_signed_by_trust_anchor,
};
use tokio_rustls::rustls::crypto::{
    CryptoProvider, verify_tls12_signature, verify_tls13_signature,
};
use tokio_rustls::rustls::pki_types::pem::{Error as PemError, PemObject};
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use tokio_rustls::rustls::server::ParsedCertificate;
use tokio_rustls::rustls::{
    self, ClientConfig, DigitallySignedStruct, RootCertStore, ServerConfig, SignatureScheme,
};
use tokio_rustls::{TlsAcceptor, TlsConnector, TlsStream, rustls::crypto::ring};

#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    #[error("cannot read a PEM certificate chain: {0}")]
    Certificate(PemError),
    #[error("holds no certificate")]
    NoCertificate,
    #[error("cannot read a PEM private key: {0}")]
    PrivateKey(PemError),
    #[error("the certificate and the key do not make a TLS configuration: {0}")]
    Unusable(#[from] rustls::Error),
    #[error("cannot read a PEM root certificate: {0}")]
    RootCertificate(PemError),
    #[error("holds no root certificate to verify the server with")]
    NoRootCertificate,
    #[error("the root certificate does not make a verifier: {0}")]
    Verifier(#[from] VerifierBuilderError),
}

/// What this node terminates client TLS with.
#[derive(Clone)]
pub struct ClientTls {
    acceptor: TlsAcceptor,
}

impl fmt::Debug for ClientTls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ClientTls")
    }
}

impl ClientTls {
    pub fn from_pem(certificates: &[u8], private_key: &[u8]) -> Result<Self, TlsError> {
        let chain = CertificateDer::pem_slice_iter(certificates)
            .collect::<Result<Vec<CertificateDer<'static>>, PemError>>()
            .map_err(TlsError::Certificate)?;
        if chain.is_empty() {
            return Err(TlsError::NoCertificate);
        }
        let key = PrivateKeyDer::from_pem_slice(private_key).map_err(TlsError::PrivateKey)?;
        let config = ServerConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_safe_default_protocol_versions()?
            .with_no_client_auth()
            .with_single_cert(chain, key)?;
        Ok(Self {
            acceptor: TlsAcceptor::from(Arc::new(config)),
        })
    }
}

/// How far a server connection goes to encrypt, named after libpq's `sslmode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SslMode {
    Disable,
    #[default]
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

impl SslMode {
    #[must_use]
    pub fn demands_encryption(self) -> bool {
        matches!(self, Self::Require | Self::VerifyCa | Self::VerifyFull)
    }

    fn verifies(self) -> bool {
        matches!(self, Self::VerifyCa | Self::VerifyFull)
    }
}

impl fmt::Display for SslMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Disable => "disable",
            Self::Prefer => "prefer",
            Self::Require => "require",
            Self::VerifyCa => "verify-ca",
            Self::VerifyFull => "verify-full",
        })
    }
}

/// How this node encrypts the connections it opens to an instance.
#[derive(Clone)]
pub struct ServerTls {
    mode: SslMode,
    connector: Option<TlsConnector>,
}

impl fmt::Debug for ServerTls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerTls")
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

impl ServerTls {
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            mode: SslMode::Disable,
            connector: None,
        }
    }

    /// `verify-ca` and `verify-full` read the authorities they trust from
    /// `root_certificates`; the other modes never look at a certificate and
    /// take none.
    pub fn new(mode: SslMode, root_certificates: Option<&[u8]>) -> Result<Self, TlsError> {
        let provider = Arc::new(ring::default_provider());
        let verifier: Option<Arc<dyn ServerCertVerifier>> = match mode {
            SslMode::Disable => None,
            SslMode::Prefer | SslMode::Require => Some(Arc::new(AnyCertificate {
                provider: Arc::clone(&provider),
            })),
            SslMode::VerifyCa => Some(Arc::new(SignedByKnownAuthority {
                roots: root_store(root_certificates)?,
                provider: Arc::clone(&provider),
            })),
            SslMode::VerifyFull => Some(
                WebPkiServerVerifier::builder_with_provider(
                    root_store(root_certificates)?,
                    Arc::clone(&provider),
                )
                .build()?,
            ),
        };
        let connector = verifier
            .map(|verifier| {
                let config = ClientConfig::builder_with_provider(provider)
                    .with_safe_default_protocol_versions()?
                    .dangerous()
                    .with_custom_certificate_verifier(verifier)
                    .with_no_client_auth();
                Ok::<_, TlsError>(TlsConnector::from(Arc::new(config)))
            })
            .transpose()?;
        Ok(Self { mode, connector })
    }

    #[must_use]
    pub fn mode(&self) -> SslMode {
        self.mode
    }

    pub(crate) async fn encrypt<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: S,
        host: &str,
    ) -> io::Result<MaybeTls<S>> {
        let connector = self
            .connector
            .as_ref()
            .ok_or_else(|| io::Error::other("this connection does not encrypt"))?;
        let stream = connector.connect(self.server_name(host)?, stream).await?;
        Ok(MaybeTls::Encrypted(Box::new(stream.into())))
    }

    /// The name the certificate is checked against, and the SNI this node
    /// sends. A mode that does not check the name takes any host as it stands,
    /// so an address no name can be made of still connects.
    fn server_name(&self, host: &str) -> io::Result<ServerName<'static>> {
        match ServerName::try_from(host.to_owned()) {
            Ok(name) => Ok(name),
            Err(source) if self.mode.verifies() => Err(io::Error::other(format!(
                "`{host}` is not a name a certificate can be checked against: {source}"
            ))),
            Err(_) => Ok(ServerName::try_from(UNVERIFIED_NAME).expect("a literal server name")),
        }
    }
}

const UNVERIFIED_NAME: &str = "unverified.invalid";

fn root_store(root_certificates: Option<&[u8]>) -> Result<Arc<RootCertStore>, TlsError> {
    let Some(pem) = root_certificates else {
        return Err(TlsError::NoRootCertificate);
    };
    let certificates = CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<CertificateDer<'static>>, PemError>>()
        .map_err(TlsError::RootCertificate)?;
    if certificates.is_empty() {
        return Err(TlsError::RootCertificate(PemError::NoItemsFound));
    }
    let mut roots = RootCertStore::empty();
    for certificate in certificates {
        roots.add(certificate)?;
    }
    Ok(Arc::new(roots))
}

/// `require` encrypts without saying anything about who is on the other end,
/// the same as libpq's `sslmode = require`.
#[derive(Debug)]
struct AnyCertificate {
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for AnyCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// `verify-ca` asks that a known authority signed the certificate and stops
/// there: the name on it is not the name this node dialed when an instance is
/// reached through an address the certificate does not carry.
#[derive(Debug)]
struct SignedByKnownAuthority {
    roots: Arc<RootCertStore>,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for SignedByKnownAuthority {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        verify_server_cert_signed_by_trust_anchor(
            &ParsedCertificate::try_from(end_entity)?,
            &self.roots,
            intermediates,
            now,
            self.provider.signature_verification_algorithms.all,
        )?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// A connection, either as it stands or once it was encrypted. The protocol
/// settles the encryption before the startup packet, so the same code runs over
/// both a client connection this node terminated and a server connection it
/// opened.
#[derive(Debug)]
pub enum MaybeTls<S> {
    Plain(S),
    Encrypted(Box<TlsStream<S>>),
}

impl<S> MaybeTls<S> {
    #[must_use]
    pub fn is_encrypted(&self) -> bool {
        matches!(self, Self::Encrypted(_))
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> MaybeTls<S> {
    pub(crate) async fn encrypt(self, tls: &ClientTls) -> io::Result<Self> {
        match self {
            Self::Plain(stream) => tls
                .acceptor
                .accept(stream)
                .await
                .map(|stream| Self::Encrypted(Box::new(stream.into()))),
            Self::Encrypted(_) => Err(io::Error::other("the connection is already encrypted")),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for MaybeTls<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Encrypted(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for MaybeTls<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Encrypted(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Self::Encrypted(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Encrypted(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}
