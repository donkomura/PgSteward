use std::fmt;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::rustls::pki_types::pem::{Error as PemError, PemObject};
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::{self, ServerConfig};
use tokio_rustls::server::TlsStream;
use tokio_rustls::{TlsAcceptor, rustls::crypto::ring};

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

/// A client connection, either as it arrived or after this node answered its
/// TLS request. The protocol asks for the encryption before the startup packet,
/// so the same session code runs over both.
#[derive(Debug)]
pub enum MaybeTls<S> {
    Plain(S),
    Encrypted(Box<TlsStream<S>>),
}

impl<S: AsyncRead + AsyncWrite + Unpin> MaybeTls<S> {
    pub(crate) async fn encrypt(self, tls: &ClientTls) -> io::Result<Self> {
        match self {
            Self::Plain(stream) => tls
                .acceptor
                .accept(stream)
                .await
                .map(|stream| Self::Encrypted(Box::new(stream))),
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
