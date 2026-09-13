use std::fmt;
use std::io;

use bytes::BytesMut;
use pgsteward_protocol::backend::{
    EncryptionResponse, ErrorResponse, encode_authentication_ok, encode_encryption_response,
    encode_error_response, sqlstate,
};
use pgsteward_protocol::framing::{FrameError, decode_startup_frame};
use pgsteward_protocol::startup::{
    CancelKey, SUPPORTED_MAJOR, StartupError, StartupMessage, StartupRequest, decode_startup,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::tenant::{TenantId, TenantResolveError};

const MAX_STARTUP_PACKET: usize = 10_000;
const READ_CHUNK: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Encryption {
    Tls,
    GssApi,
}

impl fmt::Display for Encryption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Tls => "TLS",
            Self::GssApi => "GSSAPI",
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AcceptError {
    #[error("i/o error while accepting a client: {0}")]
    Io(#[from] io::Error),
    #[error("the client closed the connection before sending a startup packet")]
    ConnectionClosed,
    #[error("malformed startup packet: {0}")]
    Frame(FrameError),
    #[error(transparent)]
    Startup(StartupError),
    #[error(transparent)]
    Tenant(TenantResolveError),
    #[error("the client asked for {0} encryption twice")]
    RepeatedEncryptionRequest(Encryption),
}

impl AcceptError {
    fn response(&self) -> Option<ErrorResponse> {
        match self {
            Self::Io(_) | Self::ConnectionClosed => None,
            Self::Startup(StartupError::UnsupportedProtocolVersion(version)) => {
                Some(ErrorResponse::fatal(
                    sqlstate::FEATURE_NOT_SUPPORTED,
                    format!(
                        "unsupported frontend protocol {version}; this node speaks {SUPPORTED_MAJOR}.0"
                    ),
                ))
            }
            Self::Tenant(_) => Some(
                ErrorResponse::fatal(
                    sqlstate::INVALID_AUTHORIZATION_SPECIFICATION,
                    self.to_string(),
                )
                .with_hint("Name the user in the connection string."),
            ),
            Self::Frame(_) | Self::RepeatedEncryptionRequest(_) | Self::Startup(_) => Some(
                ErrorResponse::fatal(sqlstate::PROTOCOL_VIOLATION, self.to_string()),
            ),
        }
    }
}

#[derive(Debug)]
pub enum Accepted<S> {
    Session(ClientSession<S>),
    Cancel(CancelKey),
}

#[derive(Debug)]
pub struct ClientSession<S> {
    stream: S,
    pending: BytesMut,
    tenant: TenantId,
    startup: StartupMessage,
}

impl<S> ClientSession<S> {
    #[must_use]
    pub fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    #[must_use]
    pub fn parameters(&self) -> &[(String, String)] {
        self.startup.parameters()
    }

    #[must_use]
    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.startup.parameter(name)
    }

    #[must_use]
    pub fn into_parts(self) -> (S, BytesMut) {
        (self.stream, self.pending)
    }
}

pub async fn accept<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
) -> Result<Accepted<S>, AcceptError> {
    let mut pending = BytesMut::with_capacity(READ_CHUNK);
    let mut asked = Negotiation::default();
    loop {
        let request = match read_startup(&mut stream, &mut pending).await {
            Ok(request) => request,
            Err(error) => return Err(refuse(&mut stream, error).await),
        };
        let encryption = match request {
            StartupRequest::Ssl => Encryption::Tls,
            StartupRequest::GssEnc => Encryption::GssApi,
            StartupRequest::Cancel(key) => return Ok(Accepted::Cancel(key)),
            StartupRequest::Startup(startup) => {
                let tenant = match TenantId::from_startup(&startup) {
                    Ok(tenant) => tenant,
                    Err(error) => {
                        return Err(refuse(&mut stream, AcceptError::Tenant(error)).await);
                    }
                };
                write(&mut stream, encode_authentication_ok).await?;
                return Ok(Accepted::Session(ClientSession {
                    stream,
                    pending,
                    tenant,
                    startup,
                }));
            }
        };
        if !asked.first_time(encryption) {
            let error = AcceptError::RepeatedEncryptionRequest(encryption);
            return Err(refuse(&mut stream, error).await);
        }
        write(&mut stream, |out| {
            encode_encryption_response(EncryptionResponse::Refused, out);
        })
        .await?;
    }
}

#[derive(Debug, Default)]
struct Negotiation {
    tls: bool,
    gssapi: bool,
}

impl Negotiation {
    fn first_time(&mut self, encryption: Encryption) -> bool {
        let asked = match encryption {
            Encryption::Tls => &mut self.tls,
            Encryption::GssApi => &mut self.gssapi,
        };
        !std::mem::replace(asked, true)
    }
}

async fn read_startup<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
) -> Result<StartupRequest, AcceptError> {
    loop {
        if let Some(body) =
            decode_startup_frame(buf, MAX_STARTUP_PACKET).map_err(AcceptError::Frame)?
        {
            return decode_startup(&body).map_err(AcceptError::Startup);
        }
        buf.reserve(READ_CHUNK);
        if stream.read_buf(buf).await? == 0 {
            return Err(AcceptError::ConnectionClosed);
        }
    }
}

async fn refuse<S: AsyncWrite + Unpin>(stream: &mut S, error: AcceptError) -> AcceptError {
    if let Some(response) = error.response() {
        let mut out = BytesMut::new();
        encode_error_response(&response, &mut out);
        if let Err(source) = stream.write_all(&out).await {
            tracing::debug!(%source, "could not tell the client why it was refused");
            return error;
        }
        let _ = stream.flush().await;
    }
    error
}

async fn write<S: AsyncWrite + Unpin>(
    stream: &mut S,
    encode: impl FnOnce(&mut BytesMut),
) -> io::Result<()> {
    let mut out = BytesMut::new();
    encode(&mut out);
    stream.write_all(&out).await?;
    stream.flush().await
}
