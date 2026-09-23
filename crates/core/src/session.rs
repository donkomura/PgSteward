use std::fmt;
use std::io;

use bytes::{Bytes, BytesMut};
use pgsteward_protocol::backend::{
    EncryptionResponse, ErrorResponse, encode_authentication_ok, encode_authentication_sasl,
    encode_authentication_sasl_continue, encode_authentication_sasl_final,
    encode_encryption_response, encode_error_response, sqlstate,
};
use pgsteward_protocol::framing::{Frame, FrameError, decode_frame, decode_startup_frame};
use pgsteward_protocol::frontend::{FrontendError, decode_sasl_initial_response};
use pgsteward_protocol::message::FrontendTag;
use pgsteward_protocol::startup::{
    CancelKey, SUPPORTED_MAJOR, StartupError, StartupMessage, StartupRequest, decode_startup,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::auth::{AuthMethod, Credentials};
use crate::scram::{MECHANISM, ScramError, ScramExchange, ScramVerifier, nonce};
use crate::tenant::{TenantId, TenantResolveError};
use crate::tls::{ClientTls, MaybeTls};

const MAX_STARTUP_PACKET: usize = 10_000;
const MAX_AUTH_MESSAGE: usize = 10_000;
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
    #[error("the client sent unencrypted data after this node answered its TLS request")]
    UnencryptedData,
    #[error("malformed SASL message: {0}")]
    Frontend(#[from] FrontendError),
    #[error("the client asked for the {0:?} SASL mechanism; this node offers {MECHANISM}")]
    UnsupportedMechanism(String),
    #[error("the client sent {:?} where a SASL response was expected", *.0 as char)]
    UnexpectedMessage(u8),
    #[error("{MECHANISM} exchange failed: {0}")]
    Scram(ScramError),
    #[error("password authentication failed for user {0:?}")]
    AuthenticationFailed(String),
    #[error("this node already holds the {max} client connections it accepts")]
    TooManyClients { max: usize },
}

impl AcceptError {
    fn response(&self) -> Option<ErrorResponse> {
        match self {
            Self::Io(_) | Self::ConnectionClosed | Self::UnencryptedData => None,
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
            Self::UnsupportedMechanism(_) | Self::Scram(ScramError::ChannelBinding) => Some(
                ErrorResponse::fatal(sqlstate::FEATURE_NOT_SUPPORTED, self.to_string()),
            ),
            Self::TooManyClients { .. } => Some(
                ErrorResponse::fatal(sqlstate::TOO_MANY_CONNECTIONS, self.to_string()).with_hint(
                    "Raise node.max_client_connections, or spread the clients over more proxy nodes.",
                ),
            ),
            Self::AuthenticationFailed(user) => Some(ErrorResponse::fatal(
                sqlstate::INVALID_PASSWORD,
                format!("password authentication failed for user {user:?}"),
            )),
            Self::Frame(_)
            | Self::RepeatedEncryptionRequest(_)
            | Self::Startup(_)
            | Self::Frontend(_)
            | Self::UnexpectedMessage(_)
            | Self::Scram(_) => Some(ErrorResponse::fatal(
                sqlstate::PROTOCOL_VIOLATION,
                self.to_string(),
            )),
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

pub async fn accept<S: AsyncRead + AsyncWrite + Unpin, C: Credentials>(
    stream: S,
    credentials: C,
    tls: Option<&ClientTls>,
) -> Result<Accepted<MaybeTls<S>>, AcceptError> {
    let mut stream = MaybeTls::Plain(stream);
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
                let method = credentials.method(&tenant);
                if let Err(error) = authenticate(&mut stream, &mut pending, method, &tenant).await {
                    return Err(refuse(&mut stream, error).await);
                }
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
        match tls.filter(|_| encryption == Encryption::Tls) {
            Some(tls) => {
                write(&mut stream, |out| {
                    encode_encryption_response(EncryptionResponse::Accepted, out);
                })
                .await?;
                if !pending.is_empty() {
                    return Err(AcceptError::UnencryptedData);
                }
                stream = stream.encrypt(tls).await?;
            }
            None => {
                write(&mut stream, |out| {
                    encode_encryption_response(EncryptionResponse::Refused, out);
                })
                .await?;
            }
        }
    }
}

async fn authenticate<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    pending: &mut BytesMut,
    method: Option<AuthMethod>,
    tenant: &TenantId,
) -> Result<(), AcceptError> {
    let verifier = match method {
        Some(AuthMethod::Trust) => return Ok(()),
        Some(AuthMethod::ScramSha256(verifier)) => verifier,
        None => ScramVerifier::mock(),
    };
    match scram(stream, pending, verifier).await {
        Err(AcceptError::Scram(ScramError::Proof)) => {
            Err(AcceptError::AuthenticationFailed(tenant.user().to_owned()))
        }
        outcome => outcome,
    }
}

async fn scram<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    pending: &mut BytesMut,
    verifier: ScramVerifier,
) -> Result<(), AcceptError> {
    let mut exchange = ScramExchange::new(verifier, nonce());
    write(stream, |out| {
        encode_authentication_sasl(&[MECHANISM], out);
    })
    .await?;

    let initial = read_sasl_response(stream, pending).await?;
    let initial = decode_sasl_initial_response(&initial)?;
    if initial.mechanism != MECHANISM {
        return Err(AcceptError::UnsupportedMechanism(
            initial.mechanism.to_owned(),
        ));
    }
    let server_first = exchange
        .server_first(initial.data)
        .map_err(AcceptError::Scram)?;
    write(stream, |out| {
        encode_authentication_sasl_continue(&server_first, out);
    })
    .await?;

    let client_final = read_sasl_response(stream, pending).await?;
    let server_final = exchange
        .server_final(&client_final)
        .map_err(AcceptError::Scram)?;
    write(stream, |out| {
        encode_authentication_sasl_final(&server_final, out);
    })
    .await
    .map_err(AcceptError::from)
}

async fn read_sasl_response<S: AsyncRead + Unpin>(
    stream: &mut S,
    pending: &mut BytesMut,
) -> Result<Bytes, AcceptError> {
    let frame = read_frame(stream, pending).await?;
    if FrontendTag::try_from(frame.tag) != Ok(FrontendTag::Password) {
        return Err(AcceptError::UnexpectedMessage(frame.tag));
    }
    Ok(frame.body)
}

async fn read_frame<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
) -> Result<Frame, AcceptError> {
    loop {
        if let Some(frame) = decode_frame(buf, MAX_AUTH_MESSAGE).map_err(AcceptError::Frame)? {
            return Ok(frame);
        }
        buf.reserve(READ_CHUNK);
        if stream.read_buf(buf).await? == 0 {
            return Err(AcceptError::ConnectionClosed);
        }
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

/// Tells a client the node is full and closes.
///
/// The packet the client has already sent is read first. RFC 9293 3.10.4 has a
/// close with unread data send a reset, and the reset takes the refusal with
/// it, so the client would never learn why it was turned away.
pub(crate) async fn refuse_over_limit<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    max: usize,
) -> AcceptError {
    let mut pending = BytesMut::with_capacity(READ_CHUNK);
    let _ = read_startup(stream, &mut pending).await;
    refuse(stream, AcceptError::TooManyClients { max }).await
}

pub(crate) async fn refuse<S: AsyncWrite + Unpin>(
    stream: &mut S,
    error: AcceptError,
) -> AcceptError {
    if let Some(response) = error.response() {
        let mut out = BytesMut::new();
        encode_error_response(&response, &mut out);
        if let Err(source) = stream.write_all(&out).await {
            tracing::debug!(%source, "could not tell the client why it was refused");
            return error;
        }
        let _ = stream.flush().await;
        let _ = stream.shutdown().await;
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
