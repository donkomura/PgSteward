use std::collections::BTreeMap;
use std::fmt;
use std::io;

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use pgsteward_protocol::startup::{
    CancelKey, ProtocolVersion, StartupMessage, StartupRequest, encode_startup,
};
use postgres_protocol::authentication::md5_hash;
use postgres_protocol::authentication::sasl::{ChannelBinding, SCRAM_SHA_256, ScramSha256};
use postgres_protocol::message::backend::{AuthenticationSaslBody, ErrorFields, Message};
use postgres_protocol::message::frontend;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::rt::Net;

const READ_CHUNK: usize = 8 * 1024;
const SQLSTATE_FIELD: u8 = b'C';
const MESSAGE_FIELD: u8 = b'M';
const IDLE: u8 = b'I';

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ApplicationName(String);

impl ApplicationName {
    pub const PREFIX: &'static str = "pgsteward-";

    #[must_use]
    pub fn new(identifier: &str) -> Self {
        Self(format!("{}{identifier}", Self::PREFIX))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ApplicationName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ServerCredentials {
    pub user: String,
    pub database: String,
    pub password: Option<String>,
}

impl fmt::Debug for ServerCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerCredentials")
            .field("user", &self.user)
            .field("database", &self.database)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HandshakeError {
    #[error("i/o error during startup: {0}")]
    Io(#[from] io::Error),
    #[error("server refused the connection ({code}): {message}")]
    Server { code: String, message: String },
    #[error(
        "server requested {0} authentication, which is not supported; use SCRAM-SHA-256 or MD5"
    )]
    UnsupportedAuthentication(&'static str),
    #[error("server requested {0} authentication but no password is configured for this instance")]
    PasswordRequired(&'static str),
    #[error("SCRAM-SHA-256 exchange failed: {0}")]
    Scram(io::Error),
    #[error("protocol violation during startup: {0}")]
    UnexpectedMessage(&'static str),
    #[error("server closed the connection during startup")]
    ConnectionClosed,
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    #[error("cannot reach the server: {0}")]
    Unreachable(io::Error),
    #[error(transparent)]
    Handshake(#[from] HandshakeError),
}

#[derive(Debug)]
pub struct ServerConnection<S> {
    stream: S,
    parameters: BTreeMap<String, String>,
    backend_key: CancelKey,
}

pub async fn connect<N: Net>(
    net: &N,
    addr: &str,
    credentials: &ServerCredentials,
    application_name: &ApplicationName,
) -> Result<ServerConnection<N::Stream>, ConnectError> {
    let stream = net.connect(addr).await.map_err(|source| {
        ConnectError::Unreachable(io::Error::new(source.kind(), format!("{addr}: {source}")))
    })?;
    Ok(ServerConnection::handshake(stream, credentials, application_name).await?)
}

impl<S: AsyncRead + AsyncWrite + Unpin> ServerConnection<S> {
    pub async fn handshake(
        mut stream: S,
        credentials: &ServerCredentials,
        application_name: &ApplicationName,
    ) -> Result<Self, HandshakeError> {
        let mut out = BytesMut::new();
        encode_startup(&startup_request(credentials, application_name), &mut out);
        stream.write_all(&out).await?;
        stream.flush().await?;

        let mut read_buf = BytesMut::with_capacity(READ_CHUNK);
        let mut startup = StartupState::default();
        loop {
            match read_message(&mut stream, &mut read_buf).await? {
                Message::ReadyForQuery(body) => {
                    let backend_key = startup.ready(body.status())?;
                    return Ok(Self {
                        stream,
                        parameters: startup.parameters,
                        backend_key,
                    });
                }
                Message::ErrorResponse(body) => {
                    return Err(HandshakeError::Server {
                        code: error_field(body.fields(), SQLSTATE_FIELD),
                        message: error_field(body.fields(), MESSAGE_FIELD),
                    });
                }
                Message::NoticeResponse(body) => {
                    tracing::debug!(
                        notice = %error_field(body.fields(), MESSAGE_FIELD),
                        "notice during startup"
                    );
                }
                Message::ParameterStatus(body) => {
                    startup
                        .parameters
                        .insert(body.name()?.to_owned(), body.value()?.to_owned());
                }
                Message::BackendKeyData(body) => {
                    startup.backend_key = Some(CancelKey {
                        process_id: body.process_id(),
                        secret_key: body.secret_key(),
                    });
                }
                Message::AuthenticationOk => startup.authentication_ok()?,
                other => {
                    answer_authentication(&mut stream, credentials, &mut startup, other).await?;
                }
            }
        }
    }

    #[must_use]
    pub fn parameters(&self) -> &BTreeMap<String, String> {
        &self.parameters
    }

    #[must_use]
    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.parameters.get(name).map(String::as_str)
    }

    #[must_use]
    pub fn backend_key(&self) -> CancelKey {
        self.backend_key
    }

    pub async fn terminate(mut self) -> io::Result<()> {
        send(&mut self.stream, |buf| {
            frontend::terminate(buf);
            Ok(())
        })
        .await?;
        self.stream.shutdown().await
    }
}

#[derive(Default)]
enum ScramState {
    #[default]
    Absent,
    InProgress(Box<ScramSha256>),
    Proved,
}

#[derive(Default)]
struct StartupState {
    parameters: BTreeMap<String, String>,
    backend_key: Option<CancelKey>,
    authenticated: bool,
    scram: ScramState,
}

impl StartupState {
    fn authentication_ok(&mut self) -> Result<(), HandshakeError> {
        if matches!(self.scram, ScramState::InProgress(_)) {
            return Err(HandshakeError::UnexpectedMessage(
                "AuthenticationOk before the server proved itself with a SASL final message",
            ));
        }
        self.authenticated = true;
        Ok(())
    }

    fn scram_in_progress(&mut self) -> Result<&mut ScramSha256, HandshakeError> {
        match &mut self.scram {
            ScramState::InProgress(scram) => Ok(scram),
            _ => Err(HandshakeError::UnexpectedMessage(
                "SASL continuation without a SASL exchange",
            )),
        }
    }

    fn ready(&self, status: u8) -> Result<CancelKey, HandshakeError> {
        if !self.authenticated {
            return Err(HandshakeError::UnexpectedMessage(
                "ReadyForQuery before AuthenticationOk",
            ));
        }
        let Some(backend_key) = self.backend_key else {
            return Err(HandshakeError::UnexpectedMessage(
                "ReadyForQuery before BackendKeyData",
            ));
        };
        if status != IDLE {
            return Err(HandshakeError::UnexpectedMessage(
                "ReadyForQuery with a non-idle transaction status at startup",
            ));
        }
        Ok(backend_key)
    }
}

async fn answer_authentication<S: AsyncWrite + Unpin>(
    stream: &mut S,
    credentials: &ServerCredentials,
    startup: &mut StartupState,
    message: Message,
) -> Result<(), HandshakeError> {
    match message {
        Message::AuthenticationSasl(body) => {
            if !offers_scram_sha_256(&body)? {
                return Err(HandshakeError::UnsupportedAuthentication(
                    "SASL without the SCRAM-SHA-256 mechanism",
                ));
            }
            let password = credentials
                .password
                .as_deref()
                .ok_or(HandshakeError::PasswordRequired(SCRAM_SHA_256))?;
            let scram = ScramSha256::new(password.as_bytes(), ChannelBinding::unsupported());
            send(stream, |buf| {
                frontend::sasl_initial_response(SCRAM_SHA_256, scram.message(), buf)
            })
            .await?;
            startup.scram = ScramState::InProgress(Box::new(scram));
            Ok(())
        }
        Message::AuthenticationSaslContinue(body) => {
            let scram = startup.scram_in_progress()?;
            scram.update(body.data()).map_err(HandshakeError::Scram)?;
            send(stream, |buf| frontend::sasl_response(scram.message(), buf)).await?;
            Ok(())
        }
        Message::AuthenticationSaslFinal(body) => {
            startup
                .scram_in_progress()?
                .finish(body.data())
                .map_err(HandshakeError::Scram)?;
            startup.scram = ScramState::Proved;
            Ok(())
        }
        Message::AuthenticationMd5Password(body) => {
            let password = credentials
                .password
                .as_deref()
                .ok_or(HandshakeError::PasswordRequired("MD5"))?;
            let hash = md5_hash(
                credentials.user.as_bytes(),
                password.as_bytes(),
                body.salt(),
            );
            send(stream, |buf| {
                frontend::password_message(hash.as_bytes(), buf)
            })
            .await?;
            Ok(())
        }
        Message::AuthenticationCleartextPassword => Err(HandshakeError::UnsupportedAuthentication(
            "cleartext password",
        )),
        Message::AuthenticationGss
        | Message::AuthenticationGssContinue(_)
        | Message::AuthenticationKerberosV5
        | Message::AuthenticationSspi => {
            Err(HandshakeError::UnsupportedAuthentication("GSSAPI/Kerberos"))
        }
        Message::AuthenticationScmCredential => {
            Err(HandshakeError::UnsupportedAuthentication("SCM credential"))
        }
        _ => Err(HandshakeError::UnexpectedMessage(
            "message other than authentication, ParameterStatus, BackendKeyData, ReadyForQuery or ErrorResponse before ReadyForQuery",
        )),
    }
}

fn offers_scram_sha_256(body: &AuthenticationSaslBody) -> Result<bool, HandshakeError> {
    let mut mechanisms = body.mechanisms();
    while let Some(mechanism) = mechanisms.next()? {
        if mechanism == SCRAM_SHA_256 {
            return Ok(true);
        }
    }
    Ok(false)
}

fn startup_request(
    credentials: &ServerCredentials,
    application_name: &ApplicationName,
) -> StartupRequest {
    StartupRequest::Startup(StartupMessage::new(
        ProtocolVersion::V3_0,
        vec![
            ("user".to_owned(), credentials.user.clone()),
            ("database".to_owned(), credentials.database.clone()),
            (
                "application_name".to_owned(),
                application_name.as_str().to_owned(),
            ),
        ],
    ))
}

async fn read_message<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
) -> Result<Message, HandshakeError> {
    loop {
        if let Some(message) = Message::parse(buf)? {
            return Ok(message);
        }
        buf.reserve(READ_CHUNK);
        if stream.read_buf(buf).await? == 0 {
            return Err(HandshakeError::ConnectionClosed);
        }
    }
}

async fn send<S: AsyncWrite + Unpin>(
    stream: &mut S,
    encode: impl FnOnce(&mut BytesMut) -> io::Result<()>,
) -> io::Result<()> {
    let mut out = BytesMut::new();
    encode(&mut out)?;
    stream.write_all(&out).await?;
    stream.flush().await
}

fn error_field(mut fields: ErrorFields<'_>, wanted: u8) -> String {
    while let Ok(Some(field)) = fields.next() {
        if field.type_() == wanted {
            return String::from_utf8_lossy(field.value_bytes()).into_owned();
        }
    }
    String::new()
}
