use std::collections::BTreeMap;
use std::fmt;
use std::io;

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use pgsteward_protocol::startup::{
    CancelKey, ProtocolVersion, StartupMessage, StartupRequest, encode_startup,
};
use postgres_protocol::authentication::md5_hash;
use postgres_protocol::message::backend::{ErrorFields, Message};
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
                Message::AuthenticationOk => startup.authenticated = true,
                other => answer_authentication(&mut stream, credentials, other).await?,
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
struct StartupState {
    parameters: BTreeMap<String, String>,
    backend_key: Option<CancelKey>,
    authenticated: bool,
}

impl StartupState {
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
    message: Message,
) -> Result<(), HandshakeError> {
    match message {
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
        Message::AuthenticationSasl(_) => Err(HandshakeError::UnsupportedAuthentication("SASL")),
        Message::AuthenticationGss
        | Message::AuthenticationGssContinue(_)
        | Message::AuthenticationKerberosV5
        | Message::AuthenticationSspi => {
            Err(HandshakeError::UnsupportedAuthentication("GSSAPI/Kerberos"))
        }
        Message::AuthenticationScmCredential => {
            Err(HandshakeError::UnsupportedAuthentication("SCM credential"))
        }
        Message::AuthenticationSaslContinue(_) | Message::AuthenticationSaslFinal(_) => Err(
            HandshakeError::UnexpectedMessage("SASL continuation without a SASL exchange"),
        ),
        _ => Err(HandshakeError::UnexpectedMessage(
            "message other than authentication, ParameterStatus, BackendKeyData, ReadyForQuery or ErrorResponse before ReadyForQuery",
        )),
    }
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
