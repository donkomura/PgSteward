use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::io;

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use pgsteward_protocol::framing::{Frame, MAX_MESSAGE, decode_frame};
use pgsteward_protocol::startup::{
    CancelKey, ProtocolVersion, StartupMessage, StartupRequest, encode_startup,
};
use postgres_protocol::authentication::md5_hash;
use postgres_protocol::authentication::sasl::{ChannelBinding, SCRAM_SHA_256, ScramSha256};
use postgres_protocol::message::backend::{
    AuthenticationSaslBody, DataRowBody, ErrorFields, Message,
};
use postgres_protocol::message::frontend;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::pool::CloseServer;
use crate::rt::Net;
use crate::tls::{MaybeTls, ServerTls, SslMode};

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
pub enum QueryError {
    #[error("i/o error while running a query: {0}")]
    Io(#[from] io::Error),
    #[error("server refused the query ({code}): {message}")]
    Server { code: String, message: String },
    #[error("protocol violation while running a query: {0}")]
    UnexpectedMessage(&'static str),
    #[error("server closed the connection while running a query")]
    ConnectionClosed,
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    #[error("cannot reach the server: {0}")]
    Unreachable(io::Error),
    #[error(transparent)]
    Handshake(#[from] HandshakeError),
    #[error("the server does not encrypt, and this node is configured to demand it")]
    EncryptionRefused,
    #[error("the server answered the encryption request with `{}`, which is neither S nor N", *.0 as char)]
    EncryptionAnswer(u8),
    #[error("cannot encrypt the connection to the server: {0}")]
    Encryption(io::Error),
}

pub type Row = Vec<Option<String>>;

pub trait SimpleQuery {
    fn simple_query(
        &mut self,
        sql: &str,
    ) -> impl Future<Output = Result<Vec<Row>, QueryError>> + Send;
}

#[derive(Debug)]
pub struct ServerConnection<S> {
    stream: S,
    read_buf: BytesMut,
    parameters: BTreeMap<String, String>,
    backend_key: CancelKey,
}

pub async fn connect<N: Net>(
    net: &N,
    addr: &str,
    credentials: &ServerCredentials,
    application_name: &ApplicationName,
    tls: &ServerTls,
) -> Result<ServerConnection<MaybeTls<N::Stream>>, ConnectError> {
    let stream = net.connect(addr).await.map_err(|source| {
        ConnectError::Unreachable(io::Error::new(source.kind(), format!("{addr}: {source}")))
    })?;
    let stream = request_encryption(stream, tls, host_of(addr)).await?;
    Ok(ServerConnection::handshake(stream, credentials, application_name).await?)
}

/// The `SSLRequest` exchange, which comes before the startup packet: one packet
/// out and one byte back, `S` to encrypt and `N` to carry on in the clear.
pub async fn request_encryption<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    tls: &ServerTls,
    host: &str,
) -> Result<MaybeTls<S>, ConnectError> {
    if tls.mode() == SslMode::Disable {
        return Ok(MaybeTls::Plain(stream));
    }
    let mut out = BytesMut::new();
    encode_startup(&StartupRequest::Ssl, &mut out);
    write_all(&mut stream, &out).await?;

    let mut answer = [0u8; 1];
    stream
        .read_exact(&mut answer)
        .await
        .map_err(HandshakeError::Io)?;
    match answer[0] {
        b'S' => tls
            .encrypt(stream, host)
            .await
            .map_err(ConnectError::Encryption),
        b'N' if tls.mode().demands_encryption() => Err(ConnectError::EncryptionRefused),
        b'N' => Ok(MaybeTls::Plain(stream)),
        other => Err(ConnectError::EncryptionAnswer(other)),
    }
}

async fn write_all<S: AsyncWrite + Unpin>(
    stream: &mut S,
    out: &[u8],
) -> Result<(), HandshakeError> {
    stream.write_all(out).await?;
    stream.flush().await?;
    Ok(())
}

/// The host of `host:port`, which is the name a certificate is checked against.
fn host_of(addr: &str) -> &str {
    if let Some(host) = addr.strip_prefix('[') {
        return host.split_once(']').map_or(addr, |(host, _)| host);
    }
    match addr.rsplit_once(':') {
        Some((host, port)) if port.parse::<u16>().is_ok() => host,
        _ => addr,
    }
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
                        read_buf,
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

    #[must_use]
    pub fn into_parts(self) -> (S, BytesMut) {
        (self.stream, self.read_buf)
    }

    pub async fn read_frame(&mut self) -> io::Result<Option<Frame>> {
        loop {
            if let Some(frame) = decode_frame(&mut self.read_buf, MAX_MESSAGE)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
            {
                return Ok(Some(frame));
            }
            self.read_buf.reserve(READ_CHUNK);
            if self.stream.read_buf(&mut self.read_buf).await? == 0 {
                return Ok(None);
            }
        }
    }

    pub async fn forward(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.stream.write_all(bytes).await?;
        self.stream.flush().await
    }

    async fn close_and_drain(mut self) {
        let _ = send(&mut self.stream, |buf| {
            frontend::terminate(buf);
            Ok(())
        })
        .await;
        let _ = self.stream.shutdown().await;
        let mut sink = [0u8; READ_CHUNK];
        while matches!(self.stream.read(&mut sink).await, Ok(read) if read > 0) {}
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

impl<S: AsyncRead + AsyncWrite + Unpin + Send> CloseServer for ServerConnection<S> {
    async fn close(self) {
        self.close_and_drain().await;
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> SimpleQuery for ServerConnection<S> {
    async fn simple_query(&mut self, sql: &str) -> Result<Vec<Row>, QueryError> {
        send(&mut self.stream, |buf| frontend::query(sql, buf)).await?;

        let mut rows = Vec::new();
        let mut refusal = None;
        loop {
            match read_message(&mut self.stream, &mut self.read_buf).await? {
                Message::ReadyForQuery(_) => {
                    return refusal.map_or(Ok(rows), Err);
                }
                Message::DataRow(body) => rows.push(row_values(&body)?),
                Message::RowDescription(_)
                | Message::CommandComplete(_)
                | Message::EmptyQueryResponse => {}
                Message::ErrorResponse(body) => {
                    refusal = Some(QueryError::Server {
                        code: error_field(body.fields(), SQLSTATE_FIELD),
                        message: error_field(body.fields(), MESSAGE_FIELD),
                    });
                }
                Message::NoticeResponse(body) => {
                    tracing::debug!(
                        notice = %error_field(body.fields(), MESSAGE_FIELD),
                        "notice while running a query"
                    );
                }
                Message::ParameterStatus(body) => {
                    self.parameters
                        .insert(body.name()?.to_owned(), body.value()?.to_owned());
                }
                _ => {
                    return Err(QueryError::UnexpectedMessage(
                        "message other than a row, a completion, ParameterStatus, NoticeResponse, ErrorResponse or ReadyForQuery in a simple query result",
                    ));
                }
            }
        }
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
) -> Result<Message, ReadError> {
    loop {
        if let Some(message) = Message::parse(buf).map_err(ReadError::Io)? {
            return Ok(message);
        }
        buf.reserve(READ_CHUNK);
        if stream.read_buf(buf).await.map_err(ReadError::Io)? == 0 {
            return Err(ReadError::Closed);
        }
    }
}

enum ReadError {
    Io(io::Error),
    Closed,
}

impl From<ReadError> for HandshakeError {
    fn from(error: ReadError) -> Self {
        match error {
            ReadError::Io(source) => Self::Io(source),
            ReadError::Closed => Self::ConnectionClosed,
        }
    }
}

impl From<ReadError> for QueryError {
    fn from(error: ReadError) -> Self {
        match error {
            ReadError::Io(source) => Self::Io(source),
            ReadError::Closed => Self::ConnectionClosed,
        }
    }
}

fn row_values(body: &DataRowBody) -> Result<Row, QueryError> {
    let buffer = body.buffer();
    let mut ranges = body.ranges();
    let mut values = Vec::new();
    while let Some(range) = ranges.next()? {
        values.push(range.map(|range| String::from_utf8_lossy(&buffer[range]).into_owned()));
    }
    Ok(values)
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
