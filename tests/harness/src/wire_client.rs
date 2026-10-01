use std::io;

use bytes::{BufMut, BytesMut};
use fallible_iterator::FallibleIterator;
use pgsteward_protocol::framing::{Frame, decode_frame, encode_frame};
use pgsteward_protocol::startup::{
    CancelKey, ProtocolVersion, StartupMessage, StartupRequest, encode_startup,
};
use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const MAX_FRAME: usize = 1 << 20;

/// A client application speaking the wire protocol over any stream, for tests
/// that drive a proxy or a node the way an application would.
#[derive(Debug)]
pub struct WireClient<S> {
    stream: S,
    buf: BytesMut,
    parameters: Vec<(String, String)>,
    cancel_key: Option<CancelKey>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> WireClient<S> {
    /// Logs in and reads the greeting up to the first `ReadyForQuery`. Without a
    /// password the server must let the client in as it is.
    pub async fn login(
        stream: S,
        user: &str,
        database: &str,
        password: Option<&str>,
    ) -> io::Result<Self> {
        let mut client = Self {
            stream,
            buf: BytesMut::new(),
            parameters: Vec::new(),
            cancel_key: None,
        };
        let mut out = BytesMut::new();
        encode_startup(
            &StartupRequest::Startup(StartupMessage::new(
                ProtocolVersion::V3_0,
                vec![
                    ("user".to_owned(), user.to_owned()),
                    ("database".to_owned(), database.to_owned()),
                ],
            )),
            &mut out,
        );
        client.write(&out).await?;
        match password {
            Some(password) => client.prove(password).await?,
            None => assert!(matches!(
                client.read_message().await?,
                Message::AuthenticationOk
            )),
        }
        client.read_greeting().await?;
        Ok(client)
    }

    pub fn parameters(&self) -> &[(String, String)] {
        &self.parameters
    }

    pub fn cancel_key(&self) -> Option<CancelKey> {
        self.cancel_key
    }

    async fn prove(&mut self, password: &str) -> io::Result<()> {
        let mut scram = ScramSha256::new(password.as_bytes(), ChannelBinding::unsupported());
        assert!(matches!(
            self.read_message().await?,
            Message::AuthenticationSasl(_)
        ));
        let mut out = BytesMut::new();
        frontend::sasl_initial_response("SCRAM-SHA-256", scram.message(), &mut out)?;
        self.write(&out).await?;

        let Message::AuthenticationSaslContinue(body) = self.read_message().await? else {
            panic!("expected an AuthenticationSASLContinue");
        };
        scram.update(body.data())?;
        let mut out = BytesMut::new();
        frontend::sasl_response(scram.message(), &mut out)?;
        self.write(&out).await?;

        let Message::AuthenticationSaslFinal(body) = self.read_message().await? else {
            panic!("expected an AuthenticationSASLFinal");
        };
        scram.finish(body.data())?;
        assert!(matches!(
            self.read_message().await?,
            Message::AuthenticationOk
        ));
        Ok(())
    }

    async fn read_greeting(&mut self) -> io::Result<()> {
        loop {
            match self.read_message().await? {
                Message::ParameterStatus(body) => self
                    .parameters
                    .push((body.name()?.to_owned(), body.value()?.to_owned())),
                Message::BackendKeyData(body) => {
                    self.cancel_key = Some(CancelKey {
                        process_id: body.process_id(),
                        secret_key: body.secret_key(),
                    });
                }
                Message::ReadyForQuery(body) => {
                    assert_eq!(body.status(), b'I');
                    return Ok(());
                }
                _ => panic!("unexpected message in the greeting"),
            }
        }
    }

    async fn write(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.stream.write_all(bytes).await?;
        self.stream.flush().await
    }

    pub async fn send_query(&mut self, sql: &str) -> io::Result<()> {
        let mut body = BytesMut::new();
        body.put_slice(sql.as_bytes());
        body.put_u8(0);
        let mut out = BytesMut::new();
        encode_frame(b'Q', &body, &mut out);
        self.write(&out).await
    }

    pub async fn read_frame(&mut self) -> io::Result<Frame> {
        loop {
            if let Some(frame) = decode_frame(&mut self.buf, MAX_FRAME).map_err(io::Error::other)? {
                return Ok(frame);
            }
            if self.stream.read_buf(&mut self.buf).await? == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
        }
    }

    async fn read_message(&mut self) -> io::Result<Message> {
        let frame = self.read_frame().await?;
        let mut bytes = BytesMut::new();
        encode_frame(frame.tag, &frame.body, &mut bytes);
        Ok(Message::parse(&mut bytes)?.expect("a whole message"))
    }

    /// Reads one query's result up to `ReadyForQuery`: the values of every row in
    /// order, and the transaction status.
    pub async fn read_result(&mut self) -> io::Result<(Vec<String>, u8)> {
        let mut values = Vec::new();
        loop {
            match self.read_message().await? {
                Message::DataRow(body) => {
                    let mut ranges = body.ranges();
                    while let Some(range) = ranges.next()? {
                        let range = range.expect("a non-null value");
                        values.push(
                            String::from_utf8(body.buffer()[range].to_vec())
                                .map_err(io::Error::other)?,
                        );
                    }
                }
                Message::ReadyForQuery(body) => return Ok((values, body.status())),
                Message::RowDescription(_) | Message::CommandComplete(_) => {}
                Message::ErrorResponse(_) => panic!("the server refused the query"),
                _ => panic!("unexpected message in a query result"),
            }
        }
    }

    pub async fn query(&mut self, sql: &str) -> io::Result<(Vec<String>, u8)> {
        self.send_query(sql).await?;
        self.read_result().await
    }
}
