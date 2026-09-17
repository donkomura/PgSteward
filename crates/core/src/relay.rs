use std::io;

use std::sync::{Arc, OnceLock};

use bytes::BytesMut;
use pgsteward_protocol::backend::{
    ErrorResponse, encode_backend_key_data, encode_error_response, encode_parameter_status,
    encode_ready_for_query, sqlstate,
};
use pgsteward_protocol::framing::{Frame, FrameError, MAX_MESSAGE, decode_frame, encode_frame};
use pgsteward_protocol::message::{BackendTag, FrontendTag, MessageError, TransactionStatus};
use pgsteward_protocol::ready::{ReadyTracker, TrackerError};
use pgsteward_protocol::startup::CancelKey;
use rand::RngExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, copy_bidirectional};

use crate::pool::{OpenServer, Pool, PoolError};
use crate::rt::Clock;
use crate::server::ServerConnection;
use crate::session::ClientSession;

const READ_CHUNK: usize = 8 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum RelayError {
    #[error("i/o error while relaying a session: {0}")]
    Io(#[from] io::Error),
    #[error("malformed message: {0}")]
    Frame(#[from] FrameError),
    #[error(transparent)]
    Message(#[from] MessageError),
    #[error(transparent)]
    Tracker(#[from] TrackerError),
    #[error("the server closed the connection while a request was in flight")]
    ServerClosed,
    #[error(transparent)]
    Pool(#[from] PoolError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Boundary {
    Released,
    ClientClosed { may_release: bool },
}

pub async fn session_mode<C, S>(
    client: ClientSession<C>,
    server: ServerConnection<S>,
) -> Result<(), RelayError>
where
    C: AsyncRead + AsyncWrite + Unpin,
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut greeting = BytesMut::new();
    for (name, value) in server.parameters() {
        encode_parameter_status(name, value, &mut greeting);
    }
    encode_backend_key_data(server.backend_key(), &mut greeting);
    encode_ready_for_query(TransactionStatus::Idle, &mut greeting);

    let (mut client_stream, from_client) = client.into_parts();
    let (mut server_stream, from_server) = server.into_parts();

    greeting.extend_from_slice(&from_server);
    client_stream.write_all(&greeting).await?;
    client_stream.flush().await?;
    if !from_client.is_empty() {
        server_stream.write_all(&from_client).await?;
        server_stream.flush().await?;
    }

    copy_bidirectional(&mut client_stream, &mut server_stream).await?;
    Ok(())
}

pub async fn serve_assignment<C, S>(
    client: &mut C,
    pending: &mut BytesMut,
    server: &mut ServerConnection<S>,
) -> Result<Boundary, RelayError>
where
    C: AsyncRead + AsyncWrite + Unpin,
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut tracker = ReadyTracker::new();
    loop {
        if forward_requests(pending, server, &mut tracker).await? == Requests::Terminated {
            return Ok(Boundary::ClientClosed {
                may_release: tracker.may_release(),
            });
        }
        let event = tokio::select! {
            read = fill(client, pending) => Event::FromClient(read?),
            frame = server.read_frame() => Event::FromServer(frame?),
        };
        match event {
            Event::FromClient(0) => {
                return Ok(Boundary::ClientClosed {
                    may_release: tracker.may_release(),
                });
            }
            Event::FromClient(_) => {}
            Event::FromServer(None) => return Err(RelayError::ServerClosed),
            Event::FromServer(Some(frame)) => {
                let ready = match BackendTag::try_from(frame.tag) {
                    Ok(tag) => tracker.on_backend(tag, &frame.body)?.is_some(),
                    Err(_) => false,
                };
                let mut out = BytesMut::new();
                encode_frame(frame.tag, &frame.body, &mut out);
                client.write_all(&out).await?;
                client.flush().await?;
                if ready && tracker.may_release() {
                    return Ok(Boundary::Released);
                }
            }
        }
    }
}

enum Event {
    FromClient(usize),
    FromServer(Option<Frame>),
}

#[derive(PartialEq, Eq)]
enum Requests {
    Drained,
    Terminated,
}

async fn forward_requests<S: AsyncRead + AsyncWrite + Unpin>(
    pending: &mut BytesMut,
    server: &mut ServerConnection<S>,
    tracker: &mut ReadyTracker,
) -> Result<Requests, RelayError> {
    let mut out = BytesMut::new();
    let mut requests = Requests::Drained;
    while let Some(frame) = decode_frame(pending, MAX_MESSAGE)? {
        let tag = FrontendTag::try_from(frame.tag)?;
        if tag == FrontendTag::Terminate {
            requests = Requests::Terminated;
            break;
        }
        tracker.on_frontend(tag);
        encode_frame(frame.tag, &frame.body, &mut out);
    }
    if !out.is_empty() {
        server.forward(&out).await?;
    }
    Ok(requests)
}

async fn fill<C: AsyncRead + Unpin>(client: &mut C, pending: &mut BytesMut) -> io::Result<usize> {
    pending.reserve(READ_CHUNK);
    client.read_buf(pending).await
}

/// The `ParameterStatus` set every client is greeted with, taken from the first
/// server connection this node opens.
///
/// A client waits for `ReadyForQuery` before it sends anything, so the greeting
/// cannot wait for the client's first message the way an assignment does. The
/// first client to arrive borrows a connection just long enough to copy its
/// parameters and gives it straight back; every client after that is greeted
/// without touching the pool.
#[derive(Debug, Clone, Default)]
pub struct Welcome(Arc<OnceLock<Vec<(String, String)>>>);

impl Welcome {
    async fn parameters<S, O, K>(&self, pool: &Pool<O, K>) -> Result<&[(String, String)], PoolError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
        O: OpenServer<Connection = ServerConnection<S>>,
        K: Clock,
    {
        if self.0.get().is_none() {
            let assigned = pool.acquire().await?;
            let parameters = assigned
                .parameters()
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect();
            drop(assigned);
            let _ = self.0.set(parameters);
        }
        Ok(self.0.get().expect("the welcome is set"))
    }
}

pub async fn transaction_mode<C, S, O, K>(
    client: ClientSession<C>,
    pool: &Pool<O, K>,
    welcome: &Welcome,
) -> Result<(), RelayError>
where
    C: AsyncRead + AsyncWrite + Unpin,
    S: AsyncRead + AsyncWrite + Unpin + Send,
    O: OpenServer<Connection = ServerConnection<S>>,
    K: Clock,
{
    let (mut client, mut pending) = client.into_parts();
    let mut greeting = BytesMut::new();
    match welcome.parameters(pool).await {
        Ok(parameters) => {
            for (name, value) in parameters {
                encode_parameter_status(name, value, &mut greeting);
            }
        }
        Err(error) => return give_up(&mut client, error).await,
    }
    encode_backend_key_data(cancel_key(), &mut greeting);
    encode_ready_for_query(TransactionStatus::Idle, &mut greeting);
    client.write_all(&greeting).await?;
    client.flush().await?;

    loop {
        if pending.is_empty() && fill(&mut client, &mut pending).await? == 0 {
            return Ok(());
        }
        if pending.first() == Some(&u8::from(FrontendTag::Terminate)) {
            return Ok(());
        }
        let mut assigned = match pool.acquire().await {
            Ok(assigned) => assigned,
            Err(error) => return give_up(&mut client, error).await,
        };
        match serve_assignment(&mut client, &mut pending, &mut assigned).await {
            Ok(Boundary::Released) => {}
            Ok(Boundary::ClientClosed { may_release: true }) => return Ok(()),
            Ok(Boundary::ClientClosed { may_release: false }) => {
                assigned.discard().await;
                return Ok(());
            }
            Err(error) => {
                assigned.discard().await;
                return Err(error);
            }
        }
    }
}

fn cancel_key() -> CancelKey {
    let mut rng = rand::rng();
    CancelKey {
        process_id: rng.random(),
        secret_key: rng.random(),
    }
}

async fn give_up<C: AsyncWrite + Unpin>(
    client: &mut C,
    error: PoolError,
) -> Result<(), RelayError> {
    let PoolError::WaitTimeout { waited } = error else {
        return Err(error.into());
    };
    let mut out = BytesMut::new();
    encode_error_response(
        &ErrorResponse::fatal(
            sqlstate::TOO_MANY_CONNECTIONS,
            format!("no server connection became free within {waited:?}"),
        )
        .with_hint("Retry, or ask for more connection slots for this tenant."),
        &mut out,
    );
    client.write_all(&out).await?;
    client.flush().await?;
    let _ = client.shutdown().await;
    Ok(())
}
