use std::future::{Future, ready};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::{BufMut, Bytes, BytesMut};
use pgsteward_core::budget::ServerLimits;
use pgsteward_core::rt::{Listener, Runtime};
use pgsteward_core::server::ApplicationName;
use pgsteward_protocol::backend::{
    ErrorResponse, encode_authentication_ok, encode_backend_key_data, encode_error_response,
    encode_parameter_status, encode_ready_for_query, sqlstate,
};
use pgsteward_protocol::framing::{Frame, decode_frame, decode_startup_frame, encode_frame};
use pgsteward_protocol::message::TransactionStatus;
use pgsteward_protocol::startup::{CancelKey, StartupMessage, StartupRequest, decode_startup};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::cap::{ObserveConnections, ObserveError};

const MAX_FRAME: usize = 1 << 20;
const READ_CHUNK: usize = 4096;
const SERVER_VERSION: &str = "16.0 (pgsteward-harness)";
const TEXT_OID: i32 = 25;
const BACKEND_COLUMN: &str = "backend";
const COMMAND_TAG: &[u8] = b"SELECT 1\0";
const SHOW_TAG: &[u8] = b"SHOW\0";
const COUNT_COLUMN: &str = "count";

/// What PostgreSQL 16 starts with when its configuration sets none of them.
pub const POSTGRESQL_DEFAULT_LIMITS: ServerLimits = ServerLimits {
    max_connections: 100,
    superuser_reserved_connections: 3,
    reserved_connections: 0,
};

#[derive(Debug, Clone, Default)]
pub struct FakePostgresStats {
    live: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    accepted: Arc<AtomicUsize>,
    foreign: Arc<AtomicUsize>,
}

impl FakePostgresStats {
    #[must_use]
    pub fn live(&self) -> usize {
        self.live.load(Ordering::SeqCst)
    }

    #[must_use]
    pub fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }

    #[must_use]
    pub fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }

    /// The live connections whose `application_name` does not carry this
    /// system's prefix, which is what `pg_stat_activity` is asked to count.
    fn foreign(&self) -> usize {
        self.foreign.load(Ordering::SeqCst)
    }

    fn on_foreign(&self) -> LiveGuard {
        self.foreign.fetch_add(1, Ordering::SeqCst);
        LiveGuard(Arc::clone(&self.foreign))
    }

    fn on_open(&self) -> (LiveGuard, i32) {
        let backend = self.accepted.fetch_add(1, Ordering::SeqCst) + 1;
        let live = self.live.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(live, Ordering::SeqCst);
        let backend =
            i32::try_from(backend).expect("the fake PostgreSQL serves fewer than i32::MAX clients");
        (LiveGuard(Arc::clone(&self.live)), backend)
    }
}

struct LiveGuard(Arc<AtomicUsize>);

impl Drop for LiveGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl ObserveConnections for FakePostgresStats {
    fn observe(&self) -> impl Future<Output = Result<usize, ObserveError>> + Send {
        ready(Ok(self.live()))
    }
}

#[derive(Debug)]
pub struct FakePostgres {
    addr: SocketAddr,
    stats: FakePostgresStats,
}

impl FakePostgres {
    pub async fn start<R: Runtime>(
        rt: &R,
        bind_addr: &str,
        stats: FakePostgresStats,
    ) -> io::Result<Self> {
        Self::start_with_limits(rt, bind_addr, stats, POSTGRESQL_DEFAULT_LIMITS).await
    }

    /// Starts a fake PostgreSQL whose `SHOW` reports `limits`, so that a node
    /// pointed at it derives its total budget from them.
    pub async fn start_with_limits<R: Runtime>(
        rt: &R,
        bind_addr: &str,
        stats: FakePostgresStats,
        limits: ServerLimits,
    ) -> io::Result<Self> {
        let listener = rt.bind(bind_addr).await?;
        let addr = listener.local_addr()?;
        let accept_rt = rt.clone();
        let accept_stats = stats.clone();
        rt.spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let (guard, backend) = accept_stats.on_open();
                let stats = accept_stats.clone();
                accept_rt.spawn(async move {
                    let _guard = guard;
                    let _ = serve(stream, backend, limits, stats).await;
                });
            }
        });
        Ok(Self { addr, stats })
    }

    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    #[must_use]
    pub fn stats(&self) -> &FakePostgresStats {
        &self.stats
    }
}

async fn serve<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    backend: i32,
    limits: ServerLimits,
    stats: FakePostgresStats,
) -> io::Result<()> {
    let mut buf = BytesMut::new();
    let startup = loop {
        let Some(body) = read_startup(&mut stream, &mut buf).await? else {
            return Ok(());
        };
        match decode_startup(&body) {
            Ok(StartupRequest::Startup(startup)) => break startup,
            // The fake PostgreSQL holds no certificate, so it answers every
            // encryption request the way a server built without TLS does.
            Ok(StartupRequest::Ssl | StartupRequest::GssEnc) => {
                write(&mut stream, b"N").await?;
            }
            _ => return Ok(()),
        }
    };
    let _foreign = is_foreign(&startup).then(|| stats.on_foreign());
    write(&mut stream, &greeting(backend)).await?;

    let mut transaction = TransactionStatus::Idle;
    while let Some(frame) = read_frame(&mut stream, &mut buf).await? {
        let mut out = BytesMut::new();
        match frame.tag {
            b'X' => return Ok(()),
            b'Q' => match answer(&frame.body, backend, limits, &stats, &mut out) {
                Ok(()) => transaction = next_status(transaction, &frame.body),
                Err(error) => {
                    encode_error_response(&error, &mut out);
                    if transaction == TransactionStatus::InTransaction {
                        transaction = TransactionStatus::Failed;
                    }
                }
            },
            _ => encode_error_response(
                &ErrorResponse::error(
                    sqlstate::FEATURE_NOT_SUPPORTED,
                    "the fake PostgreSQL answers simple queries only",
                ),
                &mut out,
            ),
        }
        encode_ready_for_query(transaction, &mut out);
        write(&mut stream, &out).await?;
    }
    Ok(())
}

/// Answers the few queries this system itself sends: `SHOW` of the limits the
/// total budget is derived from, and the count of foreign connections in
/// `pg_stat_activity`. Every other query reports the backend it ran on.
fn answer(
    query: &[u8],
    backend: i32,
    limits: ServerLimits,
    stats: &FakePostgresStats,
    out: &mut BytesMut,
) -> Result<(), ErrorResponse> {
    let text = String::from_utf8_lossy(query);
    let mut words = words(&text);
    if words
        .next()
        .is_some_and(|word| word.eq_ignore_ascii_case("SHOW"))
    {
        let setting = words.next().unwrap_or_default().to_ascii_lowercase();
        let value = show(&setting, limits).ok_or_else(|| {
            ErrorResponse::error(
                sqlstate::UNDEFINED_OBJECT,
                format!("unrecognized configuration parameter \"{setting}\""),
            )
        })?;
        encode_single_value(&setting, &value.to_string(), SHOW_TAG, out);
    } else if text.contains("pg_stat_activity") {
        encode_single_value(COUNT_COLUMN, &stats.foreign().to_string(), COMMAND_TAG, out);
    } else {
        encode_single_value(BACKEND_COLUMN, &backend.to_string(), COMMAND_TAG, out);
    }
    Ok(())
}

fn show(setting: &str, limits: ServerLimits) -> Option<u32> {
    match setting {
        "max_connections" => Some(limits.max_connections),
        "superuser_reserved_connections" => Some(limits.superuser_reserved_connections),
        "reserved_connections" => Some(limits.reserved_connections),
        _ => None,
    }
}

fn is_foreign(startup: &StartupMessage) -> bool {
    !startup
        .parameter("application_name")
        .is_some_and(|name| name.starts_with(ApplicationName::PREFIX))
}

fn words(text: &str) -> impl Iterator<Item = &str> {
    text.split(|c: char| c.is_whitespace() || c == ';' || c == '\0')
        .filter(|word| !word.is_empty())
}

fn next_status(current: TransactionStatus, query: &[u8]) -> TransactionStatus {
    let text = String::from_utf8_lossy(query);
    let first_word = words(&text).next().unwrap_or_default().to_ascii_uppercase();
    match first_word.as_str() {
        "BEGIN" | "START" => TransactionStatus::InTransaction,
        "COMMIT" | "END" | "ROLLBACK" | "ABORT" => TransactionStatus::Idle,
        _ => current,
    }
}

fn greeting(backend: i32) -> BytesMut {
    let mut out = BytesMut::new();
    encode_authentication_ok(&mut out);
    encode_parameter_status("server_version", SERVER_VERSION, &mut out);
    encode_parameter_status("client_encoding", "UTF8", &mut out);
    encode_backend_key_data(
        CancelKey {
            process_id: backend,
            secret_key: backend,
        },
        &mut out,
    );
    encode_ready_for_query(TransactionStatus::Idle, &mut out);
    out
}

fn encode_single_value(column: &str, value: &str, tag: &[u8], out: &mut BytesMut) {
    let mut description = BytesMut::new();
    description.put_i16(1);
    description.put_slice(column.as_bytes());
    description.put_u8(0);
    description.put_i32(0);
    description.put_i16(0);
    description.put_i32(TEXT_OID);
    description.put_i16(-1);
    description.put_i32(-1);
    description.put_i16(0);
    encode_frame(b'T', &description, out);

    let mut row = BytesMut::new();
    row.put_i16(1);
    row.put_i32(i32::try_from(value.len()).expect("a single value is a handful of digits"));
    row.put_slice(value.as_bytes());
    encode_frame(b'D', &row, out);

    encode_frame(b'C', tag, out);
}

async fn read_startup<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
) -> io::Result<Option<Bytes>> {
    loop {
        match decode_startup_frame(buf, MAX_FRAME) {
            Ok(Some(body)) => return Ok(Some(body)),
            Ok(None) => {}
            Err(_) => return Ok(None),
        }
        if fill(stream, buf).await? == 0 {
            return Ok(None);
        }
    }
}

async fn read_frame<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
) -> io::Result<Option<Frame>> {
    loop {
        match decode_frame(buf, MAX_FRAME) {
            Ok(Some(frame)) => return Ok(Some(frame)),
            Ok(None) => {}
            Err(_) => return Ok(None),
        }
        if fill(stream, buf).await? == 0 {
            return Ok(None);
        }
    }
}

async fn fill<S: AsyncRead + Unpin>(stream: &mut S, buf: &mut BytesMut) -> io::Result<usize> {
    buf.reserve(READ_CHUNK);
    stream.read_buf(buf).await
}

async fn write<S: AsyncWrite + Unpin>(stream: &mut S, bytes: &[u8]) -> io::Result<()> {
    stream.write_all(bytes).await?;
    stream.flush().await
}
