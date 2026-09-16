use std::future::{Future, ready};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::{BufMut, Bytes, BytesMut};
use pgsteward_core::rt::{Listener, Runtime};
use pgsteward_protocol::backend::{
    ErrorResponse, encode_authentication_ok, encode_backend_key_data, encode_error_response,
    encode_parameter_status, encode_ready_for_query, sqlstate,
};
use pgsteward_protocol::framing::{Frame, decode_frame, decode_startup_frame, encode_frame};
use pgsteward_protocol::message::TransactionStatus;
use pgsteward_protocol::startup::{CancelKey, StartupRequest, decode_startup};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::cap::{ObserveConnections, ObserveError};

const MAX_FRAME: usize = 1 << 20;
const READ_CHUNK: usize = 4096;
const SERVER_VERSION: &str = "16.0 (pgsteward-harness)";
const TEXT_OID: i32 = 25;
const BACKEND_COLUMN: &[u8] = b"backend\0";
const COMMAND_TAG: &[u8] = b"SELECT 1\0";

#[derive(Debug, Clone, Default)]
pub struct FakePostgresStats {
    live: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    accepted: Arc<AtomicUsize>,
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
                accept_rt.spawn(async move {
                    let _guard = guard;
                    let _ = serve(stream, backend).await;
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

async fn serve<S: AsyncRead + AsyncWrite + Unpin>(mut stream: S, backend: i32) -> io::Result<()> {
    let mut buf = BytesMut::new();
    let Some(body) = read_startup(&mut stream, &mut buf).await? else {
        return Ok(());
    };
    let Ok(StartupRequest::Startup(_)) = decode_startup(&body) else {
        return Ok(());
    };
    write(&mut stream, &greeting(backend)).await?;

    while let Some(frame) = read_frame(&mut stream, &mut buf).await? {
        let mut out = BytesMut::new();
        match frame.tag {
            b'X' => return Ok(()),
            b'Q' => encode_backend_row(backend, &mut out),
            _ => encode_error_response(
                &ErrorResponse::error(
                    sqlstate::FEATURE_NOT_SUPPORTED,
                    "the fake PostgreSQL answers simple queries only",
                ),
                &mut out,
            ),
        }
        encode_ready_for_query(TransactionStatus::Idle, &mut out);
        write(&mut stream, &out).await?;
    }
    Ok(())
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

fn encode_backend_row(backend: i32, out: &mut BytesMut) {
    let mut description = BytesMut::new();
    description.put_i16(1);
    description.put_slice(BACKEND_COLUMN);
    description.put_i32(0);
    description.put_i16(0);
    description.put_i32(TEXT_OID);
    description.put_i16(-1);
    description.put_i32(-1);
    description.put_i16(0);
    encode_frame(b'T', &description, out);

    let value = backend.to_string();
    let mut row = BytesMut::new();
    row.put_i16(1);
    row.put_i32(i32::try_from(value.len()).expect("a backend identifier is a handful of digits"));
    row.put_slice(value.as_bytes());
    encode_frame(b'D', &row, out);

    encode_frame(b'C', COMMAND_TAG, out);
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
