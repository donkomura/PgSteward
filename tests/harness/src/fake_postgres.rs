use std::future::{Future, ready};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use pgsteward_core::rt::{Listener, Runtime};
use tokio::io::AsyncReadExt;

use crate::cap::{ObserveConnections, ObserveError};

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

    fn on_open(&self) -> LiveGuard {
        self.accepted.fetch_add(1, Ordering::SeqCst);
        let live = self.live.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(live, Ordering::SeqCst);
        LiveGuard(Arc::clone(&self.live))
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
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let guard = accept_stats.on_open();
                accept_rt.spawn(async move {
                    let _guard = guard;
                    let mut sink = [0u8; 4096];
                    while let Ok(n) = stream.read(&mut sink).await {
                        if n == 0 {
                            break;
                        }
                    }
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
