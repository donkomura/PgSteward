use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};

pub use tokio::task::JoinHandle;
pub use tokio::time::Instant;

pub mod tokio_rt;
#[cfg(feature = "turmoil")]
pub mod turmoil_rt;

/// How fast a host's clock runs against simulated time, in millionths: a
/// clock at 1,001,000 gains a millisecond every second.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ClockRate {
    per_million: NonZeroU32,
}

impl ClockRate {
    pub const EXACT: Self = Self {
        per_million: NonZeroU32::new(1_000_000).unwrap(),
    };

    #[must_use]
    pub fn per_million(per_million: NonZeroU32) -> Self {
        Self { per_million }
    }

    #[must_use]
    pub fn get(self) -> NonZeroU32 {
        self.per_million
    }
}

pub trait Clock: Clone + Send + Sync + 'static {
    fn now(&self) -> Instant;

    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send;
}

pub trait Spawner: Clone + Send + Sync + 'static {
    fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static;

    /// Runs `future` as a worker: apart from the caller's tasks, with every
    /// task it spawns kept beside it. The worker ends when `future` does.
    fn spawn_worker<F>(&self, name: String, future: F) -> io::Result<()>
    where
        F: Future<Output = ()> + Send + 'static;
}

pub trait Listener: Send + Sync + 'static {
    type Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static;

    fn accept(&self) -> impl Future<Output = io::Result<(Self::Stream, SocketAddr)>> + Send;

    fn local_addr(&self) -> io::Result<SocketAddr>;
}

pub trait Net: Clone + Send + Sync + 'static {
    type Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static;
    type Listener: Listener<Stream = Self::Stream>;

    fn bind(&self, addr: &str) -> impl Future<Output = io::Result<Self::Listener>> + Send;

    fn connect(&self, addr: &str) -> impl Future<Output = io::Result<Self::Stream>> + Send;
}

pub trait Runtime: Clock + Spawner + Net {}

impl<T: Clock + Spawner + Net> Runtime for T {}
