use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};

pub use tokio::task::JoinHandle;
pub use tokio::time::Instant;

pub mod tokio_rt;
#[cfg(feature = "turmoil")]
pub mod turmoil_rt;

pub trait Clock: Clone + Send + Sync + 'static {
    fn now(&self) -> Instant;

    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send;
}

pub trait Spawner: Clone + Send + Sync + 'static {
    fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static;
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
