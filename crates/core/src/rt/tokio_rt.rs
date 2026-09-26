use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};

use super::{Clock, Instant, JoinHandle, Listener, Net, Spawner};

#[derive(Debug, Clone, Copy, Default)]
pub struct TokioRuntime;

impl TokioRuntime {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Clock for TokioRuntime {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send {
        tokio::time::sleep(duration)
    }
}

impl Spawner for TokioRuntime {
    fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        tokio::spawn(future)
    }
}

impl Listener for TcpListener {
    type Stream = TcpStream;

    async fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        let (stream, addr) = TcpListener::accept(self).await?;
        stream.set_nodelay(true)?;
        Ok((stream, addr))
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        TcpListener::local_addr(self)
    }
}

impl Net for TokioRuntime {
    type Stream = TcpStream;
    type Listener = TcpListener;

    fn bind(&self, addr: &str) -> impl Future<Output = io::Result<TcpListener>> + Send {
        TcpListener::bind(addr.to_owned())
    }

    fn connect(&self, addr: &str) -> impl Future<Output = io::Result<TcpStream>> + Send {
        let addr = addr.to_owned();
        async move {
            let stream = TcpStream::connect(addr).await?;
            stream.set_nodelay(true)?;
            Ok(stream)
        }
    }
}
