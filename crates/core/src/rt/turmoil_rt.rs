use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use turmoil::net::{TcpListener, TcpStream};

use super::{Clock, Instant, JoinHandle, Listener, Net, Spawner};

#[derive(Debug, Clone, Copy, Default)]
pub struct TurmoilRuntime;

impl TurmoilRuntime {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Clock for TurmoilRuntime {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send {
        tokio::time::sleep(duration)
    }
}

impl Spawner for TurmoilRuntime {
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

    fn accept(&self) -> impl Future<Output = io::Result<(TcpStream, SocketAddr)>> + Send {
        TcpListener::accept(self)
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        TcpListener::local_addr(self)
    }
}

impl Net for TurmoilRuntime {
    type Stream = TcpStream;
    type Listener = TcpListener;

    fn bind(&self, addr: &str) -> impl Future<Output = io::Result<TcpListener>> + Send {
        TcpListener::bind(addr.to_owned())
    }

    fn connect(&self, addr: &str) -> impl Future<Output = io::Result<TcpStream>> + Send {
        TcpStream::connect(addr.to_owned())
    }
}
