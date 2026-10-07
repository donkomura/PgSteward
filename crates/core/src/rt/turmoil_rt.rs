use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use turmoil::net::{TcpListener, TcpStream};

use super::{Clock, ClockRate, Instant, JoinHandle, Listener, Net, Spawner};

/// A clock that runs at its own rate from the instant the runtime is made, so a
/// host keeps one runtime and clones it rather than making another.
#[derive(Debug, Clone, Copy)]
pub struct TurmoilRuntime {
    rate: ClockRate,
    epoch: Instant,
}

impl Default for TurmoilRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl TurmoilRuntime {
    #[must_use]
    pub fn new() -> Self {
        Self::with_rate(ClockRate::EXACT)
    }

    #[must_use]
    pub fn with_rate(rate: ClockRate) -> Self {
        Self {
            rate,
            epoch: Instant::now(),
        }
    }

    fn local(&self, simulated: Duration) -> Duration {
        scale(
            simulated,
            self.rate.get().get(),
            ClockRate::EXACT.get().get(),
        )
    }

    fn simulated(&self, local: Duration) -> Duration {
        scale(local, ClockRate::EXACT.get().get(), self.rate.get().get())
    }
}

/// Rounds up, so that a sleep converted to simulated time lasts at least its
/// whole duration on the clock that measures it.
fn scale(duration: Duration, numerator: u32, denominator: u32) -> Duration {
    const NANOS_PER_SEC: u128 = 1_000_000_000;
    let nanos = (duration.as_nanos() * u128::from(numerator)).div_ceil(u128::from(denominator));
    let secs = u64::try_from(nanos / NANOS_PER_SEC).expect("a scaled duration fits in u64 seconds");
    let subsec = u32::try_from(nanos % NANOS_PER_SEC).expect("a remainder of a second fits in u32");
    Duration::new(secs, subsec)
}

impl Clock for TurmoilRuntime {
    fn now(&self) -> Instant {
        self.epoch + self.local(Instant::now().duration_since(self.epoch))
    }

    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send {
        tokio::time::sleep(self.simulated(duration))
    }
}

impl Spawner for TurmoilRuntime {
    fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        tokio::task::spawn_local(future)
    }

    fn spawn_worker<F>(&self, _name: String, future: F) -> io::Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        tokio::task::spawn_local(future);
        Ok(())
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
