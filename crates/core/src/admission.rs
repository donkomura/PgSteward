use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::io::{AsyncRead, AsyncWrite};

use crate::auth::Credentials;
use crate::session::{AcceptError, Accepted, accept, refuse_over_limit};

/// How many client connections this node holds at once. Whether to take a
/// client is the proxy's own decision (design-doc 8.2), so the limit is
/// node-local and says nothing about connection slots: a place here is not a
/// grant, and a client that holds one has not cost the instance anything yet.
#[derive(Debug, Clone)]
pub struct ClientLimit {
    max: usize,
    live: Arc<AtomicUsize>,
}

impl ClientLimit {
    #[must_use]
    pub fn new(max: usize) -> Self {
        Self {
            max,
            live: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[must_use]
    pub fn max(&self) -> usize {
        self.max
    }

    #[must_use]
    pub fn live(&self) -> usize {
        self.live.load(Ordering::SeqCst)
    }

    #[must_use]
    pub fn admit(&self) -> Option<Admitted> {
        let mut live = self.live.load(Ordering::SeqCst);
        loop {
            if live >= self.max {
                return None;
            }
            match self.live.compare_exchange_weak(
                live,
                live + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    return Some(Admitted {
                        live: Arc::clone(&self.live),
                    });
                }
                Err(current) => live = current,
            }
        }
    }

    pub async fn accept<S: AsyncRead + AsyncWrite + Unpin, C: Credentials>(
        &self,
        mut stream: S,
        credentials: C,
    ) -> Result<(Admitted, Accepted<S>), AcceptError> {
        let Some(admitted) = self.admit() else {
            return Err(refuse_over_limit(&mut stream, self.max).await);
        };
        Ok((admitted, accept(stream, credentials).await?))
    }
}

/// The place one client connection holds, given back when it is dropped.
#[derive(Debug)]
pub struct Admitted {
    live: Arc<AtomicUsize>,
}

impl Drop for Admitted {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}
