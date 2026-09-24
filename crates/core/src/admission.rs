use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::watch;

use crate::auth::Credentials;
use crate::session::{AcceptError, Accepted, accept, refuse_over_limit};
use crate::tls::{ClientTls, MaybeTls};

/// How many client connections this node holds at once. Whether to take a
/// client is the proxy's own decision (design-doc 8.2), so the limit is
/// node-local and says nothing about connection slots: a place here is not a
/// grant, and a client that holds one has not cost the instance anything yet.
#[derive(Debug, Clone)]
pub struct ClientLimit {
    max: usize,
    live: Arc<watch::Sender<usize>>,
}

impl ClientLimit {
    #[must_use]
    pub fn new(max: usize) -> Self {
        Self {
            max,
            live: Arc::new(watch::Sender::new(0)),
        }
    }

    #[must_use]
    pub fn max(&self) -> usize {
        self.max
    }

    #[must_use]
    pub fn live(&self) -> usize {
        *self.live.borrow()
    }

    #[must_use]
    pub fn admit(&self) -> Option<Admitted> {
        let mut taken = false;
        self.live.send_if_modified(|live| {
            if *live >= self.max {
                return false;
            }
            *live += 1;
            taken = true;
            true
        });
        taken.then(|| Admitted {
            live: Arc::clone(&self.live),
        })
    }

    /// Returns once this node holds no client connection.
    ///
    /// A node that is stopping waits here: a client that is still being served
    /// holds a place until its session ends, whichever way it ends.
    pub async fn drained(&self) {
        let mut live = self.live.subscribe();
        while *live.borrow_and_update() > 0 {
            if live.changed().await.is_err() {
                return;
            }
        }
    }

    pub async fn accept<S: AsyncRead + AsyncWrite + Unpin, C: Credentials>(
        &self,
        mut stream: S,
        credentials: C,
        tls: Option<&ClientTls>,
    ) -> Result<(Admitted, Accepted<MaybeTls<S>>), AcceptError> {
        let Some(admitted) = self.admit() else {
            return Err(refuse_over_limit(&mut stream, self.max).await);
        };
        Ok((admitted, accept(stream, credentials, tls).await?))
    }
}

/// The place one client connection holds, given back when it is dropped.
#[derive(Debug)]
pub struct Admitted {
    live: Arc<watch::Sender<usize>>,
}

impl Drop for Admitted {
    fn drop(&mut self) {
        self.live.send_modify(|live| *live -= 1);
    }
}
