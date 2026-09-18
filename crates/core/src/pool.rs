use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::sync::oneshot;

use crate::rt::{Clock, Net};
use crate::server::{ApplicationName, ConnectError, ServerConnection, ServerCredentials, connect};

pub trait OpenServer: Send + Sync + 'static {
    type Connection: Send + 'static;

    fn open(&self) -> impl Future<Output = Result<Self::Connection, ConnectError>> + Send;
}

/// Closes a server connection and waits for the server to let go of it.
///
/// A slot may only be handed on once the connection that held it is gone from
/// the instance. Dropping the socket is not enough: the server still counts the
/// connection until it notices the close, so a replacement opened in that
/// window puts the instance over its budget.
pub trait CloseServer: Sized + Send {
    fn close(self) -> impl Future<Output = ()> + Send;
}

#[derive(Debug, Clone)]
pub struct InstanceOpener<N> {
    net: N,
    addr: String,
    credentials: ServerCredentials,
    application_name: ApplicationName,
}

impl<N: Net> InstanceOpener<N> {
    #[must_use]
    pub fn new(
        net: N,
        addr: String,
        credentials: ServerCredentials,
        application_name: ApplicationName,
    ) -> Self {
        Self {
            net,
            addr,
            credentials,
            application_name,
        }
    }
}

impl<N: Net> OpenServer for InstanceOpener<N> {
    type Connection = ServerConnection<N::Stream>;

    fn open(&self) -> impl Future<Output = Result<Self::Connection, ConnectError>> + Send {
        connect(
            &self.net,
            &self.addr,
            &self.credentials,
            &self.application_name,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolLimits {
    pub slots: usize,
    pub wait_timeout: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PoolStats {
    pub slots: usize,
    pub idle: usize,
    pub in_use: usize,
    pub opening: usize,
    pub closing: usize,
    pub waiting: usize,
}

impl PoolStats {
    #[must_use]
    pub fn actual(&self) -> usize {
        self.idle + self.in_use + self.closing
    }

    #[must_use]
    pub fn occupied(&self) -> usize {
        self.actual() + self.opening
    }

    #[must_use]
    pub fn demand(&self) -> usize {
        self.waiting + self.in_use + self.opening
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    #[error("no connection slot became free within {waited:?}")]
    WaitTimeout { waited: Duration },
    #[error("could not open a server connection: {0}")]
    Open(#[from] ConnectError),
    #[error("the pool stopped answering requests")]
    Closed,
}

pub struct Pool<O: OpenServer, K: Clock> {
    open: Arc<O>,
    clock: K,
    wait_timeout: Duration,
    inner: Arc<Inner<O::Connection>>,
}

impl<O: OpenServer, K: Clock> Pool<O, K> {
    #[must_use]
    pub fn new(open: O, clock: K, limits: PoolLimits) -> Self {
        Self {
            open: Arc::new(open),
            clock,
            wait_timeout: limits.wait_timeout,
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    slots: limits.slots,
                    idle: Vec::new(),
                    in_use: 0,
                    opening: 0,
                    closing: 0,
                    retiring: Vec::new(),
                    waiters: VecDeque::new(),
                    next_ticket: 0,
                }),
            }),
        }
    }

    #[must_use]
    pub fn stats(&self) -> PoolStats {
        self.inner.stats()
    }

    /// True when nothing but this handle refers to the pool and it holds no
    /// connection: no client is waiting on it, none holds an assignment, and
    /// the instance has nothing of it left to close.
    #[must_use]
    pub fn is_unused(&self) -> bool {
        Arc::strong_count(&self.inner) == 1 && self.stats().occupied() == 0
    }

    pub async fn acquire(&self) -> Result<Assigned<O::Connection>, PoolError> {
        match self.inner.request() {
            Request::Assigned(assigned) => Ok(assigned),
            Request::Reserved(reservation) => self.open_into(reservation).await,
            Request::Queued(ticket, receiver) => match self.wait(ticket, receiver).await? {
                Handoff::Connection(assigned) => Ok(assigned),
                Handoff::Slot(reservation) => self.open_into(reservation).await,
            },
        }
    }

    async fn open_into(
        &self,
        reservation: Reservation<O::Connection>,
    ) -> Result<Assigned<O::Connection>, PoolError> {
        let connection = self.open.open().await?;
        Ok(reservation.fulfil(connection))
    }

    async fn wait(
        &self,
        ticket: u64,
        mut receiver: oneshot::Receiver<Handoff<O::Connection>>,
    ) -> Result<Handoff<O::Connection>, PoolError> {
        let started = self.clock.now();
        tokio::select! {
            handoff = &mut receiver => handoff.map_err(|_| PoolError::Closed),
            () = self.clock.sleep(self.wait_timeout) => {
                let waited = self.clock.now().duration_since(started);
                if self.inner.give_up(ticket) {
                    return Err(PoolError::WaitTimeout { waited });
                }
                receiver
                    .try_recv()
                    .map_err(|_| PoolError::WaitTimeout { waited })
            }
        }
    }
}

impl<O: OpenServer, K: Clock> Pool<O, K>
where
    O::Connection: CloseServer,
{
    pub async fn converge(&self, grant: usize) -> usize {
        let retired = self.inner.retire(grant);
        let closed = retired.len();
        for connection in retired {
            connection.close().await;
        }
        self.inner.closed(closed);
        closed
    }
}

impl<O: OpenServer, K: Clock> Clone for Pool<O, K> {
    fn clone(&self) -> Self {
        Self {
            open: Arc::clone(&self.open),
            clock: self.clock.clone(),
            wait_timeout: self.wait_timeout,
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<O: OpenServer, K: Clock> fmt::Debug for Pool<O, K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pool")
            .field("wait_timeout", &self.wait_timeout)
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

pub struct Assigned<C> {
    connection: Option<C>,
    pool: Arc<Inner<C>>,
}

impl<C> Assigned<C> {
    fn new(connection: C, pool: &Arc<Inner<C>>) -> Self {
        Self {
            connection: Some(connection),
            pool: Arc::clone(pool),
        }
    }
}

impl<C: CloseServer> Assigned<C> {
    pub async fn discard(mut self) {
        if let Some(connection) = self.connection.take() {
            connection.close().await;
        }
        let mut state = self.pool.lock();
        state.in_use -= 1;
        state.opening += 1;
        drop(state);
        drop(Reservation::new(&self.pool));
    }
}

impl<C> Deref for Assigned<C> {
    type Target = C;

    fn deref(&self) -> &C {
        self.connection
            .as_ref()
            .expect("an assignment holds its connection until it is dropped")
    }
}

impl<C> DerefMut for Assigned<C> {
    fn deref_mut(&mut self) -> &mut C {
        self.connection
            .as_mut()
            .expect("an assignment holds its connection until it is dropped")
    }
}

impl<C> Drop for Assigned<C> {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            release(&self.pool, connection);
        }
    }
}

impl<C> fmt::Debug for Assigned<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Assigned")
            .field("held", &self.connection.is_some())
            .finish_non_exhaustive()
    }
}

struct Inner<C> {
    state: Mutex<State<C>>,
}

struct State<C> {
    slots: usize,
    idle: Vec<C>,
    in_use: usize,
    opening: usize,
    closing: usize,
    retiring: Vec<C>,
    waiters: VecDeque<Waiter<C>>,
    next_ticket: u64,
}

impl<C> State<C> {
    fn occupied(&self) -> usize {
        self.idle.len() + self.in_use + self.opening + self.closing
    }

    fn over_grant(&self) -> bool {
        self.idle.len() + self.in_use + self.opening > self.slots
    }
}

struct Waiter<C> {
    ticket: u64,
    handoff: oneshot::Sender<Handoff<C>>,
}

enum Handoff<C> {
    Connection(Assigned<C>),
    Slot(Reservation<C>),
}

impl<C> Handoff<C> {
    fn reclaim(self) -> Option<C> {
        match self {
            Self::Connection(mut assigned) => assigned.connection.take(),
            Self::Slot(mut reservation) => {
                reservation.held = false;
                None
            }
        }
    }
}

enum Request<C> {
    Assigned(Assigned<C>),
    Reserved(Reservation<C>),
    Queued(u64, oneshot::Receiver<Handoff<C>>),
}

impl<C> Inner<C> {
    fn lock(&self) -> MutexGuard<'_, State<C>> {
        self.state.lock().expect("pool state lock poisoned")
    }

    fn stats(&self) -> PoolStats {
        let state = self.lock();
        PoolStats {
            slots: state.slots,
            idle: state.idle.len(),
            in_use: state.in_use,
            opening: state.opening,
            closing: state.closing,
            waiting: state.waiters.len(),
        }
    }

    fn retire(&self, grant: usize) -> Vec<C> {
        let mut state = self.lock();
        state.slots = grant;
        let mut retired = std::mem::take(&mut state.retiring);
        while state.over_grant() {
            let Some(connection) = state.idle.pop() else {
                break;
            };
            state.closing += 1;
            retired.push(connection);
        }
        retired
    }

    fn closed(self: &Arc<Self>, count: usize) {
        self.lock().closing -= count;
        self.hand_out();
    }

    fn hand_out(self: &Arc<Self>) {
        loop {
            let mut state = self.lock();
            if state.waiters.is_empty() {
                return;
            }
            if let Some(connection) = state.idle.pop() {
                state.in_use += 1;
                drop(state);
                release(self, connection);
                continue;
            }
            if state.occupied() >= state.slots {
                return;
            }
            state.opening += 1;
            let waiter = state
                .waiters
                .pop_front()
                .expect("the queue was not empty under the same lock");
            drop(state);
            if let Err(rejected) = waiter.handoff.send(Handoff::Slot(Reservation::new(self))) {
                rejected.reclaim();
                self.lock().opening -= 1;
            }
        }
    }

    fn request(self: &Arc<Self>) -> Request<C> {
        let mut state = self.lock();
        if let Some(connection) = state.idle.pop() {
            state.in_use += 1;
            return Request::Assigned(Assigned::new(connection, self));
        }
        if state.occupied() < state.slots {
            state.opening += 1;
            return Request::Reserved(Reservation::new(self));
        }
        let ticket = state.next_ticket;
        state.next_ticket += 1;
        let (handoff, receiver) = oneshot::channel();
        state.waiters.push_back(Waiter { ticket, handoff });
        Request::Queued(ticket, receiver)
    }

    fn give_up(&self, ticket: u64) -> bool {
        let mut state = self.lock();
        let queued = state.waiters.len();
        state.waiters.retain(|waiter| waiter.ticket != ticket);
        state.waiters.len() != queued
    }
}

struct Reservation<C> {
    pool: Arc<Inner<C>>,
    held: bool,
}

impl<C> Reservation<C> {
    fn new(pool: &Arc<Inner<C>>) -> Self {
        Self {
            pool: Arc::clone(pool),
            held: true,
        }
    }

    fn fulfil(mut self, connection: C) -> Assigned<C> {
        let mut state = self.pool.lock();
        state.opening -= 1;
        state.in_use += 1;
        drop(state);
        self.held = false;
        Assigned::new(connection, &self.pool)
    }
}

impl<C> Drop for Reservation<C> {
    fn drop(&mut self) {
        if !self.held {
            return;
        }
        loop {
            let mut state = self.pool.lock();
            if state.over_grant() {
                state.opening -= 1;
                return;
            }
            let Some(waiter) = state.waiters.pop_front() else {
                state.opening -= 1;
                return;
            };
            drop(state);
            match waiter
                .handoff
                .send(Handoff::Slot(Reservation::new(&self.pool)))
            {
                Ok(()) => {
                    self.held = false;
                    return;
                }
                Err(rejected) => {
                    rejected.reclaim();
                }
            }
        }
    }
}

fn release<C>(pool: &Arc<Inner<C>>, mut connection: C) {
    loop {
        let mut state = pool.lock();
        if state.over_grant() {
            state.in_use -= 1;
            state.closing += 1;
            state.retiring.push(connection);
            return;
        }
        let Some(waiter) = state.waiters.pop_front() else {
            state.in_use -= 1;
            state.idle.push(connection);
            return;
        };
        drop(state);
        match waiter
            .handoff
            .send(Handoff::Connection(Assigned::new(connection, pool)))
        {
            Ok(()) => return,
            Err(rejected) => match rejected.reclaim() {
                Some(returned) => connection = returned,
                None => return,
            },
        }
    }
}
