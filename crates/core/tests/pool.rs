use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pgsteward_core::pool::{CloseServer, OpenServer, Pool, PoolError, PoolLimits};
use pgsteward_core::rt::{Clock, tokio_rt::TokioRuntime};
use pgsteward_core::server::ConnectError;

const WAIT_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug)]
struct FakeConnection {
    id: usize,
    live: Arc<AtomicUsize>,
}

impl Drop for FakeConnection {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Debug, Clone, Default)]
struct Opener {
    attempts: Arc<AtomicUsize>,
    opened: Arc<AtomicUsize>,
    live: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    refusals: Arc<AtomicUsize>,
    delay: Duration,
}

impl Opener {
    fn refusing(refusals: usize) -> Self {
        Self {
            refusals: Arc::new(AtomicUsize::new(refusals)),
            ..Self::default()
        }
    }

    fn slow(delay: Duration) -> Self {
        Self {
            delay,
            ..Self::default()
        }
    }

    fn with_delay(self, delay: Duration) -> Self {
        Self { delay, ..self }
    }

    fn attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }

    fn opened(&self) -> usize {
        self.opened.load(Ordering::SeqCst)
    }

    fn live(&self) -> usize {
        self.live.load(Ordering::SeqCst)
    }

    fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }

    fn refuse_once(&self) -> bool {
        self.refusals
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                (left > 0).then(|| left - 1)
            })
            .is_ok()
    }
}

impl OpenServer for Opener {
    type Connection = FakeConnection;

    async fn open(&self) -> Result<FakeConnection, ConnectError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        if !self.delay.is_zero() {
            TokioRuntime::new().sleep(self.delay).await;
        }
        if self.refuse_once() {
            return Err(ConnectError::Unreachable(io::Error::other(
                "the fake opener refused this attempt",
            )));
        }
        let id = self.opened.fetch_add(1, Ordering::SeqCst);
        let live = self.live.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(live, Ordering::SeqCst);
        Ok(FakeConnection {
            id,
            live: Arc::clone(&self.live),
        })
    }
}

impl CloseServer for FakeConnection {
    async fn close(self) {}
}

fn pool(opener: Opener, slots: usize) -> Pool<Opener, TokioRuntime> {
    Pool::new(
        opener,
        TokioRuntime::new(),
        PoolLimits {
            slots,
            wait_timeout: WAIT_TIMEOUT,
        },
    )
}

#[tokio::test(start_paused = true)]
async fn a_new_pool_opens_no_connection_until_the_first_client_asks() {
    let opener = Opener::default();
    let pool = pool(opener.clone(), 2);

    assert_eq!(opener.attempts(), 0);
    assert_eq!(pool.stats().actual(), 0);

    let assigned = pool.acquire().await.unwrap();

    assert_eq!(opener.opened(), 1);
    assert_eq!(pool.stats().in_use, 1);
    assert_eq!(pool.stats().actual(), 1);
    drop(assigned);
}

#[tokio::test(start_paused = true)]
async fn a_released_connection_is_handed_to_the_next_client() {
    let opener = Opener::default();
    let pool = pool(opener.clone(), 2);

    let first = pool.acquire().await.unwrap();
    let id = first.id;
    drop(first);
    assert_eq!(pool.stats().idle, 1);

    let second = pool.acquire().await.unwrap();

    assert_eq!(second.id, id);
    assert_eq!(opener.attempts(), 1);
    assert_eq!(pool.stats().idle, 0);
    assert_eq!(pool.stats().in_use, 1);
}

#[tokio::test(start_paused = true)]
async fn one_slot_is_never_assigned_to_two_clients_at_once() {
    let opener = Opener::default();
    let pool = pool(opener.clone(), 1);

    let held = pool.acquire().await.unwrap();
    let waiting = tokio::spawn({
        let pool = pool.clone();
        async move { pool.acquire().await }
    });
    TokioRuntime::new().sleep(Duration::from_millis(100)).await;

    assert!(
        !waiting.is_finished(),
        "a client must wait while another holds the only slot"
    );
    assert_eq!(pool.stats().waiting, 1);

    let id = held.id;
    drop(held);
    let served = waiting.await.unwrap().unwrap();

    assert_eq!(served.id, id);
    assert_eq!(opener.opened(), 1);
    assert_eq!(opener.peak(), 1);
}

#[tokio::test(start_paused = true)]
async fn the_number_of_open_connections_never_exceeds_the_slots() {
    let opener = Opener::slow(Duration::from_millis(10));
    let pool = pool(opener.clone(), 2);

    let clients: Vec<_> = (0..8)
        .map(|_| {
            let pool = pool.clone();
            tokio::spawn(async move {
                let assigned = pool.acquire().await.unwrap();
                TokioRuntime::new().sleep(Duration::from_millis(5)).await;
                drop(assigned);
            })
        })
        .collect();
    for client in clients {
        client.await.unwrap();
    }

    assert_eq!(opener.peak(), 2, "more connections than slots were open");
    assert_eq!(opener.opened(), 2);
    assert_eq!(pool.stats().in_use, 0);
    assert_eq!(pool.stats().idle, 2);
}

#[tokio::test(start_paused = true)]
async fn clients_are_served_in_the_order_they_started_waiting() {
    let opener = Opener::default();
    let pool = pool(opener.clone(), 1);
    let held = pool.acquire().await.unwrap();
    let served = Arc::new(Mutex::new(Vec::new()));

    let mut waiters = Vec::new();
    for arrival in 0..3 {
        let pool = pool.clone();
        let served = Arc::clone(&served);
        waiters.push(tokio::spawn(async move {
            let assigned = pool.acquire().await.unwrap();
            served.lock().unwrap().push(arrival);
            drop(assigned);
        }));
        TokioRuntime::new().sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(pool.stats().waiting, 3);

    drop(held);
    for waiter in waiters {
        waiter.await.unwrap();
    }

    assert_eq!(served.lock().unwrap().as_slice(), [0, 1, 2]);
    assert_eq!(opener.opened(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_client_that_waits_longer_than_the_wait_timeout_is_refused() {
    let opener = Opener::default();
    let pool = pool(opener.clone(), 1);
    let held = pool.acquire().await.unwrap();

    let error = pool.acquire().await.unwrap_err();

    match error {
        PoolError::WaitTimeout { waited } => assert!(waited >= WAIT_TIMEOUT, "{waited:?}"),
        other => panic!("expected a wait timeout, got {other:?}"),
    }
    assert_eq!(
        pool.stats().waiting,
        0,
        "a refused client must leave the queue"
    );
    assert_eq!(opener.attempts(), 1);
    drop(held);
}

#[tokio::test(start_paused = true)]
async fn an_idle_connection_is_kept_however_long_it_is_unused() {
    let opener = Opener::default();
    let pool = pool(opener.clone(), 1);
    let first = pool.acquire().await.unwrap();
    let id = first.id;
    drop(first);

    TokioRuntime::new().sleep(Duration::from_secs(3600)).await;

    assert_eq!(
        opener.live(),
        1,
        "the pool has no idle timeout, so it must not close an unused connection"
    );
    let again = pool.acquire().await.unwrap();
    assert_eq!(again.id, id);
    assert_eq!(opener.attempts(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_refused_open_frees_the_slot_it_reserved() {
    let opener = Opener::refusing(1);
    let pool = pool(opener.clone(), 1);

    let error = pool.acquire().await.unwrap_err();

    assert!(matches!(error, PoolError::Open(_)), "{error:?}");
    assert_eq!(pool.stats().actual(), 0);
    assert_eq!(pool.stats().in_use, 0);

    let assigned = pool.acquire().await.unwrap();

    assert_eq!(opener.attempts(), 2);
    assert_eq!(opener.opened(), 1);
    drop(assigned);
}

#[tokio::test(start_paused = true)]
async fn the_slot_of_a_refused_open_goes_to_the_waiting_client() {
    let opener = Opener::refusing(1).with_delay(Duration::from_millis(50));
    let pool = pool(opener.clone(), 1);

    let refused = tokio::spawn({
        let pool = pool.clone();
        async move { pool.acquire().await }
    });
    TokioRuntime::new().sleep(Duration::from_millis(10)).await;
    let waiting = tokio::spawn({
        let pool = pool.clone();
        async move { pool.acquire().await }
    });
    TokioRuntime::new().sleep(Duration::from_millis(10)).await;
    assert_eq!(pool.stats().waiting, 1);

    assert!(refused.await.unwrap().is_err());
    let served = waiting.await.unwrap().unwrap();

    assert_eq!(served.id, 0);
    assert_eq!(opener.attempts(), 2);
    assert_eq!(opener.peak(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_discarded_connection_frees_its_slot_without_going_back_to_the_pool() {
    let opener = Opener::default();
    let pool = pool(opener.clone(), 1);

    let assigned = pool.acquire().await.unwrap();
    assigned.discard().await;

    assert_eq!(pool.stats().idle, 0);
    assert_eq!(pool.stats().in_use, 0);
    assert_eq!(pool.stats().actual(), 0);
    assert_eq!(opener.live(), 0);

    let next = pool.acquire().await.unwrap();

    assert_eq!(opener.opened(), 2);
    assert_eq!(
        opener.peak(),
        1,
        "the discarded connection must be closed before its replacement opens"
    );
    drop(next);
}

#[tokio::test(start_paused = true)]
async fn the_slot_of_a_discarded_connection_goes_to_the_waiting_client() {
    let opener = Opener::default();
    let pool = pool(opener.clone(), 1);

    let assigned = pool.acquire().await.unwrap();
    let waiting = tokio::spawn({
        let pool = pool.clone();
        async move { pool.acquire().await }
    });
    TokioRuntime::new().sleep(Duration::from_millis(10)).await;
    assert_eq!(pool.stats().waiting, 1);

    assigned.discard().await;
    let served = waiting.await.unwrap().unwrap();

    assert_eq!(served.id, 1);
    assert_eq!(opener.opened(), 2);
    assert_eq!(opener.peak(), 1);
}
