use std::future::{Future, ready};
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pgsteward_core::pool::{CloseServer, OpenServer, Pool, PoolError, PoolLimits};
use pgsteward_core::rt::{Clock, tokio_rt::TokioRuntime};
use pgsteward_core::server::{ConnectError, QueryError, Row, SimpleQuery};

const WAIT_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug)]
struct FakeConnection {
    id: usize,
    live: Arc<AtomicUsize>,
    statements: Arc<Mutex<Vec<String>>>,
    reset_fails: bool,
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
    reset_failures: Arc<AtomicUsize>,
    statements: Arc<Mutex<Vec<String>>>,
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

    fn failing_reset(self, resets: usize) -> Self {
        Self {
            reset_failures: Arc::new(AtomicUsize::new(resets)),
            ..self
        }
    }

    fn statements(&self) -> Vec<String> {
        self.statements
            .lock()
            .expect("statement log poisoned")
            .clone()
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

    fn fail_one_reset(&self) -> bool {
        self.reset_failures
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
            statements: Arc::clone(&self.statements),
            reset_fails: self.fail_one_reset(),
        })
    }
}

impl CloseServer for FakeConnection {
    async fn close(self) {}
}

impl SimpleQuery for FakeConnection {
    fn simple_query(
        &mut self,
        sql: &str,
    ) -> impl Future<Output = Result<Vec<Row>, QueryError>> + Send {
        self.statements
            .lock()
            .expect("statement log poisoned")
            .push(sql.to_owned());
        ready(if self.reset_fails {
            Err(QueryError::Server {
                code: "57P01".to_owned(),
                message: "the fake connection refuses to reset".to_owned(),
            })
        } else {
            Ok(Vec::new())
        })
    }
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
async fn a_client_that_leaves_while_it_waits_gives_up_its_place_in_the_queue() {
    let opener = Opener::default();
    let pool = pool(opener.clone(), 1);
    let held = pool.acquire().await.unwrap();
    let leaving = tokio::spawn({
        let pool = pool.clone();
        async move { pool.acquire().await }
    });
    TokioRuntime::new().sleep(Duration::from_millis(10)).await;
    assert_eq!(pool.stats().waiting, 1);

    leaving.abort();
    let _ = leaving.await;
    TokioRuntime::new().sleep(Duration::from_millis(10)).await;

    assert_eq!(
        pool.stats().waiting,
        0,
        "a client that went away must leave the queue"
    );
    assert_eq!(
        pool.stats().demand(),
        1,
        "only the client holding the connection still asks for one"
    );
    drop(held);
}

#[tokio::test(start_paused = true)]
async fn the_freed_connection_goes_to_the_client_behind_the_one_that_left() {
    let opener = Opener::default();
    let pool = pool(opener.clone(), 1);
    let held = pool.acquire().await.unwrap();
    let leaving = tokio::spawn({
        let pool = pool.clone();
        async move { pool.acquire().await }
    });
    TokioRuntime::new().sleep(Duration::from_millis(10)).await;
    let staying = tokio::spawn({
        let pool = pool.clone();
        async move { pool.acquire().await }
    });
    TokioRuntime::new().sleep(Duration::from_millis(10)).await;
    assert_eq!(pool.stats().waiting, 2);

    leaving.abort();
    let _ = leaving.await;
    TokioRuntime::new().sleep(Duration::from_millis(10)).await;
    assert_eq!(pool.stats().waiting, 1);

    let id = held.id;
    drop(held);
    let served = staying.await.unwrap().unwrap();

    assert_eq!(served.id, id);
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

#[tokio::test(start_paused = true)]
async fn a_raised_grant_hands_a_slot_to_a_waiting_client() {
    let opener = Opener::default();
    let pool = pool(opener.clone(), 1);
    let held = pool.acquire().await.unwrap();
    let waiting = tokio::spawn({
        let pool = pool.clone();
        async move { pool.acquire().await }
    });
    TokioRuntime::new().sleep(Duration::from_millis(10)).await;
    assert_eq!(pool.stats().waiting, 1);

    let closed = pool.converge(2).await;

    let served = waiting.await.unwrap().unwrap();
    assert_eq!(closed, 0);
    assert_eq!(pool.stats().slots, 2);
    assert_eq!(opener.opened(), 2);
    drop(served);
    drop(held);
}

#[tokio::test(start_paused = true)]
async fn a_lowered_grant_closes_an_idle_connection() {
    let opener = Opener::default();
    let pool = pool(opener.clone(), 2);
    let first = pool.acquire().await.unwrap();
    let second = pool.acquire().await.unwrap();
    drop(first);
    drop(second);
    assert_eq!(pool.stats().idle, 2);

    let closed = pool.converge(1).await;

    assert_eq!(closed, 1);
    assert_eq!(pool.stats().idle, 1);
    assert_eq!(pool.stats().actual(), 1);
    assert_eq!(opener.live(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_lowered_grant_does_not_interrupt_a_connection_in_use() {
    let opener = Opener::default();
    let pool = pool(opener.clone(), 2);
    let first = pool.acquire().await.unwrap();
    let second = pool.acquire().await.unwrap();

    let closed = pool.converge(1).await;

    assert_eq!(closed, 0);
    assert_eq!(pool.stats().in_use, 2);
    assert_eq!(opener.live(), 2);
    drop(first);
    drop(second);
}

#[tokio::test(start_paused = true)]
async fn a_connection_returned_over_the_grant_is_closed_rather_than_pooled() {
    let opener = Opener::default();
    let pool = pool(opener.clone(), 2);
    let first = pool.acquire().await.unwrap();
    let second = pool.acquire().await.unwrap();
    pool.converge(1).await;

    drop(first);

    assert_eq!(pool.stats().idle, 0);
    assert_eq!(pool.stats().closing, 1);
    assert_eq!(pool.stats().actual(), 2);
    assert_eq!(opener.live(), 2);

    let closed = pool.converge(1).await;

    assert_eq!(closed, 1);
    assert_eq!(pool.stats().closing, 0);
    assert_eq!(pool.stats().actual(), 1);
    assert_eq!(opener.live(), 1);
    drop(second);
}

#[tokio::test(start_paused = true)]
async fn a_grant_of_zero_leaves_no_slot_for_a_new_client() {
    let opener = Opener::default();
    let pool = pool(opener.clone(), 1);
    let held = pool.acquire().await.unwrap();
    pool.converge(0).await;

    drop(held);
    assert_eq!(pool.stats().closing, 1);

    let error = pool.acquire().await.unwrap_err();

    assert!(matches!(error, PoolError::WaitTimeout { .. }), "{error:?}");
    assert_eq!(opener.attempts(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_retired_connection_is_closed_before_its_replacement_opens() {
    let opener = Opener::default();
    let pool = pool(opener.clone(), 1);
    let held = pool.acquire().await.unwrap();
    pool.converge(0).await;
    drop(held);

    let closed = pool.converge(1).await;

    assert_eq!(closed, 1);
    let next = pool.acquire().await.unwrap();
    assert_eq!(opener.opened(), 2);
    assert_eq!(opener.peak(), 1);
    drop(next);
}

#[tokio::test(start_paused = true)]
async fn converging_to_the_grant_the_pool_already_holds_moves_nothing() {
    let opener = Opener::default();
    let pool = pool(opener.clone(), 2);
    let assigned = pool.acquire().await.unwrap();
    let before = pool.stats();

    let closed = pool.converge(2).await;

    assert_eq!(closed, 0);
    assert_eq!(pool.stats(), before);
    assert_eq!(opener.live(), 1);
    drop(assigned);
}

#[tokio::test(start_paused = true)]
async fn a_released_connection_is_reset_before_it_goes_back_to_the_pool() {
    let opener = Opener::default();
    let pool = pool(opener.clone(), 1);

    let assigned = pool.acquire().await.unwrap();
    let id = assigned.id;
    assigned.release().await;

    assert_eq!(opener.statements(), vec!["DISCARD ALL".to_owned()]);
    assert_eq!(pool.stats().idle, 1);

    let next = pool.acquire().await.unwrap();

    assert_eq!(next.id, id);
    assert_eq!(opener.opened(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_connection_whose_reset_fails_is_discarded_and_its_slot_kept() {
    let opener = Opener::default().failing_reset(1);
    let pool = pool(opener.clone(), 1);

    let assigned = pool.acquire().await.unwrap();
    assigned.release().await;

    assert_eq!(pool.stats().idle, 0);
    assert_eq!(pool.stats().actual(), 0);
    assert_eq!(pool.stats().slots, 1);
    assert_eq!(opener.live(), 0);

    let next = pool.acquire().await.unwrap();

    assert_eq!(next.id, 1);
    assert_eq!(opener.opened(), 2);
    assert_eq!(
        opener.peak(),
        1,
        "the connection that failed to reset must be closed before its replacement opens"
    );
}

#[tokio::test(start_paused = true)]
async fn the_slot_of_a_connection_whose_reset_fails_goes_to_the_waiting_client() {
    let opener = Opener::default().failing_reset(1);
    let pool = pool(opener.clone(), 1);

    let assigned = pool.acquire().await.unwrap();
    let waiting = tokio::spawn({
        let pool = pool.clone();
        async move { pool.acquire().await }
    });
    TokioRuntime::new().sleep(Duration::from_millis(10)).await;
    assert_eq!(pool.stats().waiting, 1);

    assigned.release().await;
    let served = waiting.await.unwrap().unwrap();

    assert_eq!(
        served.id, 1,
        "a client must never be handed the connection whose reset failed"
    );
    assert_eq!(opener.peak(), 1);
}

#[tokio::test(start_paused = true)]
async fn the_pool_counts_every_connection_it_opens() {
    let opener = Opener::default();
    let pool = pool(opener.clone(), 1);
    assert_eq!(pool.stats().opened, 0);

    drop(pool.acquire().await.unwrap());
    drop(pool.acquire().await.unwrap());
    assert_eq!(
        pool.stats().opened,
        1,
        "a reused connection is not a new one"
    );

    pool.acquire().await.unwrap().discard().await;
    let replacement = pool.acquire().await.unwrap();

    assert_eq!(pool.stats().opened, 2);
    assert_eq!(pool.stats().opened, opener.opened() as u64);
    drop(replacement);
}

#[tokio::test(start_paused = true)]
async fn an_open_the_instance_refused_is_not_counted() {
    let opener = Opener::refusing(1);
    let pool = pool(opener.clone(), 1);

    assert!(pool.acquire().await.is_err());

    assert_eq!(opener.attempts(), 1);
    assert_eq!(pool.stats().opened, 0);
}
