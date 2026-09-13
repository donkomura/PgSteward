use pgsteward_protocol::message::{BackendTag, FrontendTag, TransactionStatus};
use pgsteward_protocol::ready::{ReadyEvent, ReadyTracker, TrackerError};
use proptest::prelude::*;

fn rfq(tracker: &mut ReadyTracker, status: u8) -> Option<ReadyEvent> {
    tracker
        .on_backend(BackendTag::ReadyForQuery, &[status])
        .unwrap()
}

#[test]
fn fresh_connection_is_idle_and_releasable() {
    let tracker = ReadyTracker::new();
    assert_eq!(tracker.status(), TransactionStatus::Idle);
    assert!(tracker.may_release());
}

#[test]
fn simple_query_holds_until_ready_for_query_idle() {
    let mut tracker = ReadyTracker::new();
    tracker.on_frontend(FrontendTag::Query);
    assert!(!tracker.may_release());
    assert!(
        tracker
            .on_backend(BackendTag::RowDescription, b"")
            .unwrap()
            .is_none()
    );
    assert!(
        tracker
            .on_backend(BackendTag::DataRow, b"")
            .unwrap()
            .is_none()
    );
    assert!(
        tracker
            .on_backend(BackendTag::CommandComplete, b"")
            .unwrap()
            .is_none()
    );
    assert!(!tracker.may_release());
    assert_eq!(
        rfq(&mut tracker, b'I'),
        Some(ReadyEvent::Ready(TransactionStatus::Idle))
    );
    assert!(tracker.may_release());
}

#[test]
fn transaction_keeps_connection_across_queries_until_commit() {
    let mut tracker = ReadyTracker::new();
    tracker.on_frontend(FrontendTag::Query);
    rfq(&mut tracker, b'T');
    assert_eq!(tracker.status(), TransactionStatus::InTransaction);
    assert!(!tracker.may_release());

    tracker.on_frontend(FrontendTag::Query);
    rfq(&mut tracker, b'T');
    assert!(!tracker.may_release());

    tracker.on_frontend(FrontendTag::Query);
    rfq(&mut tracker, b'I');
    assert!(tracker.may_release());
}

#[test]
fn failed_transaction_still_holds_connection() {
    let mut tracker = ReadyTracker::new();
    tracker.on_frontend(FrontendTag::Query);
    rfq(&mut tracker, b'T');
    tracker.on_frontend(FrontendTag::Query);
    tracker.on_backend(BackendTag::ErrorResponse, b"").unwrap();
    rfq(&mut tracker, b'E');
    assert_eq!(tracker.status(), TransactionStatus::Failed);
    assert!(!tracker.may_release());
    tracker.on_frontend(FrontendTag::Query);
    rfq(&mut tracker, b'I');
    assert!(tracker.may_release());
}

#[test]
fn extended_query_holds_from_parse_until_ready_after_sync() {
    let mut tracker = ReadyTracker::new();
    tracker.on_frontend(FrontendTag::Parse);
    assert!(!tracker.may_release());
    tracker.on_frontend(FrontendTag::Bind);
    tracker.on_frontend(FrontendTag::Describe);
    tracker.on_frontend(FrontendTag::Execute);
    tracker.on_backend(BackendTag::ParseComplete, b"").unwrap();
    tracker.on_backend(BackendTag::BindComplete, b"").unwrap();
    assert!(!tracker.may_release());
    tracker.on_frontend(FrontendTag::Sync);
    assert!(!tracker.may_release());
    tracker
        .on_backend(BackendTag::CommandComplete, b"")
        .unwrap();
    assert!(!tracker.may_release());
    rfq(&mut tracker, b'I');
    assert!(tracker.may_release());
}

#[test]
fn flush_without_sync_never_releases() {
    let mut tracker = ReadyTracker::new();
    tracker.on_frontend(FrontendTag::Parse);
    tracker.on_frontend(FrontendTag::Flush);
    tracker.on_backend(BackendTag::ParseComplete, b"").unwrap();
    assert!(!tracker.may_release());
    assert_eq!(tracker.outstanding(), 0);
    tracker.on_frontend(FrontendTag::Sync);
    rfq(&mut tracker, b'I');
    assert!(tracker.may_release());
}

#[test]
fn pipelined_syncs_are_counted_individually() {
    let mut tracker = ReadyTracker::new();
    tracker.on_frontend(FrontendTag::Query);
    tracker.on_frontend(FrontendTag::Query);
    assert_eq!(tracker.outstanding(), 2);
    rfq(&mut tracker, b'I');
    assert!(!tracker.may_release());
    assert_eq!(tracker.outstanding(), 1);
    rfq(&mut tracker, b'I');
    assert!(tracker.may_release());
}

#[test]
fn function_call_expects_a_ready_for_query() {
    let mut tracker = ReadyTracker::new();
    tracker.on_frontend(FrontendTag::FunctionCall);
    assert_eq!(tracker.outstanding(), 1);
    assert!(!tracker.may_release());
    rfq(&mut tracker, b'I');
    assert!(tracker.may_release());
}

#[test]
fn copy_traffic_does_not_change_outstanding_count() {
    let mut tracker = ReadyTracker::new();
    tracker.on_frontend(FrontendTag::Query);
    tracker.on_backend(BackendTag::CopyInResponse, b"").unwrap();
    tracker.on_frontend(FrontendTag::CopyData);
    tracker.on_frontend(FrontendTag::CopyDone);
    assert_eq!(tracker.outstanding(), 1);
    tracker
        .on_backend(BackendTag::CommandComplete, b"")
        .unwrap();
    rfq(&mut tracker, b'I');
    assert!(tracker.may_release());
}

#[test]
fn unexpected_ready_for_query_is_an_error() {
    let mut tracker = ReadyTracker::new();
    assert!(matches!(
        tracker.on_backend(BackendTag::ReadyForQuery, b"I"),
        Err(TrackerError::UnexpectedReadyForQuery)
    ));
}

#[test]
fn malformed_ready_for_query_is_an_error() {
    let mut tracker = ReadyTracker::new();
    tracker.on_frontend(FrontendTag::Query);
    assert!(matches!(
        tracker.on_backend(BackendTag::ReadyForQuery, b"?"),
        Err(TrackerError::Message(_))
    ));
}

#[test]
fn terminate_marks_the_link_closed() {
    let mut tracker = ReadyTracker::new();
    tracker.on_frontend(FrontendTag::Terminate);
    assert!(tracker.is_terminated());
    assert!(!tracker.may_release());
}

#[test]
fn asynchronous_messages_while_idle_do_not_disturb_state() {
    let mut tracker = ReadyTracker::new();
    tracker.on_backend(BackendTag::NoticeResponse, b"").unwrap();
    tracker
        .on_backend(BackendTag::ParameterStatus, b"")
        .unwrap();
    tracker
        .on_backend(BackendTag::NotificationResponse, b"")
        .unwrap();
    assert!(tracker.may_release());
}

fn arb_frontend() -> impl Strategy<Value = FrontendTag> {
    prop::sample::select(vec![
        FrontendTag::Query,
        FrontendTag::Parse,
        FrontendTag::Bind,
        FrontendTag::Describe,
        FrontendTag::Execute,
        FrontendTag::Close,
        FrontendTag::Flush,
        FrontendTag::Sync,
        FrontendTag::FunctionCall,
    ])
}

fn arb_status() -> impl Strategy<Value = u8> {
    prop::sample::select(vec![b'I', b'T', b'E'])
}

proptest! {
    #[test]
    fn release_only_when_nothing_outstanding_and_idle(
        steps in prop::collection::vec((arb_frontend(), arb_status()), 0..32)
    ) {
        let mut tracker = ReadyTracker::new();
        let mut extended_open = false;
        let mut outstanding = 0u32;
        let mut status = TransactionStatus::Idle;
        for (tag, reply) in steps {
            tracker.on_frontend(tag);
            match tag {
                FrontendTag::Query | FrontendTag::FunctionCall => outstanding += 1,
                FrontendTag::Sync => {
                    outstanding += 1;
                    extended_open = false;
                }
                _ => extended_open = true,
            }
            prop_assert_eq!(tracker.outstanding(), outstanding);
            if outstanding > 0 {
                let event = tracker.on_backend(BackendTag::ReadyForQuery, &[reply]).unwrap();
                outstanding -= 1;
                status = match reply {
                    b'I' => TransactionStatus::Idle,
                    b'T' => TransactionStatus::InTransaction,
                    _ => TransactionStatus::Failed,
                };
                prop_assert_eq!(event, Some(ReadyEvent::Ready(status)));
            }
            let expected = outstanding == 0 && !extended_open && status == TransactionStatus::Idle;
            prop_assert_eq!(tracker.may_release(), expected);
            prop_assert_eq!(tracker.status(), status);
        }
    }
}
