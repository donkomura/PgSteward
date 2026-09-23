use bytes::BytesMut;
use pgsteward_core::allocation::{InstanceId, ProxyId};
use pgsteward_core::cancel::{CancelRegistry, ProxyTag, forward_cancel};
use pgsteward_core::rt::Net;
use pgsteward_core::rt::tokio_rt::TokioRuntime;
use pgsteward_protocol::framing::decode_startup_frame;
use pgsteward_protocol::startup::{CancelKey, StartupRequest, decode_startup};
use tokio::io::AsyncReadExt;

const MAX_FRAME: usize = 1 << 20;

fn registry(tag: u16) -> CancelRegistry {
    CancelRegistry::new(ProxyTag::new(tag).unwrap())
}

fn backend(process_id: i32) -> CancelKey {
    CancelKey {
        process_id,
        secret_key: 0x5eec,
    }
}

fn shop() -> InstanceId {
    InstanceId::new("shop")
}

#[test]
fn a_key_handed_to_a_client_carries_the_proxy_tag_in_its_high_bits() {
    let registry = registry(7);
    let ticket = registry.issue();

    assert_eq!(ProxyTag::in_key(ticket.key()), ProxyTag::new(7).unwrap());
}

#[test]
fn keys_of_two_proxies_are_told_apart_by_their_tag() {
    let one = registry(1);
    let other = registry(2);

    assert_eq!(
        ProxyTag::in_key(one.issue().key()),
        ProxyTag::new(1).unwrap()
    );
    assert_eq!(
        ProxyTag::in_key(other.issue().key()),
        ProxyTag::new(2).unwrap()
    );
}

#[test]
fn a_tag_that_does_not_fit_the_field_is_refused() {
    assert!(ProxyTag::new(ProxyTag::MAX).is_some());
    assert!(ProxyTag::new(ProxyTag::MAX + 1).is_none());
}

#[test]
fn a_tag_can_be_derived_from_the_proxy_identifier() {
    let proxy = ProxyId::new("127.0.0.1:6432");

    assert_eq!(ProxyTag::of(&proxy), ProxyTag::of(&proxy));
}

#[test]
fn every_client_gets_a_key_of_its_own() {
    let registry = registry(3);

    let first = registry.issue();
    let second = registry.issue();

    assert_ne!(first.key().process_id, second.key().process_id);
    assert_ne!(first.key().secret_key, second.key().secret_key);
}

#[test]
fn a_cancel_request_for_a_key_this_node_never_issued_has_no_target() {
    let registry = registry(1);
    let ticket = registry.issue();
    ticket.aim(shop(), backend(42));

    let stranger = CancelKey {
        process_id: ticket.key().process_id ^ 1,
        secret_key: ticket.key().secret_key,
    };

    assert!(registry.target(stranger).is_none());
}

#[test]
fn a_cancel_request_with_the_wrong_secret_has_no_target() {
    let registry = registry(1);
    let ticket = registry.issue();
    ticket.aim(shop(), backend(42));

    let forged = CancelKey {
        process_id: ticket.key().process_id,
        secret_key: ticket.key().secret_key ^ 1,
    };

    assert!(registry.target(forged).is_none());
}

#[test]
fn a_client_that_is_between_requests_has_no_target() {
    let registry = registry(1);
    let ticket = registry.issue();

    assert!(registry.target(ticket.key()).is_none());
}

#[test]
fn a_cancel_request_names_the_backend_that_is_serving_the_client() {
    let registry = registry(1);
    let ticket = registry.issue();
    ticket.aim(shop(), backend(42));

    let target = registry.target(ticket.key()).expect("the client is served");

    assert_eq!(target.instance(), &shop());
    assert_eq!(target.backend(), backend(42));
}

#[test]
fn a_cancel_request_that_arrives_after_the_boundary_has_no_target() {
    let registry = registry(1);
    let ticket = registry.issue();
    ticket.aim(shop(), backend(42));
    ticket.stand_down();

    assert!(registry.target(ticket.key()).is_none());
}

#[test]
fn the_next_assignment_moves_the_target_to_the_new_backend() {
    let registry = registry(1);
    let ticket = registry.issue();
    ticket.aim(shop(), backend(42));
    ticket.stand_down();
    ticket.aim(shop(), backend(43));

    let target = registry.target(ticket.key()).expect("the client is served");

    assert_eq!(target.backend(), backend(43));
}

#[test]
fn a_key_is_forgotten_when_the_client_leaves() {
    let registry = registry(1);
    let ticket = registry.issue();
    let key = ticket.key();
    ticket.aim(shop(), backend(42));

    drop(ticket);

    assert!(registry.target(key).is_none());
    assert!(registry.is_empty());
}

#[tokio::test]
async fn a_forwarded_cancel_request_carries_the_key_of_the_backend() {
    let rt = TokioRuntime::new();
    let listener = rt.bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let accepting = tokio::spawn(async move { listener.accept().await.unwrap().0 });

    forward_cancel(&rt, &address, backend(42)).await.unwrap();

    let mut stream = accepting.await.unwrap();
    let mut received = Vec::new();
    stream.read_to_end(&mut received).await.unwrap();
    let mut buf = BytesMut::from(&received[..]);
    let body = decode_startup_frame(&mut buf, MAX_FRAME).unwrap().unwrap();

    assert_eq!(
        decode_startup(&body).unwrap(),
        StartupRequest::Cancel(backend(42))
    );
}
