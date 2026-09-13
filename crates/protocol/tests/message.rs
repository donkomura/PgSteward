use pgsteward_protocol::message::{
    BackendTag, FrontendTag, MessageError, TransactionStatus, parse_ready_for_query,
};

#[test]
fn backend_tags_map_to_and_from_bytes() {
    assert_eq!(
        BackendTag::try_from(b'Z').unwrap(),
        BackendTag::ReadyForQuery
    );
    assert_eq!(
        BackendTag::try_from(b'R').unwrap(),
        BackendTag::Authentication
    );
    assert_eq!(
        BackendTag::try_from(b'K').unwrap(),
        BackendTag::BackendKeyData
    );
    assert_eq!(
        BackendTag::try_from(b'S').unwrap(),
        BackendTag::ParameterStatus
    );
    assert_eq!(
        BackendTag::try_from(b'E').unwrap(),
        BackendTag::ErrorResponse
    );
    assert_eq!(
        BackendTag::try_from(b'G').unwrap(),
        BackendTag::CopyInResponse
    );
    assert_eq!(u8::from(BackendTag::ReadyForQuery), b'Z');
    assert!(matches!(
        BackendTag::try_from(b'?'),
        Err(MessageError::UnknownBackendTag(b'?'))
    ));
}

#[test]
fn frontend_tags_map_to_and_from_bytes() {
    assert_eq!(FrontendTag::try_from(b'Q').unwrap(), FrontendTag::Query);
    assert_eq!(FrontendTag::try_from(b'S').unwrap(), FrontendTag::Sync);
    assert_eq!(FrontendTag::try_from(b'P').unwrap(), FrontendTag::Parse);
    assert_eq!(FrontendTag::try_from(b'X').unwrap(), FrontendTag::Terminate);
    assert_eq!(FrontendTag::try_from(b'p').unwrap(), FrontendTag::Password);
    assert_eq!(u8::from(FrontendTag::Sync), b'S');
    assert!(matches!(
        FrontendTag::try_from(b'Z'),
        Err(MessageError::UnknownFrontendTag(b'Z'))
    ));
}

#[test]
fn every_backend_tag_roundtrips() {
    for tag in BackendTag::ALL {
        assert_eq!(BackendTag::try_from(u8::from(tag)).unwrap(), tag);
    }
    for tag in FrontendTag::ALL {
        assert_eq!(FrontendTag::try_from(u8::from(tag)).unwrap(), tag);
    }
}

#[test]
fn ready_for_query_body_is_one_status_byte() {
    assert_eq!(
        parse_ready_for_query(b"I").unwrap(),
        TransactionStatus::Idle
    );
    assert_eq!(
        parse_ready_for_query(b"T").unwrap(),
        TransactionStatus::InTransaction
    );
    assert_eq!(
        parse_ready_for_query(b"E").unwrap(),
        TransactionStatus::Failed
    );
    assert!(matches!(
        parse_ready_for_query(b"X"),
        Err(MessageError::UnknownTransactionStatus(b'X'))
    ));
    assert!(matches!(
        parse_ready_for_query(b""),
        Err(MessageError::MalformedBody {
            tag: BackendTag::ReadyForQuery,
            ..
        })
    ));
    assert!(matches!(
        parse_ready_for_query(b"II"),
        Err(MessageError::MalformedBody {
            tag: BackendTag::ReadyForQuery,
            ..
        })
    ));
}
