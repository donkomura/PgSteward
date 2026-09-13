use bytes::BytesMut;
use pgsteward_protocol::framing::decode_startup_frame;
use pgsteward_protocol::startup::{
    CancelKey, ProtocolVersion, StartupError, StartupMessage, StartupRequest, decode_startup,
    encode_startup,
};
use proptest::prelude::*;

const SSL_REQUEST: [u8; 8] = [0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f];
const GSSENC_REQUEST: [u8; 8] = [0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x30];
const CANCEL_REQUEST: [u8; 16] = [
    0, 0, 0, 16, 0x04, 0xd2, 0x16, 0x2e, 0x00, 0x00, 0x30, 0x39, 0x7f, 0xff, 0xff, 0xff,
];

fn body_of(wire: &[u8]) -> Vec<u8> {
    let mut buf = BytesMut::from(wire);
    let body = decode_startup_frame(&mut buf, 1 << 20).unwrap().unwrap();
    assert!(buf.is_empty());
    body.to_vec()
}

fn startup_wire(version: [u8; 4], params: &[(&str, &str)]) -> Vec<u8> {
    let mut body = version.to_vec();
    for (name, value) in params {
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        body.extend_from_slice(value.as_bytes());
        body.push(0);
    }
    body.push(0);
    let len = i32::try_from(body.len() + 4).unwrap();
    let mut wire = len.to_be_bytes().to_vec();
    wire.extend_from_slice(&body);
    wire
}

fn decode_startup_message(params: &[(&str, &str)]) -> StartupMessage {
    match decode_startup(&body_of(&startup_wire([0, 3, 0, 0], params))).unwrap() {
        StartupRequest::Startup(message) => message,
        other => panic!("expected StartupMessage, got {other:?}"),
    }
}

#[test]
fn ssl_request_is_recognized_by_its_code() {
    assert_eq!(
        decode_startup(&body_of(&SSL_REQUEST)).unwrap(),
        StartupRequest::Ssl
    );
}

#[test]
fn gssenc_request_is_recognized_by_its_code() {
    assert_eq!(
        decode_startup(&body_of(&GSSENC_REQUEST)).unwrap(),
        StartupRequest::GssEnc
    );
}

#[test]
fn cancel_request_carries_process_id_and_secret_key() {
    assert_eq!(
        decode_startup(&body_of(&CANCEL_REQUEST)).unwrap(),
        StartupRequest::Cancel(CancelKey {
            process_id: 12345,
            secret_key: i32::MAX,
        })
    );
}

#[test]
fn startup_message_keeps_version_and_parameters_in_wire_order() {
    let message = decode_startup_message(&[
        ("user", "app_web"),
        ("database", "shop"),
        ("application_name", "psql"),
        ("client_encoding", "UTF8"),
    ]);
    assert_eq!(message.version(), ProtocolVersion { major: 3, minor: 0 });
    assert_eq!(
        message.parameters(),
        &[
            ("user".to_owned(), "app_web".to_owned()),
            ("database".to_owned(), "shop".to_owned()),
            ("application_name".to_owned(), "psql".to_owned()),
            ("client_encoding".to_owned(), "UTF8".to_owned()),
        ]
    );
    assert_eq!(message.parameter("application_name"), Some("psql"));
    assert_eq!(message.parameter("options"), None);
}

#[test]
fn startup_message_exposes_user_and_database() {
    let message = decode_startup_message(&[("user", "app_web"), ("database", "shop")]);
    assert_eq!(message.user(), Some("app_web"));
    assert_eq!(message.database(), Some("shop"));
}

#[test]
fn startup_message_database_defaults_to_user() {
    let message = decode_startup_message(&[("user", "app_web")]);
    assert_eq!(message.database(), Some("app_web"));
}

#[test]
fn startup_message_without_user_has_neither_user_nor_database() {
    let message = decode_startup_message(&[("database", "shop")]);
    assert_eq!(message.user(), None);
    assert_eq!(message.database(), None);

    let message = decode_startup_message(&[("user", ""), ("database", "shop")]);
    assert_eq!(message.user(), None);
    assert_eq!(message.database(), None);
}

#[test]
fn startup_message_with_empty_parameter_list_is_accepted() {
    let message = decode_startup_message(&[]);
    assert!(message.parameters().is_empty());
    assert_eq!(message.user(), None);
}

#[test]
fn startup_message_with_a_newer_minor_version_is_accepted() {
    let body = body_of(&startup_wire([0, 3, 0, 2], &[("user", "u")]));
    match decode_startup(&body).unwrap() {
        StartupRequest::Startup(message) => {
            assert_eq!(message.version(), ProtocolVersion { major: 3, minor: 2 });
        }
        other => panic!("expected StartupMessage, got {other:?}"),
    }
}

#[test]
fn startup_message_with_another_major_version_is_rejected() {
    let body = body_of(&startup_wire([0, 2, 0, 0], &[("user", "u")]));
    assert_eq!(
        decode_startup(&body),
        Err(StartupError::UnsupportedProtocolVersion(ProtocolVersion {
            major: 2,
            minor: 0
        }))
    );
}

#[test]
fn unknown_request_code_is_rejected() {
    let body = body_of(&[0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x31]);
    assert_eq!(
        decode_startup(&body),
        Err(StartupError::UnknownRequestCode(80_877_105))
    );
}

#[test]
fn body_shorter_than_a_request_code_is_malformed() {
    assert!(matches!(
        decode_startup(&[0, 3, 0]),
        Err(StartupError::Malformed(_))
    ));
}

#[test]
fn ssl_and_gssenc_requests_must_have_exactly_the_code() {
    let mut with_trailing = body_of(&SSL_REQUEST);
    with_trailing.push(0);
    assert!(matches!(
        decode_startup(&with_trailing),
        Err(StartupError::Malformed(_))
    ));
    let mut with_trailing = body_of(&GSSENC_REQUEST);
    with_trailing.push(0);
    assert!(matches!(
        decode_startup(&with_trailing),
        Err(StartupError::Malformed(_))
    ));
}

#[test]
fn cancel_request_must_have_exactly_two_keys() {
    let short = &body_of(&CANCEL_REQUEST)[..8];
    assert!(matches!(
        decode_startup(short),
        Err(StartupError::Malformed(_))
    ));
    let mut long = body_of(&CANCEL_REQUEST);
    long.push(0);
    assert!(matches!(
        decode_startup(&long),
        Err(StartupError::Malformed(_))
    ));
}

#[test]
fn startup_message_without_the_final_terminator_is_malformed() {
    let mut body = body_of(&startup_wire([0, 3, 0, 0], &[("user", "u")]));
    body.pop();
    assert!(matches!(
        decode_startup(&body),
        Err(StartupError::Malformed(_))
    ));
}

#[test]
fn startup_message_with_a_name_but_no_value_is_malformed() {
    let body = [0u8, 3, 0, 0, b'u', b's', b'e', b'r', 0, 0];
    assert!(matches!(
        decode_startup(&body),
        Err(StartupError::Malformed(_))
    ));
}

#[test]
fn startup_message_with_bytes_after_the_terminator_is_malformed() {
    let mut body = body_of(&startup_wire([0, 3, 0, 0], &[("user", "u")]));
    body.extend_from_slice(b"x\0");
    assert!(matches!(
        decode_startup(&body),
        Err(StartupError::Malformed(_))
    ));
}

#[test]
fn startup_message_with_invalid_utf8_is_rejected() {
    let body = [0u8, 3, 0, 0, b'u', b's', b'e', b'r', 0, 0xff, 0xfe, 0, 0];
    assert_eq!(decode_startup(&body), Err(StartupError::InvalidUtf8));
}

#[test]
fn encode_startup_writes_the_fixed_requests_byte_for_byte() {
    let mut out = BytesMut::new();
    encode_startup(&StartupRequest::Ssl, &mut out);
    assert_eq!(&out[..], &SSL_REQUEST);

    let mut out = BytesMut::new();
    encode_startup(&StartupRequest::GssEnc, &mut out);
    assert_eq!(&out[..], &GSSENC_REQUEST);

    let mut out = BytesMut::new();
    encode_startup(
        &StartupRequest::Cancel(CancelKey {
            process_id: 12345,
            secret_key: i32::MAX,
        }),
        &mut out,
    );
    assert_eq!(&out[..], &CANCEL_REQUEST);
}

#[test]
fn encode_startup_writes_a_startup_message_byte_for_byte() {
    let message = StartupMessage::new(
        ProtocolVersion { major: 3, minor: 0 },
        vec![
            ("user".to_owned(), "app_web".to_owned()),
            ("database".to_owned(), "shop".to_owned()),
        ],
    );
    let mut out = BytesMut::new();
    encode_startup(&StartupRequest::Startup(message), &mut out);
    assert_eq!(
        &out[..],
        &startup_wire([0, 3, 0, 0], &[("user", "app_web"), ("database", "shop")])[..]
    );
}

fn arb_parameter() -> impl Strategy<Value = (String, String)> {
    ("[a-zA-Z_.][a-zA-Z0-9_.]{0,15}", "[^\0]{0,32}")
}

proptest! {
    #[test]
    fn startup_messages_roundtrip(
        minor in 0u16..8,
        parameters in prop::collection::vec(arb_parameter(), 0..8),
    ) {
        let message = StartupMessage::new(ProtocolVersion { major: 3, minor }, parameters);
        let mut wire = BytesMut::new();
        encode_startup(&StartupRequest::Startup(message.clone()), &mut wire);
        let body = decode_startup_frame(&mut wire, 1 << 20).unwrap().unwrap();
        prop_assert!(wire.is_empty());
        prop_assert_eq!(decode_startup(&body).unwrap(), StartupRequest::Startup(message));
    }

    #[test]
    fn cancel_requests_roundtrip(process_id in any::<i32>(), secret_key in any::<i32>()) {
        let request = StartupRequest::Cancel(CancelKey { process_id, secret_key });
        let mut wire = BytesMut::new();
        encode_startup(&request, &mut wire);
        let body = decode_startup_frame(&mut wire, 1 << 20).unwrap().unwrap();
        prop_assert_eq!(decode_startup(&body).unwrap(), request);
    }
}
