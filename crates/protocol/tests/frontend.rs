use bytes::{BufMut, BytesMut};
use pgsteward_protocol::frontend::{FrontendError, decode_sasl_initial_response};
use pgsteward_protocol::message::FrontendTag;
use postgres_protocol::message::frontend;

fn body_of_a_real_client(mechanism: &str, data: &[u8]) -> Vec<u8> {
    let mut out = BytesMut::new();
    frontend::sasl_initial_response(mechanism, data, &mut out).expect("a well-formed frame");
    out[5..].to_vec()
}

#[test]
fn a_sasl_initial_response_carries_the_mechanism_and_the_client_first_message() {
    let client_first = b"n,,n=,r=clientnonce";
    let body = body_of_a_real_client("SCRAM-SHA-256", client_first);

    let decoded = decode_sasl_initial_response(&body).expect("a well-formed body");
    assert_eq!(decoded.mechanism, "SCRAM-SHA-256");
    assert_eq!(decoded.data, client_first);
}

#[test]
fn a_sasl_initial_response_without_data_declares_a_length_of_minus_one() {
    let mut body = BytesMut::new();
    body.put_slice(b"SCRAM-SHA-256\0");
    body.put_i32(-1);

    let decoded = decode_sasl_initial_response(&body).expect("a well-formed body");
    assert_eq!(decoded.mechanism, "SCRAM-SHA-256");
    assert!(decoded.data.is_empty());
}

#[test]
fn a_mechanism_name_without_a_terminator_is_malformed() {
    let error = decode_sasl_initial_response(b"SCRAM-SHA-256").expect_err("no terminator");
    assert!(
        matches!(
            error,
            FrontendError::MalformedBody {
                tag: FrontendTag::Password,
                ..
            }
        ),
        "{error:?}"
    );
}

#[test]
fn a_truncated_length_is_malformed() {
    let error = decode_sasl_initial_response(b"SCRAM-SHA-256\0\0\0").expect_err("no length");
    assert!(
        matches!(error, FrontendError::MalformedBody { .. }),
        "{error:?}"
    );
}

#[test]
fn a_declared_length_that_disagrees_with_the_data_is_malformed() {
    let mut body = BytesMut::new();
    body.put_slice(b"SCRAM-SHA-256\0");
    body.put_i32(64);
    body.put_slice(b"n,,n=,r=clientnonce");

    let error = decode_sasl_initial_response(&body).expect_err("the length disagrees");
    assert!(
        matches!(error, FrontendError::MalformedBody { .. }),
        "{error:?}"
    );
}

#[test]
fn a_mechanism_name_that_is_not_utf8_is_malformed() {
    let mut body = BytesMut::new();
    body.put_slice(&[0xff, 0xfe, 0]);
    body.put_i32(0);

    let error = decode_sasl_initial_response(&body).expect_err("not UTF-8");
    assert!(
        matches!(error, FrontendError::MalformedBody { .. }),
        "{error:?}"
    );
}
