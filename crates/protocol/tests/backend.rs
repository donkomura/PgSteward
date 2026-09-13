use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use pgsteward_protocol::backend::{
    EncryptionResponse, ErrorResponse, Severity, encode_authentication_ok,
    encode_encryption_response, encode_error_response, sqlstate,
};
use postgres_protocol::message::backend::{ErrorResponseBody, Message};

fn encoded(encode: impl FnOnce(&mut BytesMut)) -> BytesMut {
    let mut out = BytesMut::new();
    encode(&mut out);
    out
}

fn parse(mut bytes: BytesMut) -> Message {
    let message = Message::parse(&mut bytes)
        .expect("a well-formed backend message")
        .expect("a complete backend message");
    assert!(bytes.is_empty(), "bytes follow the encoded message");
    message
}

fn fields(body: &ErrorResponseBody) -> Vec<(u8, String)> {
    let mut collected = Vec::new();
    let mut fields = body.fields();
    while let Some(field) = fields.next().expect("well-formed error fields") {
        collected.push((
            field.type_(),
            String::from_utf8(field.value_bytes().to_vec()).expect("UTF-8 field value"),
        ));
    }
    collected
}

fn refusal() -> ErrorResponse {
    ErrorResponse::fatal(
        sqlstate::INVALID_AUTHORIZATION_SPECIFICATION,
        "no PostgreSQL user name specified in startup packet",
    )
}

#[test]
fn an_accepted_encryption_request_is_answered_with_a_single_byte() {
    let out = encoded(|out| encode_encryption_response(EncryptionResponse::Accepted, out));
    assert_eq!(&out[..], b"S");
}

#[test]
fn a_refused_encryption_request_is_answered_with_a_single_byte() {
    let out = encoded(|out| encode_encryption_response(EncryptionResponse::Refused, out));
    assert_eq!(&out[..], b"N");
}

#[test]
fn authentication_ok_parses_as_an_authentication_message() {
    let message = parse(encoded(encode_authentication_ok));
    assert!(matches!(message, Message::AuthenticationOk));
}

#[test]
fn an_error_response_carries_both_severity_fields_the_sqlstate_and_the_message() {
    let message = parse(encoded(|out| encode_error_response(&refusal(), out)));
    let Message::ErrorResponse(body) = message else {
        panic!("expected an ErrorResponse");
    };
    assert_eq!(
        fields(&body),
        vec![
            (b'S', "FATAL".to_owned()),
            (b'V', "FATAL".to_owned()),
            (
                b'C',
                sqlstate::INVALID_AUTHORIZATION_SPECIFICATION.to_owned()
            ),
            (
                b'M',
                "no PostgreSQL user name specified in startup packet".to_owned()
            ),
        ]
    );
}

#[test]
fn an_error_response_keeps_the_detail_and_the_hint_after_the_message() {
    let error = ErrorResponse::error(sqlstate::FEATURE_NOT_SUPPORTED, "TLS is not supported yet")
        .with_detail("This node terminates plaintext connections only.")
        .with_hint("Connect with sslmode=disable.");
    let message = parse(encoded(|out| encode_error_response(&error, out)));
    let Message::ErrorResponse(body) = message else {
        panic!("expected an ErrorResponse");
    };
    assert_eq!(
        fields(&body),
        vec![
            (b'S', "ERROR".to_owned()),
            (b'V', "ERROR".to_owned()),
            (b'C', sqlstate::FEATURE_NOT_SUPPORTED.to_owned()),
            (b'M', "TLS is not supported yet".to_owned()),
            (
                b'D',
                "This node terminates plaintext connections only.".to_owned()
            ),
            (b'H', "Connect with sslmode=disable.".to_owned()),
        ]
    );
}

#[test]
fn severity_is_written_as_postgres_spells_it() {
    assert_eq!(Severity::Error.as_str(), "ERROR");
    assert_eq!(Severity::Fatal.as_str(), "FATAL");
}
