use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use pgsteward_protocol::backend::{
    Column, EncryptionResponse, ErrorResponse, Severity, encode_authentication_ok,
    encode_authentication_sasl, encode_authentication_sasl_continue,
    encode_authentication_sasl_final, encode_backend_key_data, encode_command_complete,
    encode_data_row, encode_empty_query_response, encode_encryption_response,
    encode_error_response, encode_parameter_status, encode_ready_for_query, encode_row_description,
    sqlstate,
};
use pgsteward_protocol::message::TransactionStatus;
use pgsteward_protocol::startup::CancelKey;
use postgres_protocol::message::backend::{ErrorResponseBody, Message};

const TEXT_OID: u32 = 25;
const INT8_OID: u32 = 20;

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

#[test]
fn authentication_sasl_lists_every_offered_mechanism() {
    let message = parse(encoded(|out| {
        encode_authentication_sasl(&["SCRAM-SHA-256", "SCRAM-SHA-256-PLUS"], out);
    }));
    let Message::AuthenticationSasl(body) = message else {
        panic!("expected an AuthenticationSASL");
    };
    let offered: Vec<String> = body
        .mechanisms()
        .map(|mechanism| Ok(mechanism.to_owned()))
        .collect()
        .expect("well-formed mechanism names");
    assert_eq!(offered, vec!["SCRAM-SHA-256", "SCRAM-SHA-256-PLUS"]);
}

#[test]
fn authentication_sasl_continue_carries_the_server_first_message() {
    let server_first = "r=clientnonceservernonce,s=c2FsdA==,i=4096";
    let message = parse(encoded(|out| {
        encode_authentication_sasl_continue(server_first, out);
    }));
    let Message::AuthenticationSaslContinue(body) = message else {
        panic!("expected an AuthenticationSASLContinue");
    };
    assert_eq!(body.data(), server_first.as_bytes());
}

#[test]
fn authentication_sasl_final_carries_the_server_signature() {
    let server_final = "v=c2VydmVyc2lnbmF0dXJl";
    let message = parse(encoded(|out| {
        encode_authentication_sasl_final(server_final, out);
    }));
    let Message::AuthenticationSaslFinal(body) = message else {
        panic!("expected an AuthenticationSASLFinal");
    };
    assert_eq!(body.data(), server_final.as_bytes());
}

#[test]
fn invalid_password_is_the_sqlstate_postgres_uses_for_a_failed_password() {
    assert_eq!(sqlstate::INVALID_PASSWORD, "28P01");
}

#[test]
fn a_parameter_status_carries_the_name_and_the_value() {
    let message = parse(encoded(|out| {
        encode_parameter_status("server_version", "16.4", out);
    }));
    let Message::ParameterStatus(body) = message else {
        panic!("expected a ParameterStatus");
    };
    assert_eq!(body.name().expect("a UTF-8 name"), "server_version");
    assert_eq!(body.value().expect("a UTF-8 value"), "16.4");
}

#[test]
fn backend_key_data_carries_the_process_id_and_the_secret_key() {
    let key = CancelKey {
        process_id: 4242,
        secret_key: 987_654_321,
    };
    let message = parse(encoded(|out| encode_backend_key_data(key, out)));
    let Message::BackendKeyData(body) = message else {
        panic!("expected a BackendKeyData");
    };
    assert_eq!(body.process_id(), key.process_id);
    assert_eq!(body.secret_key(), key.secret_key);
}

#[test]
fn ready_for_query_carries_the_transaction_status() {
    for (status, byte) in [
        (TransactionStatus::Idle, b'I'),
        (TransactionStatus::InTransaction, b'T'),
        (TransactionStatus::Failed, b'E'),
    ] {
        let message = parse(encoded(|out| encode_ready_for_query(status, out)));
        let Message::ReadyForQuery(body) = message else {
            panic!("expected a ReadyForQuery");
        };
        assert_eq!(body.status(), byte);
    }
}

#[test]
fn a_row_description_names_every_column_and_asks_for_the_text_format() {
    let columns = [
        Column::text("database"),
        Column::count("granted"),
        Column::count("actual"),
    ];
    let message = parse(encoded(|out| encode_row_description(&columns, out)));
    let Message::RowDescription(body) = message else {
        panic!("expected a RowDescription");
    };
    let described: Vec<(String, u32, i16, i16)> = body
        .fields()
        .map(|field| {
            Ok((
                field.name().to_owned(),
                field.type_oid(),
                field.type_size(),
                field.format(),
            ))
        })
        .collect()
        .expect("well-formed column descriptions");
    assert_eq!(
        described,
        vec![
            ("database".to_owned(), TEXT_OID, -1, 0),
            ("granted".to_owned(), INT8_OID, 8, 0),
            ("actual".to_owned(), INT8_OID, 8, 0),
        ]
    );
}

#[test]
fn a_row_description_leaves_the_columns_without_a_table_of_their_own() {
    let message = parse(encoded(|out| {
        encode_row_description(&[Column::text("database")], out);
    }));
    let Message::RowDescription(body) = message else {
        panic!("expected a RowDescription");
    };
    let field = body
        .fields()
        .next()
        .expect("well-formed column descriptions")
        .expect("one column");
    assert_eq!(field.table_oid(), 0);
    assert_eq!(field.column_id(), 0);
    assert_eq!(field.type_modifier(), -1);
}

#[test]
fn a_data_row_carries_the_values_in_the_order_the_columns_were_described() {
    let message = parse(encoded(|out| {
        encode_data_row(&[Some("app_web"), Some("30"), None], out);
    }));
    let Message::DataRow(body) = message else {
        panic!("expected a DataRow");
    };
    let buffer = body.buffer().to_vec();
    let values: Vec<Option<String>> = body
        .ranges()
        .map(|range| {
            Ok(range.map(|range| {
                String::from_utf8(buffer[range].to_vec()).expect("a UTF-8 text value")
            }))
        })
        .collect()
        .expect("well-formed value ranges");
    assert_eq!(
        values,
        vec![Some("app_web".to_owned()), Some("30".to_owned()), None]
    );
}

#[test]
fn a_command_complete_carries_the_tag() {
    for tag in ["SHOW", "RELOAD", "PAUSE"] {
        let message = parse(encoded(|out| encode_command_complete(tag, out)));
        let Message::CommandComplete(body) = message else {
            panic!("expected a CommandComplete");
        };
        assert_eq!(body.tag().expect("a UTF-8 tag"), tag);
    }
}

#[test]
fn an_empty_query_response_stands_in_for_a_text_that_held_no_statement() {
    let message = parse(encoded(encode_empty_query_response));
    assert!(matches!(message, Message::EmptyQueryResponse));
}
