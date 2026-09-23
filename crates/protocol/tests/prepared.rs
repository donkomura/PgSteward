use bytes::BytesMut;
use pgsteward_protocol::frontend::FrontendError;
use pgsteward_protocol::message::FrontendTag;
use pgsteward_protocol::prepared::{PreparedStatements, Verdict};
use postgres_protocol::IsNull;
use postgres_protocol::message::frontend;

const HEADER: usize = 5;

fn parse_body(statement: &str, query: &str) -> Vec<u8> {
    let mut out = BytesMut::new();
    frontend::parse(statement, query, std::iter::empty(), &mut out).expect("a well-formed frame");
    out[HEADER..].to_vec()
}

fn bind_body(portal: &str, statement: &str) -> Vec<u8> {
    let mut out = BytesMut::new();
    let encoded = frontend::bind(
        portal,
        statement,
        std::iter::empty::<i16>(),
        std::iter::empty::<i32>(),
        |_: i32, _: &mut BytesMut| Ok::<_, Box<dyn std::error::Error + Sync + Send>>(IsNull::Yes),
        std::iter::empty::<i16>(),
        &mut out,
    );
    assert!(encoded.is_ok(), "a well-formed frame");
    out[HEADER..].to_vec()
}

fn close_statement_body(statement: &str) -> Vec<u8> {
    let mut out = BytesMut::new();
    frontend::close(b'S', statement, &mut out).expect("a well-formed frame");
    out[HEADER..].to_vec()
}

fn close_portal_body(portal: &str) -> Vec<u8> {
    let mut out = BytesMut::new();
    frontend::close(b'P', portal, &mut out).expect("a well-formed frame");
    out[HEADER..].to_vec()
}

fn parse(statements: &mut PreparedStatements, name: &str) {
    let body = parse_body(name, "SELECT 1");
    let verdict = statements
        .on_frontend(FrontendTag::Parse, &body)
        .expect("a well-formed Parse");
    assert_eq!(verdict, Verdict::Forward);
}

#[test]
fn a_bind_to_a_statement_parsed_on_this_connection_is_forwarded() {
    let mut statements = PreparedStatements::new();
    parse(&mut statements, "s1");

    let body = bind_body("", "s1");
    assert_eq!(
        statements.on_frontend(FrontendTag::Bind, &body).unwrap(),
        Verdict::Forward
    );
}

#[test]
fn a_bind_to_a_statement_this_connection_never_parsed_is_rejected_by_name() {
    let mut statements = PreparedStatements::new();

    let body = bind_body("", "s1");
    assert_eq!(
        statements.on_frontend(FrontendTag::Bind, &body).unwrap(),
        Verdict::Reject("s1")
    );
}

#[test]
fn the_unnamed_statement_follows_the_same_rule() {
    let mut statements = PreparedStatements::new();

    let body = bind_body("", "");
    assert_eq!(
        statements.on_frontend(FrontendTag::Bind, &body).unwrap(),
        Verdict::Reject("")
    );

    let mut statements = PreparedStatements::new();
    parse(&mut statements, "");
    let body = bind_body("", "");
    assert_eq!(
        statements.on_frontend(FrontendTag::Bind, &body).unwrap(),
        Verdict::Forward
    );
}

#[test]
fn everything_between_a_rejection_and_the_sync_is_dropped() {
    let mut statements = PreparedStatements::new();
    let bind = bind_body("", "s1");
    assert_eq!(
        statements.on_frontend(FrontendTag::Bind, &bind).unwrap(),
        Verdict::Reject("s1")
    );

    for (tag, body) in [
        (FrontendTag::Describe, b"P\0".as_slice()),
        (FrontendTag::Execute, b"\0\0\0\0\0".as_slice()),
        (FrontendTag::Flush, b"".as_slice()),
        (FrontendTag::Query, b"SELECT 1\0".as_slice()),
        (FrontendTag::Bind, bind.as_slice()),
    ] {
        assert_eq!(
            statements.on_frontend(tag, body).unwrap(),
            Verdict::Skip,
            "{tag:?} arrived after a rejection"
        );
    }
}

#[test]
fn the_sync_that_closes_a_rejected_window_reaches_the_server() {
    let mut statements = PreparedStatements::new();
    let bind = bind_body("", "s1");
    statements.on_frontend(FrontendTag::Bind, &bind).unwrap();

    assert_eq!(
        statements.on_frontend(FrontendTag::Sync, b"").unwrap(),
        Verdict::Forward
    );

    parse(&mut statements, "s1");
    assert_eq!(
        statements.on_frontend(FrontendTag::Bind, &bind).unwrap(),
        Verdict::Forward
    );
}

#[test]
fn a_closed_statement_is_no_longer_known() {
    let mut statements = PreparedStatements::new();
    parse(&mut statements, "s1");

    let body = close_statement_body("s1");
    assert_eq!(
        statements.on_frontend(FrontendTag::Close, &body).unwrap(),
        Verdict::Forward
    );

    let body = bind_body("", "s1");
    assert_eq!(
        statements.on_frontend(FrontendTag::Bind, &body).unwrap(),
        Verdict::Reject("s1")
    );
}

#[test]
fn closing_a_portal_leaves_the_statement_of_the_same_name_known() {
    let mut statements = PreparedStatements::new();
    parse(&mut statements, "s1");

    let body = close_portal_body("s1");
    assert_eq!(
        statements.on_frontend(FrontendTag::Close, &body).unwrap(),
        Verdict::Forward
    );

    let body = bind_body("", "s1");
    assert_eq!(
        statements.on_frontend(FrontendTag::Bind, &body).unwrap(),
        Verdict::Forward
    );
}

#[test]
fn a_bind_whose_statement_name_is_not_terminated_is_malformed() {
    let mut statements = PreparedStatements::new();

    let error = statements
        .on_frontend(FrontendTag::Bind, b"\0s1")
        .expect_err("the statement name has no terminator");
    assert!(
        matches!(
            error,
            FrontendError::MalformedBody {
                tag: FrontendTag::Bind,
                ..
            }
        ),
        "{error:?}"
    );
}

#[test]
fn a_parse_whose_statement_name_is_not_terminated_is_malformed() {
    let mut statements = PreparedStatements::new();

    let error = statements
        .on_frontend(FrontendTag::Parse, b"s1")
        .expect_err("the statement name has no terminator");
    assert!(
        matches!(
            error,
            FrontendError::MalformedBody {
                tag: FrontendTag::Parse,
                ..
            }
        ),
        "{error:?}"
    );
}

#[test]
fn a_close_with_no_target_is_malformed() {
    let mut statements = PreparedStatements::new();

    let error = statements
        .on_frontend(FrontendTag::Close, b"")
        .expect_err("the target is missing");
    assert!(
        matches!(
            error,
            FrontendError::MalformedBody {
                tag: FrontendTag::Close,
                ..
            }
        ),
        "{error:?}"
    );
}

#[test]
fn messages_that_name_no_statement_are_forwarded_unchanged() {
    let mut statements = PreparedStatements::new();

    for (tag, body) in [
        (FrontendTag::Query, b"SELECT 1\0".as_slice()),
        (FrontendTag::Execute, b"\0\0\0\0\0".as_slice()),
        (FrontendTag::Flush, b"".as_slice()),
        (FrontendTag::Sync, b"".as_slice()),
        (FrontendTag::CopyData, b"row\n".as_slice()),
        (FrontendTag::Terminate, b"".as_slice()),
    ] {
        assert_eq!(
            statements.on_frontend(tag, body).unwrap(),
            Verdict::Forward,
            "{tag:?}"
        );
    }
}
