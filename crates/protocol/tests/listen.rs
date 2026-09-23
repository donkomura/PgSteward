use bytes::BytesMut;
use pgsteward_protocol::frontend::FrontendError;
use pgsteward_protocol::listen::has_listen;
use pgsteward_protocol::message::FrontendTag;
use pgsteward_protocol::prepared::{PreparedStatements, Verdict};
use postgres_protocol::IsNull;
use postgres_protocol::message::frontend;

const HEADER: usize = 5;

fn query_body(sql: &str) -> Vec<u8> {
    let mut out = BytesMut::new();
    frontend::query(sql, &mut out).expect("a well-formed frame");
    out[HEADER..].to_vec()
}

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

#[test]
fn a_text_whose_statement_starts_with_listen_registers_a_listener() {
    for sql in [
        "LISTEN chan",
        "listen chan",
        "LiStEn chan",
        "   \n\t LISTEN chan",
        "LISTEN chan;",
        "LISTEN \"Chan\"",
    ] {
        assert!(has_listen(sql), "{sql}");
    }
}

#[test]
fn a_listen_that_follows_another_statement_is_found() {
    for sql in [
        "SELECT 1; LISTEN chan",
        "BEGIN; LISTEN chan; COMMIT",
        "SELECT 1;;LISTEN chan",
        "SELECT 'quoted'; LISTEN chan",
    ] {
        assert!(has_listen(sql), "{sql}");
    }
}

#[test]
fn a_comment_in_front_of_a_listen_does_not_hide_it() {
    for sql in [
        "-- wake me up\nLISTEN chan",
        "/* wake me up */ LISTEN chan",
        "/* outer /* inner */ still outer */ LISTEN chan",
        "SELECT 1; -- one\n LISTEN chan",
    ] {
        assert!(has_listen(sql), "{sql}");
    }
}

#[test]
fn a_listen_inside_a_literal_is_text_and_not_a_statement() {
    for sql in [
        "SELECT 'a; LISTEN chan'",
        "SELECT 'it''s; LISTEN chan'",
        "SELECT $$a; LISTEN chan$$",
        "SELECT $tag$a; LISTEN chan$tag$",
        "SELECT \"a; LISTEN chan\"",
        "SELECT \"a\"\"; LISTEN chan\"",
        r"SELECT e'\'; LISTEN chan'",
        r"SELECT E'\'; LISTEN chan'",
    ] {
        assert!(!has_listen(sql), "{sql}");
    }
}

#[test]
fn a_listen_inside_a_comment_is_not_a_statement() {
    for sql in [
        "SELECT 1; -- LISTEN chan",
        "SELECT 1; /* LISTEN chan */",
        "-- LISTEN chan",
        "/* LISTEN chan */",
    ] {
        assert!(!has_listen(sql), "{sql}");
    }
}

#[test]
fn a_word_that_merely_begins_with_listen_is_not_a_listen() {
    for sql in [
        "UNLISTEN chan",
        "UNLISTEN *",
        "NOTIFY chan",
        "NOTIFY chan, 'payload'",
        "LISTENER",
        "listen_to()",
        "SELECT listen FROM t",
        "SELECT * FROM listen",
    ] {
        assert!(!has_listen(sql), "{sql}");
    }
}

#[test]
fn a_text_that_carries_no_statement_registers_nothing() {
    for sql in ["", "   ", ";", ";;", "-- nothing", "/* nothing */"] {
        assert!(!has_listen(sql), "{sql:?}");
    }
}

#[test]
fn a_simple_query_that_listens_is_refused() {
    let mut statements = PreparedStatements::new();

    assert_eq!(
        statements
            .on_frontend(FrontendTag::Query, &query_body("LISTEN chan"))
            .unwrap(),
        Verdict::RejectListen
    );
}

#[test]
fn a_refused_simple_query_leaves_the_window_as_it_found_it() {
    let mut statements = PreparedStatements::new();

    statements
        .on_frontend(FrontendTag::Query, &query_body("LISTEN chan"))
        .unwrap();

    assert_eq!(
        statements
            .on_frontend(FrontendTag::Query, &query_body("SELECT 1"))
            .unwrap(),
        Verdict::Forward
    );
}

#[test]
fn a_parse_that_listens_is_refused_and_its_name_is_never_recorded() {
    let mut statements = PreparedStatements::new();

    assert_eq!(
        statements
            .on_frontend(FrontendTag::Parse, &parse_body("s1", "LISTEN chan"))
            .unwrap(),
        Verdict::RejectListen
    );
    statements.discard_until_sync();

    let bind = bind_body("", "s1");
    assert_eq!(
        statements.on_frontend(FrontendTag::Bind, &bind).unwrap(),
        Verdict::Skip
    );
    assert_eq!(
        statements.on_frontend(FrontendTag::Sync, b"").unwrap(),
        Verdict::Forward
    );
    assert_eq!(
        statements.on_frontend(FrontendTag::Bind, &bind).unwrap(),
        Verdict::Reject("s1")
    );
}

#[test]
fn a_notify_crosses_unchanged() {
    let mut statements = PreparedStatements::new();

    assert_eq!(
        statements
            .on_frontend(FrontendTag::Query, &query_body("NOTIFY chan, 'payload'"))
            .unwrap(),
        Verdict::Forward
    );
    assert_eq!(
        statements
            .on_frontend(FrontendTag::Parse, &parse_body("s1", "NOTIFY chan"))
            .unwrap(),
        Verdict::Forward
    );
    assert_eq!(
        statements
            .on_frontend(FrontendTag::Bind, &bind_body("", "s1"))
            .unwrap(),
        Verdict::Forward
    );
}

#[test]
fn an_unlisten_crosses_unchanged() {
    let mut statements = PreparedStatements::new();

    assert_eq!(
        statements
            .on_frontend(FrontendTag::Query, &query_body("UNLISTEN *"))
            .unwrap(),
        Verdict::Forward
    );
}

#[test]
fn a_listen_that_arrives_while_a_refused_window_is_open_is_dropped_with_the_rest() {
    let mut statements = PreparedStatements::new();
    statements
        .on_frontend(FrontendTag::Bind, &bind_body("", "s1"))
        .unwrap();

    assert_eq!(
        statements
            .on_frontend(FrontendTag::Query, &query_body("LISTEN chan"))
            .unwrap(),
        Verdict::Skip
    );
}

#[test]
fn a_query_whose_text_is_not_terminated_is_malformed() {
    let mut statements = PreparedStatements::new();

    let error = statements
        .on_frontend(FrontendTag::Query, b"LISTEN chan")
        .expect_err("the query text has no terminator");
    assert!(
        matches!(
            error,
            FrontendError::MalformedBody {
                tag: FrontendTag::Query,
                ..
            }
        ),
        "{error:?}"
    );
}

#[test]
fn a_parse_whose_query_text_is_not_terminated_is_malformed() {
    let mut statements = PreparedStatements::new();

    let error = statements
        .on_frontend(FrontendTag::Parse, b"s1\0LISTEN chan")
        .expect_err("the query text has no terminator");
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
