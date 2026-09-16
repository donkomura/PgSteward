use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use pgsteward_core::scram::{
    DEFAULT_ITERATIONS, ScramError, ScramExchange, ScramVerifier, VerifierError,
};
use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256};

const SALT: &[u8] = b"pgsteward-salt-1";
const SERVER_NONCE: &str = "3rfcNHYJY1ZVvWVs7j";
// The password "pencil", the salt and the iteration count are the SCRAM-SHA-256 test vector of
// RFC 7677 section 3 (https://datatracker.ietf.org/doc/html/rfc7677#section-3), written the way
// PostgreSQL stores a verifier in `pg_authid.rolpassword`.
const PENCIL_SALT: &str = "W22ZaJ0SNY7soEsUEjb6gQ==";
const PENCIL_VERIFIER: &str = "SCRAM-SHA-256$4096:W22ZaJ0SNY7soEsUEjb6gQ==$\
WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=:wfPLwcE6nTWhTAmQ7tl2KeoiWGPlZqQxSrmfPwDl2dU=";

fn exchange(password: &str) -> ScramExchange {
    ScramExchange::new(
        ScramVerifier::from_password(password, SALT, DEFAULT_ITERATIONS),
        SERVER_NONCE.to_owned(),
    )
}

fn client(password: &str) -> ScramSha256 {
    ScramSha256::new(password.as_bytes(), ChannelBinding::unsupported())
}

fn client_nonce(client_first: &[u8]) -> String {
    let client_first = std::str::from_utf8(client_first).expect("a UTF-8 client-first message");
    client_first
        .rsplit_once(",r=")
        .expect("a client nonce")
        .1
        .to_owned()
}

#[test]
fn a_full_exchange_proves_the_client_to_the_server_and_the_server_to_the_client() {
    let mut client = client("secret");
    let mut exchange = exchange("secret");

    let server_first = exchange
        .server_first(client.message())
        .expect("a well-formed client-first message");
    client
        .update(server_first.as_bytes())
        .expect("a well-formed server-first message");
    let server_final = exchange
        .server_final(client.message())
        .expect("the client proof matches");

    client
        .finish(server_final.as_bytes())
        .expect("the server signature matches");
}

#[test]
fn the_server_first_message_echoes_the_client_nonce_and_names_the_salt_and_the_iterations() {
    let client = client("secret");
    let nonce = client_nonce(client.message());

    let server_first = exchange("secret")
        .server_first(client.message())
        .expect("a well-formed client-first message");

    assert_eq!(
        server_first,
        format!(
            "r={nonce}{SERVER_NONCE},s={},i={DEFAULT_ITERATIONS}",
            STANDARD.encode(SALT)
        )
    );
}

#[test]
fn a_password_that_does_not_match_the_verifier_fails_the_proof() {
    let mut client = client("wrong");
    let mut exchange = exchange("secret");
    let server_first = exchange.server_first(client.message()).unwrap();
    client.update(server_first.as_bytes()).unwrap();

    let error = exchange
        .server_final(client.message())
        .expect_err("the proof is computed from another password");

    assert!(matches!(error, ScramError::Proof), "{error:?}");
}

#[test]
fn a_password_is_prepared_with_saslprep_before_it_is_salted() {
    let mut client = client("pa\u{00ad}ssword");
    let mut exchange = exchange("password");
    let server_first = exchange.server_first(client.message()).unwrap();
    client.update(server_first.as_bytes()).unwrap();

    exchange
        .server_final(client.message())
        .expect("SASLprep maps the soft hyphen to nothing on both sides");
}

#[test]
fn a_client_that_asks_for_channel_binding_is_refused() {
    let client_first = b"p=tls-server-end-point,,n=,r=clientnonce";

    let error = exchange("secret")
        .server_first(client_first)
        .expect_err("this node offers SCRAM-SHA-256 without channel binding");

    assert!(matches!(error, ScramError::ChannelBinding), "{error:?}");
}

#[test]
fn a_client_that_believes_the_server_has_no_channel_binding_is_accepted() {
    let client_first = b"y,,n=,r=clientnonce";

    let server_first = exchange("secret")
        .server_first(client_first)
        .expect("y is the header of a client that found no -PLUS mechanism");

    assert!(server_first.starts_with("r=clientnonce"), "{server_first}");
}

#[test]
fn a_client_first_message_without_a_nonce_is_malformed() {
    let error = exchange("secret")
        .server_first(b"n,,n=")
        .expect_err("no client nonce");

    assert!(matches!(error, ScramError::Malformed(_)), "{error:?}");
}

#[test]
fn a_client_final_message_that_does_not_echo_the_server_nonce_is_refused() {
    let mut exchange = exchange("secret");
    exchange.server_first(b"n,,n=,r=clientnonce").unwrap();

    let error = exchange
        .server_final(b"c=biws,r=clientnonce,p=cHJvb2Y=")
        .expect_err("the nonce is not the one the server sent");

    assert!(matches!(error, ScramError::Nonce), "{error:?}");
}

#[test]
fn a_client_final_message_whose_header_was_altered_is_refused() {
    let mut exchange = exchange("secret");
    exchange.server_first(b"n,,n=,r=clientnonce").unwrap();
    let echoed = format!("clientnonce{SERVER_NONCE}");

    let error = exchange
        .server_final(format!("c=eSws,r={echoed},p=cHJvb2Y=").as_bytes())
        .expect_err("the client echoed a header it did not send");

    assert!(matches!(error, ScramError::ChannelBinding), "{error:?}");
}

#[test]
fn a_client_final_message_before_a_client_first_message_is_out_of_order() {
    let error = exchange("secret")
        .server_final(b"c=biws,r=clientnonce,p=cHJvb2Y=")
        .expect_err("nothing has been proved yet");

    assert!(matches!(error, ScramError::OutOfOrder), "{error:?}");
}

#[test]
fn a_repeated_client_first_message_is_out_of_order() {
    let mut exchange = exchange("secret");
    exchange.server_first(b"n,,n=,r=clientnonce").unwrap();

    let error = exchange
        .server_first(b"n,,n=,r=clientnonce")
        .expect_err("the exchange has already moved on");

    assert!(matches!(error, ScramError::OutOfOrder), "{error:?}");
}

#[test]
fn a_verifier_never_prints_the_keys_it_holds() {
    let printed = format!(
        "{:?}",
        ScramVerifier::from_password("secret", SALT, DEFAULT_ITERATIONS)
    );

    assert!(printed.contains("ScramVerifier"), "{printed}");
    assert!(!printed.contains("secret"), "{printed}");
    assert!(!printed.contains("stored_key"), "{printed}");
}

#[test]
fn a_mock_verifier_lets_the_exchange_run_to_its_end_and_then_fails_the_proof() {
    let mut client = client("secret");
    let mut exchange = ScramExchange::new(ScramVerifier::mock(), SERVER_NONCE.to_owned());
    let server_first = exchange.server_first(client.message()).unwrap();
    client.update(server_first.as_bytes()).unwrap();

    let error = exchange
        .server_final(client.message())
        .expect_err("no password matches a mock verifier");

    assert!(matches!(error, ScramError::Proof), "{error:?}");
}

#[test]
fn no_two_mock_verifiers_are_alike() {
    assert_ne!(ScramVerifier::mock(), ScramVerifier::mock());
}

#[test]
fn a_verifier_read_from_the_text_postgres_stores_holds_what_the_password_would_have_made() {
    let salt = STANDARD.decode(PENCIL_SALT).unwrap();

    let parsed: ScramVerifier = PENCIL_VERIFIER.parse().expect("a well-formed verifier");

    assert_eq!(
        parsed,
        ScramVerifier::from_password("pencil", &salt, DEFAULT_ITERATIONS)
    );
}

#[test]
fn a_verifier_read_from_text_proves_a_client_that_knows_the_password() {
    let mut client = client("pencil");
    let mut exchange =
        ScramExchange::new(PENCIL_VERIFIER.parse().unwrap(), SERVER_NONCE.to_owned());

    let server_first = exchange.server_first(client.message()).unwrap();
    client.update(server_first.as_bytes()).unwrap();
    let server_final = exchange
        .server_final(client.message())
        .expect("the client proof matches");

    client
        .finish(server_final.as_bytes())
        .expect("the server signature matches");
}

#[test]
fn a_verifier_text_that_names_another_mechanism_is_refused() {
    let error = "md5be86a79bf2043622d58d5453c47d4860"
        .parse::<ScramVerifier>()
        .expect_err("this node only reads SCRAM-SHA-256 verifiers");

    assert!(matches!(error, VerifierError::Mechanism), "{error:?}");
}

#[test]
fn a_verifier_text_that_is_missing_a_part_is_refused() {
    let without_keys = PENCIL_VERIFIER.split_once('$').unwrap().0.to_owned() + "$4096:c2FsdA==";

    let error = without_keys
        .parse::<ScramVerifier>()
        .expect_err("a verifier names a salt, a stored key and a server key");

    assert!(matches!(error, VerifierError::Malformed(_)), "{error:?}");
}

#[test]
fn a_verifier_text_whose_iteration_count_is_not_a_positive_number_is_refused() {
    for iterations in ["0", "-1", "many"] {
        let text = PENCIL_VERIFIER.replace("$4096:", &format!("${iterations}:"));

        let error = text
            .parse::<ScramVerifier>()
            .expect_err("the iteration count is salted into the password");

        assert!(matches!(error, VerifierError::Iterations), "{error:?}");
    }
}

#[test]
fn a_verifier_text_whose_salt_is_not_base64_is_refused() {
    let text = PENCIL_VERIFIER.replace(PENCIL_SALT, "not base64");

    let error = text
        .parse::<ScramVerifier>()
        .expect_err("the salt is sent to the client as it was stored");

    assert!(matches!(error, VerifierError::Malformed(_)), "{error:?}");
}

#[test]
fn a_verifier_text_whose_keys_are_not_the_length_of_a_sha_256_digest_is_refused() {
    let text = PENCIL_VERIFIER.replace("WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=", "c2hvcnQ=");

    let error = text
        .parse::<ScramVerifier>()
        .expect_err("a stored key is a SHA-256 digest");

    assert!(matches!(error, VerifierError::Malformed(_)), "{error:?}");
}

#[test]
fn a_verifier_read_from_text_never_prints_the_keys_it_holds() {
    let printed = format!("{:?}", PENCIL_VERIFIER.parse::<ScramVerifier>().unwrap());

    assert!(printed.contains("ScramVerifier"), "{printed}");
    assert!(!printed.contains("WG5d8oPm"), "{printed}");
}
