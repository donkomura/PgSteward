use std::fmt;
use std::str::FromStr;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hmac::{Hmac, KeyInit, Mac};
use rand::Rng;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

pub const MECHANISM: &str = "SCRAM-SHA-256";
pub const DEFAULT_ITERATIONS: u32 = 4096;

const KEY_LEN: usize = 32;
const SALT_LEN: usize = 16;
const NONCE_LEN: usize = 18;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ScramError {
    #[error("malformed SCRAM message: {0}")]
    Malformed(&'static str),
    #[error("channel binding is not supported")]
    ChannelBinding,
    #[error("the client did not echo the nonce the server sent")]
    Nonce,
    #[error("the client proof does not match the stored verifier")]
    Proof,
    #[error("a SCRAM message arrived out of order")]
    OutOfOrder,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum VerifierError {
    #[error(
        "not a {MECHANISM} verifier; expected `{MECHANISM}$<iterations>:<salt>$<stored key>:<server key>`"
    )]
    Mechanism,
    #[error("malformed {MECHANISM} verifier: {0}")]
    Malformed(&'static str),
    #[error("the iteration count of a {MECHANISM} verifier must be a positive number")]
    Iterations,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ScramVerifier {
    salt: Vec<u8>,
    iterations: u32,
    stored_key: [u8; KEY_LEN],
    server_key: [u8; KEY_LEN],
}

impl fmt::Debug for ScramVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScramVerifier")
            .field("iterations", &self.iterations)
            .finish_non_exhaustive()
    }
}

impl ScramVerifier {
    #[must_use]
    pub fn from_password(password: &str, salt: &[u8], iterations: u32) -> Self {
        let prepared = prepare(password);
        let salted = salted_password(prepared.as_bytes(), salt, iterations);
        let client_key = hmac(&salted, b"Client Key");
        Self {
            salt: salt.to_vec(),
            iterations,
            stored_key: Sha256::digest(client_key).into(),
            server_key: hmac(&salted, b"Server Key"),
        }
    }

    #[must_use]
    pub fn mock() -> Self {
        Self::from_password(
            &random_base64(KEY_LEN),
            &random_bytes(SALT_LEN),
            DEFAULT_ITERATIONS,
        )
    }
}

impl FromStr for ScramVerifier {
    type Err = VerifierError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let body = text
            .strip_prefix(MECHANISM)
            .and_then(|rest| rest.strip_prefix('$'))
            .ok_or(VerifierError::Mechanism)?;
        let (parameters, keys) = body.split_once('$').ok_or(VerifierError::Malformed(
            "no `$` between the salt and the keys",
        ))?;
        let (iterations, salt) = parameters.split_once(':').ok_or(VerifierError::Malformed(
            "no `:` between the iterations and the salt",
        ))?;
        let (stored_key, server_key) = keys
            .split_once(':')
            .ok_or(VerifierError::Malformed("no `:` between the two keys"))?;

        let iterations: u32 = iterations.parse().map_err(|_| VerifierError::Iterations)?;
        if iterations == 0 {
            return Err(VerifierError::Iterations);
        }
        let salt = STANDARD
            .decode(salt)
            .map_err(|_| VerifierError::Malformed("the salt is not base64"))?;
        if salt.is_empty() {
            return Err(VerifierError::Malformed("the salt is empty"));
        }
        Ok(Self {
            salt,
            iterations,
            stored_key: key(stored_key, "the stored key is not a base64 SHA-256 digest")?,
            server_key: key(server_key, "the server key is not a base64 SHA-256 digest")?,
        })
    }
}

fn key(text: &str, complaint: &'static str) -> Result<[u8; KEY_LEN], VerifierError> {
    let decoded = STANDARD
        .decode(text)
        .map_err(|_| VerifierError::Malformed(complaint))?;
    decoded
        .try_into()
        .map_err(|_| VerifierError::Malformed(complaint))
}

#[must_use]
pub fn nonce() -> String {
    random_base64(NONCE_LEN)
}

#[derive(Debug)]
pub struct ScramExchange {
    verifier: ScramVerifier,
    server_nonce: String,
    state: State,
}

#[derive(Debug)]
enum State {
    Start,
    AwaitingProof(Challenge),
    Done,
}

#[derive(Debug)]
struct Challenge {
    gs2_header: String,
    client_first_bare: String,
    server_first: String,
    nonce: String,
}

impl ScramExchange {
    #[must_use]
    pub fn new(verifier: ScramVerifier, server_nonce: String) -> Self {
        Self {
            verifier,
            server_nonce,
            state: State::Start,
        }
    }

    pub fn server_first(&mut self, client_first: &[u8]) -> Result<String, ScramError> {
        if !matches!(self.state, State::Start) {
            return Err(ScramError::OutOfOrder);
        }
        let client_first = text(client_first, "the client-first message is not valid UTF-8")?;
        let (gs2_header, bare) = split_gs2_header(client_first)?;
        let client_nonce = attribute(bare, 'r').ok_or(ScramError::Malformed(
            "the client-first message has no nonce",
        ))?;
        let nonce = format!("{client_nonce}{}", self.server_nonce);
        let server_first = format!(
            "r={nonce},s={},i={}",
            STANDARD.encode(&self.verifier.salt),
            self.verifier.iterations
        );
        self.state = State::AwaitingProof(Challenge {
            gs2_header: gs2_header.to_owned(),
            client_first_bare: bare.to_owned(),
            server_first: server_first.clone(),
            nonce,
        });
        Ok(server_first)
    }

    pub fn server_final(&mut self, client_final: &[u8]) -> Result<String, ScramError> {
        let challenge = match std::mem::replace(&mut self.state, State::Done) {
            State::AwaitingProof(challenge) => challenge,
            other => {
                self.state = other;
                return Err(ScramError::OutOfOrder);
            }
        };
        let client_final = text(client_final, "the client-final message is not valid UTF-8")?;
        let (without_proof, proof) =
            client_final
                .rsplit_once(",p=")
                .ok_or(ScramError::Malformed(
                    "the client-final message has no proof",
                ))?;
        let echoed_header = attribute(without_proof, 'c').ok_or(ScramError::Malformed(
            "the client-final message has no channel-binding attribute",
        ))?;
        if echoed_header != STANDARD.encode(&challenge.gs2_header) {
            return Err(ScramError::ChannelBinding);
        }
        if attribute(without_proof, 'r').ok_or(ScramError::Malformed(
            "the client-final message has no nonce",
        ))? != challenge.nonce
        {
            return Err(ScramError::Nonce);
        }
        let auth_message = format!(
            "{},{},{without_proof}",
            challenge.client_first_bare, challenge.server_first
        );
        self.verify_proof(proof, &auth_message)?;
        Ok(format!(
            "v={}",
            STANDARD.encode(hmac(&self.verifier.server_key, auth_message.as_bytes()))
        ))
    }

    fn verify_proof(&self, proof: &str, auth_message: &str) -> Result<(), ScramError> {
        let proof = STANDARD
            .decode(proof)
            .map_err(|_| ScramError::Malformed("the client proof is not base64"))?;
        let proof: [u8; KEY_LEN] = proof.try_into().map_err(|_| ScramError::Proof)?;
        let signature = hmac(&self.verifier.stored_key, auth_message.as_bytes());
        let mut client_key = [0u8; KEY_LEN];
        for (key, (proven, signed)) in client_key.iter_mut().zip(proof.iter().zip(signature)) {
            *key = proven ^ signed;
        }
        let stored: [u8; KEY_LEN] = Sha256::digest(client_key).into();
        if bool::from(stored.ct_eq(&self.verifier.stored_key)) {
            Ok(())
        } else {
            Err(ScramError::Proof)
        }
    }
}

fn text<'a>(bytes: &'a [u8], reason: &'static str) -> Result<&'a str, ScramError> {
    std::str::from_utf8(bytes).map_err(|_| ScramError::Malformed(reason))
}

fn split_gs2_header(client_first: &str) -> Result<(&str, &str), ScramError> {
    let mut commas = client_first.match_indices(',');
    let (flag_end, _) = commas.next().ok_or(ScramError::Malformed(
        "the client-first message has no GS2 header",
    ))?;
    let (header_end, _) = commas
        .next()
        .ok_or(ScramError::Malformed("the GS2 header has no authzid field"))?;
    match &client_first[..flag_end] {
        "n" | "y" => {}
        flag if flag.starts_with("p=") => return Err(ScramError::ChannelBinding),
        _ => {
            return Err(ScramError::Malformed(
                "the GS2 header has an unknown channel-binding flag",
            ));
        }
    }
    Ok(client_first.split_at(header_end + 1))
}

fn attribute(message: &str, key: char) -> Option<&str> {
    message.split(',').find_map(|attribute| {
        let (name, value) = attribute.split_once('=')?;
        (name.parse::<char>().ok()? == key).then_some(value)
    })
}

fn prepare(password: &str) -> String {
    stringprep::saslprep(password)
        .map_or_else(|_| password.to_owned(), std::borrow::Cow::into_owned)
}

fn salted_password(password: &[u8], salt: &[u8], iterations: u32) -> [u8; KEY_LEN] {
    let mut block = Vec::with_capacity(salt.len() + 4);
    block.extend_from_slice(salt);
    block.extend_from_slice(&1u32.to_be_bytes());
    let mut previous = hmac(password, &block);
    let mut salted = previous;
    for _ in 1..iterations {
        previous = hmac(password, &previous);
        for (byte, next) in salted.iter_mut().zip(previous) {
            *byte ^= next;
        }
    }
    salted
}

fn hmac(key: &[u8], data: &[u8]) -> [u8; KEY_LEN] {
    let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(key)
        .expect("HMAC-SHA-256 accepts a key of any length");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

fn random_bytes(len: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; len];
    rand::rng().fill_bytes(&mut bytes);
    bytes
}

fn random_base64(len: usize) -> String {
    STANDARD.encode(random_bytes(len))
}
