use std::fmt;

use bytes::{BufMut, BytesMut};

use crate::framing::encode_startup_frame;

const SSL_REQUEST_CODE: i32 = 80_877_103;
const GSSENC_REQUEST_CODE: i32 = 80_877_104;
const CANCEL_REQUEST_CODE: i32 = 80_877_102;
const SPECIAL_REQUEST_MAJOR: u16 = 1234;
const CODE_LEN: usize = 4;
const CANCEL_KEY_LEN: usize = 8;

pub const SUPPORTED_MAJOR: u16 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProtocolVersion {
    pub major: u16,
    pub minor: u16,
}

impl ProtocolVersion {
    pub const V3_0: Self = Self { major: 3, minor: 0 };

    fn from_code(code: [u8; CODE_LEN]) -> Self {
        let [major_hi, major_lo, minor_hi, minor_lo] = code;
        Self {
            major: u16::from_be_bytes([major_hi, major_lo]),
            minor: u16::from_be_bytes([minor_hi, minor_lo]),
        }
    }

    fn to_code(self) -> [u8; CODE_LEN] {
        let [major_hi, major_lo] = self.major.to_be_bytes();
        let [minor_hi, minor_lo] = self.minor.to_be_bytes();
        [major_hi, major_lo, minor_hi, minor_lo]
    }
}

impl fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CancelKey {
    pub process_id: i32,
    pub secret_key: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupMessage {
    version: ProtocolVersion,
    parameters: Vec<(String, String)>,
}

impl StartupMessage {
    #[must_use]
    pub fn new(version: ProtocolVersion, parameters: Vec<(String, String)>) -> Self {
        Self {
            version,
            parameters,
        }
    }

    #[must_use]
    pub fn version(&self) -> ProtocolVersion {
        self.version
    }

    #[must_use]
    pub fn parameters(&self) -> &[(String, String)] {
        &self.parameters
    }

    #[must_use]
    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.parameters
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    #[must_use]
    pub fn user(&self) -> Option<&str> {
        self.parameter("user").filter(|user| !user.is_empty())
    }

    #[must_use]
    pub fn database(&self) -> Option<&str> {
        let user = self.user()?;
        Some(
            self.parameter("database")
                .filter(|database| !database.is_empty())
                .unwrap_or(user),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupRequest {
    Ssl,
    GssEnc,
    Cancel(CancelKey),
    Startup(StartupMessage),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StartupError {
    #[error("unknown startup request code {0}")]
    UnknownRequestCode(i32),
    #[error("unsupported protocol version {0}; only major version {SUPPORTED_MAJOR} is supported")]
    UnsupportedProtocolVersion(ProtocolVersion),
    #[error("malformed startup packet: {0}")]
    Malformed(&'static str),
    #[error("startup parameter is not valid UTF-8")]
    InvalidUtf8,
}

pub fn decode_startup(body: &[u8]) -> Result<StartupRequest, StartupError> {
    let (code, rest) = body
        .split_first_chunk::<CODE_LEN>()
        .ok_or(StartupError::Malformed("shorter than a request code"))?;
    match i32::from_be_bytes(*code) {
        SSL_REQUEST_CODE => fixed_request(
            rest,
            StartupRequest::Ssl,
            "SSLRequest carries bytes after the request code",
        ),
        GSSENC_REQUEST_CODE => fixed_request(
            rest,
            StartupRequest::GssEnc,
            "GSSENCRequest carries bytes after the request code",
        ),
        CANCEL_REQUEST_CODE => decode_cancel_key(rest).map(StartupRequest::Cancel),
        other => {
            let version = ProtocolVersion::from_code(*code);
            if version.major == SPECIAL_REQUEST_MAJOR {
                return Err(StartupError::UnknownRequestCode(other));
            }
            if version.major != SUPPORTED_MAJOR {
                return Err(StartupError::UnsupportedProtocolVersion(version));
            }
            let parameters = decode_parameters(rest)?;
            Ok(StartupRequest::Startup(StartupMessage::new(
                version, parameters,
            )))
        }
    }
}

pub fn encode_startup(request: &StartupRequest, out: &mut BytesMut) {
    let mut body = BytesMut::new();
    match request {
        StartupRequest::Ssl => body.put_i32(SSL_REQUEST_CODE),
        StartupRequest::GssEnc => body.put_i32(GSSENC_REQUEST_CODE),
        StartupRequest::Cancel(key) => {
            body.put_i32(CANCEL_REQUEST_CODE);
            body.put_i32(key.process_id);
            body.put_i32(key.secret_key);
        }
        StartupRequest::Startup(message) => {
            body.put_slice(&message.version.to_code());
            for (name, value) in &message.parameters {
                put_cstring(&mut body, name);
                put_cstring(&mut body, value);
            }
            body.put_u8(0);
        }
    }
    encode_startup_frame(&body, out);
}

fn fixed_request(
    rest: &[u8],
    request: StartupRequest,
    trailing: &'static str,
) -> Result<StartupRequest, StartupError> {
    if rest.is_empty() {
        Ok(request)
    } else {
        Err(StartupError::Malformed(trailing))
    }
}

fn decode_cancel_key(rest: &[u8]) -> Result<CancelKey, StartupError> {
    let keys: &[u8; CANCEL_KEY_LEN] = rest.try_into().map_err(|_| {
        StartupError::Malformed("CancelRequest must carry exactly a process id and a secret key")
    })?;
    let (process_id, secret_key) = keys.split_first_chunk::<4>().expect("8-byte key pair");
    Ok(CancelKey {
        process_id: i32::from_be_bytes(*process_id),
        secret_key: i32::from_be_bytes(secret_key.try_into().expect("4-byte secret key")),
    })
}

fn decode_parameters(mut rest: &[u8]) -> Result<Vec<(String, String)>, StartupError> {
    let mut parameters = Vec::new();
    loop {
        match rest {
            [] => return Err(StartupError::Malformed("parameter list is not terminated")),
            [0] => return Ok(parameters),
            [0, ..] => {
                return Err(StartupError::Malformed(
                    "bytes follow the parameter list terminator",
                ));
            }
            _ => {}
        }
        let (name, after_name) = take_cstring(rest, "parameter name is not terminated")?;
        let (value, after_value) = take_cstring(after_name, "parameter value is missing")?;
        parameters.push((name.to_owned(), value.to_owned()));
        rest = after_value;
    }
}

fn take_cstring<'a>(
    bytes: &'a [u8],
    unterminated: &'static str,
) -> Result<(&'a str, &'a [u8]), StartupError> {
    let end = bytes
        .iter()
        .position(|byte| *byte == 0)
        .ok_or(StartupError::Malformed(unterminated))?;
    let text = std::str::from_utf8(&bytes[..end]).map_err(|_| StartupError::InvalidUtf8)?;
    Ok((text, &bytes[end + 1..]))
}

fn put_cstring(out: &mut BytesMut, text: &str) {
    out.put_slice(text.as_bytes());
    out.put_u8(0);
}
