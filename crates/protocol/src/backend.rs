use bytes::{BufMut, BytesMut};

use crate::framing::encode_frame;
use crate::message::{BackendTag, TransactionStatus};
use crate::startup::CancelKey;

pub mod sqlstate {
    pub const FEATURE_NOT_SUPPORTED: &str = "0A000";
    pub const PROTOCOL_VIOLATION: &str = "08P01";
    pub const INVALID_AUTHORIZATION_SPECIFICATION: &str = "28000";
    pub const INVALID_PASSWORD: &str = "28P01";
    pub const INVALID_SQL_STATEMENT_NAME: &str = "26000";
    pub const TOO_MANY_CONNECTIONS: &str = "53300";
}

const AUTHENTICATION_OK: i32 = 0;
const AUTHENTICATION_SASL: i32 = 10;
const AUTHENTICATION_SASL_CONTINUE: i32 = 11;
const AUTHENTICATION_SASL_FINAL: i32 = 12;

const SEVERITY_FIELD: u8 = b'S';
const SEVERITY_NON_LOCALIZED_FIELD: u8 = b'V';
const SQLSTATE_FIELD: u8 = b'C';
const MESSAGE_FIELD: u8 = b'M';
const DETAIL_FIELD: u8 = b'D';
const HINT_FIELD: u8 = b'H';

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EncryptionResponse {
    Accepted,
    Refused,
}

impl EncryptionResponse {
    #[must_use]
    pub fn to_byte(self) -> u8 {
        match self {
            Self::Accepted => b'S',
            Self::Refused => b'N',
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Severity {
    Error,
    Fatal,
}

impl Severity {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Error => "ERROR",
            Self::Fatal => "FATAL",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorResponse {
    pub severity: Severity,
    pub code: String,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
}

impl ErrorResponse {
    pub fn error(code: &str, message: impl Into<String>) -> Self {
        Self::new(Severity::Error, code, message)
    }

    pub fn fatal(code: &str, message: impl Into<String>) -> Self {
        Self::new(Severity::Fatal, code, message)
    }

    fn new(severity: Severity, code: &str, message: impl Into<String>) -> Self {
        Self {
            severity,
            code: code.to_owned(),
            message: message.into(),
            detail: None,
            hint: None,
        }
    }

    #[must_use]
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    #[must_use]
    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }
}

pub fn encode_encryption_response(response: EncryptionResponse, out: &mut BytesMut) {
    out.put_u8(response.to_byte());
}

pub fn encode_authentication_ok(out: &mut BytesMut) {
    let mut body = BytesMut::new();
    body.put_i32(AUTHENTICATION_OK);
    encode_frame(BackendTag::Authentication.into(), &body, out);
}

pub fn encode_authentication_sasl(mechanisms: &[&str], out: &mut BytesMut) {
    let mut body = BytesMut::new();
    body.put_i32(AUTHENTICATION_SASL);
    for mechanism in mechanisms {
        body.put_slice(mechanism.as_bytes());
        body.put_u8(0);
    }
    body.put_u8(0);
    encode_frame(BackendTag::Authentication.into(), &body, out);
}

pub fn encode_authentication_sasl_continue(data: &str, out: &mut BytesMut) {
    encode_authentication_sasl_data(AUTHENTICATION_SASL_CONTINUE, data, out);
}

pub fn encode_authentication_sasl_final(data: &str, out: &mut BytesMut) {
    encode_authentication_sasl_data(AUTHENTICATION_SASL_FINAL, data, out);
}

fn encode_authentication_sasl_data(kind: i32, data: &str, out: &mut BytesMut) {
    let mut body = BytesMut::new();
    body.put_i32(kind);
    body.put_slice(data.as_bytes());
    encode_frame(BackendTag::Authentication.into(), &body, out);
}

pub fn encode_parameter_status(name: &str, value: &str, out: &mut BytesMut) {
    let mut body = BytesMut::new();
    body.put_slice(name.as_bytes());
    body.put_u8(0);
    body.put_slice(value.as_bytes());
    body.put_u8(0);
    encode_frame(BackendTag::ParameterStatus.into(), &body, out);
}

pub fn encode_backend_key_data(key: CancelKey, out: &mut BytesMut) {
    let mut body = BytesMut::new();
    body.put_i32(key.process_id);
    body.put_i32(key.secret_key);
    encode_frame(BackendTag::BackendKeyData.into(), &body, out);
}

pub fn encode_ready_for_query(status: TransactionStatus, out: &mut BytesMut) {
    let mut body = BytesMut::new();
    body.put_u8(status.into());
    encode_frame(BackendTag::ReadyForQuery.into(), &body, out);
}

pub fn encode_error_response(error: &ErrorResponse, out: &mut BytesMut) {
    let severity = error.severity.as_str();
    let mut body = BytesMut::new();
    put_field(&mut body, SEVERITY_FIELD, severity);
    put_field(&mut body, SEVERITY_NON_LOCALIZED_FIELD, severity);
    put_field(&mut body, SQLSTATE_FIELD, &error.code);
    put_field(&mut body, MESSAGE_FIELD, &error.message);
    if let Some(detail) = &error.detail {
        put_field(&mut body, DETAIL_FIELD, detail);
    }
    if let Some(hint) = &error.hint {
        put_field(&mut body, HINT_FIELD, hint);
    }
    body.put_u8(0);
    encode_frame(BackendTag::ErrorResponse.into(), &body, out);
}

fn put_field(out: &mut BytesMut, kind: u8, value: &str) {
    out.put_u8(kind);
    out.put_slice(value.as_bytes());
    out.put_u8(0);
}
