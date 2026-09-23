use crate::message::FrontendTag;

const LENGTH_BYTES: usize = 4;
const NO_DATA: i32 = -1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FrontendError {
    #[error("malformed {tag:?} body: {reason}")]
    MalformedBody {
        tag: FrontendTag,
        reason: &'static str,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SaslInitialResponse<'a> {
    pub mechanism: &'a str,
    pub data: &'a [u8],
}

pub fn decode_sasl_initial_response(body: &[u8]) -> Result<SaslInitialResponse<'_>, FrontendError> {
    let terminator = body
        .iter()
        .position(|byte| *byte == 0)
        .ok_or_else(|| malformed("the mechanism name is not terminated"))?;
    let mechanism = std::str::from_utf8(&body[..terminator])
        .map_err(|_| malformed("the mechanism name is not valid UTF-8"))?;
    let (declared, data) = body[terminator + 1..]
        .split_at_checked(LENGTH_BYTES)
        .ok_or_else(|| malformed("the response length is truncated"))?;
    let declared = i32::from_be_bytes([declared[0], declared[1], declared[2], declared[3]]);
    if declared == NO_DATA {
        if !data.is_empty() {
            return Err(malformed("a response of no data is followed by bytes"));
        }
        return Ok(SaslInitialResponse {
            mechanism,
            data: &[],
        });
    }
    let declared =
        usize::try_from(declared).map_err(|_| malformed("the response length is negative"))?;
    if declared != data.len() {
        return Err(malformed(
            "the response length disagrees with the rest of the message",
        ));
    }
    Ok(SaslInitialResponse { mechanism, data })
}

fn malformed(reason: &'static str) -> FrontendError {
    malformed_body(FrontendTag::Password, reason)
}

fn malformed_body(tag: FrontendTag, reason: &'static str) -> FrontendError {
    FrontendError::MalformedBody { tag, reason }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseTarget<'a> {
    Statement(&'a str),
    Portal(&'a str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Parse<'a> {
    pub statement: &'a str,
    pub query: &'a str,
}

pub fn decode_query(body: &[u8]) -> Result<&str, FrontendError> {
    let (query, _) = cstr(body, FrontendTag::Query, "the query text is not terminated")?;
    Ok(query)
}

pub fn decode_parse(body: &[u8]) -> Result<Parse<'_>, FrontendError> {
    let (statement, rest) = cstr(
        body,
        FrontendTag::Parse,
        "the statement name is not terminated",
    )?;
    let (query, _) = cstr(rest, FrontendTag::Parse, "the query text is not terminated")?;
    Ok(Parse { statement, query })
}

pub fn decode_bind(body: &[u8]) -> Result<&str, FrontendError> {
    let (_, rest) = cstr(body, FrontendTag::Bind, "the portal name is not terminated")?;
    let (statement, _) = cstr(
        rest,
        FrontendTag::Bind,
        "the statement name is not terminated",
    )?;
    Ok(statement)
}

pub fn decode_close(body: &[u8]) -> Result<CloseTarget<'_>, FrontendError> {
    let (target, rest) = body
        .split_first()
        .ok_or_else(|| malformed_body(FrontendTag::Close, "the target is missing"))?;
    let (name, _) = cstr(
        rest,
        FrontendTag::Close,
        "the target name is not terminated",
    )?;
    match target {
        b'S' => Ok(CloseTarget::Statement(name)),
        b'P' => Ok(CloseTarget::Portal(name)),
        _ => Err(malformed_body(
            FrontendTag::Close,
            "the target is neither a statement nor a portal",
        )),
    }
}

fn cstr<'a>(
    body: &'a [u8],
    tag: FrontendTag,
    unterminated: &'static str,
) -> Result<(&'a str, &'a [u8]), FrontendError> {
    let terminator = body
        .iter()
        .position(|byte| *byte == 0)
        .ok_or_else(|| malformed_body(tag, unterminated))?;
    let text = std::str::from_utf8(&body[..terminator])
        .map_err(|_| malformed_body(tag, "a name is not valid UTF-8"))?;
    Ok((text, &body[terminator + 1..]))
}
