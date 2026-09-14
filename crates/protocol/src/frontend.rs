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
    FrontendError::MalformedBody {
        tag: FrontendTag::Password,
        reason,
    }
}
