use bytes::{Buf, BufMut, Bytes, BytesMut};

const HEADER_LEN: usize = 5;
const LENGTH_FIELD_LEN: usize = 4;
const STARTUP_CODE_LEN: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub tag: u8,
    pub body: Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FrameError {
    #[error("invalid message length {len}")]
    InvalidLength { len: i32 },
    #[error("message length {len} exceeds the limit of {max} bytes")]
    TooLarge { len: usize, max: usize },
}

pub fn encode_frame(tag: u8, body: &[u8], out: &mut BytesMut) {
    out.reserve(HEADER_LEN + body.len());
    out.put_u8(tag);
    out.put_i32(length_field(body.len()));
    out.put_slice(body);
}

pub fn decode_frame(buf: &mut BytesMut, max_len: usize) -> Result<Option<Frame>, FrameError> {
    if buf.len() < HEADER_LEN {
        return Ok(None);
    }
    let tag = buf[0];
    let body_len = body_len_from_header(&buf[1..HEADER_LEN], LENGTH_FIELD_LEN, max_len)?;
    if buf.len() < HEADER_LEN + body_len {
        return Ok(None);
    }
    buf.advance(HEADER_LEN);
    let body = buf.split_to(body_len).freeze();
    Ok(Some(Frame { tag, body }))
}

pub fn encode_startup_frame(body: &[u8], out: &mut BytesMut) {
    out.reserve(LENGTH_FIELD_LEN + body.len());
    out.put_i32(length_field(body.len()));
    out.put_slice(body);
}

pub fn decode_startup_frame(
    buf: &mut BytesMut,
    max_len: usize,
) -> Result<Option<Bytes>, FrameError> {
    if buf.len() < LENGTH_FIELD_LEN {
        return Ok(None);
    }
    let body_len = body_len_from_header(
        &buf[..LENGTH_FIELD_LEN],
        LENGTH_FIELD_LEN + STARTUP_CODE_LEN,
        max_len,
    )?;
    if buf.len() < LENGTH_FIELD_LEN + body_len {
        return Ok(None);
    }
    buf.advance(LENGTH_FIELD_LEN);
    Ok(Some(buf.split_to(body_len).freeze()))
}

fn length_field(body_len: usize) -> i32 {
    i32::try_from(body_len + LENGTH_FIELD_LEN).expect("message body exceeds i32::MAX")
}

fn body_len_from_header(
    length_bytes: &[u8],
    min_len: usize,
    max_len: usize,
) -> Result<usize, FrameError> {
    let len = i32::from_be_bytes(length_bytes.try_into().expect("4-byte length field"));
    let total = usize::try_from(len).map_err(|_| FrameError::InvalidLength { len })?;
    if total < min_len {
        return Err(FrameError::InvalidLength { len });
    }
    if total > max_len {
        return Err(FrameError::TooLarge {
            len: total,
            max: max_len,
        });
    }
    Ok(total - LENGTH_FIELD_LEN)
}
