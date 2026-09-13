use bytes::{BufMut, Bytes, BytesMut};
use pgsteward_protocol::framing::{
    FrameError, decode_frame, decode_startup_frame, encode_frame, encode_startup_frame,
};
use proptest::prelude::*;

#[test]
fn encode_frame_writes_tag_and_length_including_itself() {
    let mut out = BytesMut::new();
    encode_frame(b'Q', b"SELECT 1\0", &mut out);
    assert_eq!(&out[..], b"Q\x00\x00\x00\x0dSELECT 1\0");
}

#[test]
fn decode_frame_returns_none_until_full_frame_is_buffered() {
    let mut buf = BytesMut::from(&b"Q\x00\x00\x00\x0dSELECT"[..]);
    assert!(decode_frame(&mut buf, 1 << 20).unwrap().is_none());
    assert_eq!(buf.len(), 11);
    buf.put_slice(b" 1\0");
    let frame = decode_frame(&mut buf, 1 << 20).unwrap().unwrap();
    assert_eq!(frame.tag, b'Q');
    assert_eq!(&frame.body[..], b"SELECT 1\0");
    assert!(buf.is_empty());
}

#[test]
fn decode_frame_leaves_trailing_bytes_for_the_next_frame() {
    let mut buf = BytesMut::new();
    encode_frame(b'Z', b"I", &mut buf);
    encode_frame(b'Q', b"x\0", &mut buf);
    let first = decode_frame(&mut buf, 1 << 20).unwrap().unwrap();
    assert_eq!(first.tag, b'Z');
    let second = decode_frame(&mut buf, 1 << 20).unwrap().unwrap();
    assert_eq!(second.tag, b'Q');
    assert!(decode_frame(&mut buf, 1 << 20).unwrap().is_none());
}

#[test]
fn decode_frame_rejects_length_shorter_than_header() {
    let mut buf = BytesMut::from(&b"Q\x00\x00\x00\x03"[..]);
    assert!(matches!(
        decode_frame(&mut buf, 1 << 20),
        Err(FrameError::InvalidLength { len: 3 })
    ));
}

#[test]
fn decode_frame_rejects_negative_length() {
    let mut buf = BytesMut::from(&b"Q\xff\xff\xff\xff"[..]);
    assert!(matches!(
        decode_frame(&mut buf, 1 << 20),
        Err(FrameError::InvalidLength { len: -1 })
    ));
}

#[test]
fn decode_frame_rejects_frames_over_the_limit_before_buffering_them() {
    let mut buf = BytesMut::from(&b"d\x00\x00\x10\x00"[..]);
    assert!(matches!(
        decode_frame(&mut buf, 1024),
        Err(FrameError::TooLarge {
            len: 4096,
            max: 1024
        })
    ));
}

#[test]
fn startup_frame_has_no_tag_and_keeps_the_protocol_code_in_the_body() {
    let mut out = BytesMut::new();
    encode_startup_frame(
        &[
            0x00, 0x03, 0x00, 0x00, b'u', b's', b'e', b'r', 0, b'a', 0, 0,
        ],
        &mut out,
    );
    assert_eq!(out[..4], [0, 0, 0, 16]);
    let mut buf = out.clone();
    let body = decode_startup_frame(&mut buf, 1 << 20).unwrap().unwrap();
    assert_eq!(body[..4], [0x00, 0x03, 0x00, 0x00]);
    assert_eq!(body.len(), 12);
    assert!(buf.is_empty());
}

#[test]
fn startup_frame_needs_at_least_a_protocol_code() {
    let mut buf = BytesMut::from(&[0u8, 0, 0, 7, 1, 2, 3][..]);
    assert!(matches!(
        decode_startup_frame(&mut buf, 1 << 20),
        Err(FrameError::InvalidLength { len: 7 })
    ));
}

fn arb_frames() -> impl Strategy<Value = Vec<(u8, Vec<u8>)>> {
    prop::collection::vec(
        (any::<u8>(), prop::collection::vec(any::<u8>(), 0..64)),
        0..16,
    )
}

proptest! {
    #[test]
    fn frames_roundtrip_through_arbitrary_chunking(frames in arb_frames(), chunk in 1usize..32) {
        let mut wire = BytesMut::new();
        for (tag, body) in &frames {
            encode_frame(*tag, body, &mut wire);
        }
        let wire: Bytes = wire.freeze();

        let mut buf = BytesMut::new();
        let mut decoded = Vec::new();
        for piece in wire.chunks(chunk) {
            buf.put_slice(piece);
            while let Some(frame) = decode_frame(&mut buf, 1 << 20).unwrap() {
                decoded.push((frame.tag, frame.body.to_vec()));
            }
        }
        prop_assert!(buf.is_empty());
        prop_assert_eq!(decoded, frames);
    }
}
