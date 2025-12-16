//! VCP Codec for Tokio
//!
//! Implements Encoder and Decoder traits for framed I/O.

use bytes::BytesMut;
use std::io;
use tokio_util::codec::{Decoder, Encoder};

use super::frame::{Frame, FrameHeader, HEADER_SIZE};

/// Tokio codec for VCP frames
#[derive(Debug, Default)]
pub struct VcpCodec {
    /// Current decode state
    state: DecodeState,
}

#[derive(Debug, Default)]
enum DecodeState {
    #[default]
    Header,
    Payload(FrameHeader),
}

impl VcpCodec {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Decoder for VcpCodec {
    type Item = Frame;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            match &self.state {
                DecodeState::Header => {
                    if src.len() < HEADER_SIZE {
                        return Ok(None);
                    }

                    let header = FrameHeader::decode(&mut src.split_to(HEADER_SIZE).freeze())?;
                    self.state = DecodeState::Payload(header);
                }

                DecodeState::Payload(header) => {
                    let payload_len = header.payload_len as usize;

                    if src.len() < payload_len {
                        return Ok(None);
                    }

                    let payload = src.split_to(payload_len).freeze();
                    let frame = Frame {
                        header: header.clone(),
                        payload,
                    };

                    self.state = DecodeState::Header;
                    return Ok(Some(frame));
                }
            }
        }
    }
}

impl Encoder<Frame> for VcpCodec {
    type Error = io::Error;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.reserve(HEADER_SIZE + item.payload.len());
        item.encode(dst);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::OpCode;
    use bytes::Bytes;

    #[test]
    fn test_codec_roundtrip() {
        let mut codec = VcpCodec::new();
        let frame = Frame::new(OpCode::Get, 42, Bytes::from_static(b"hello"));

        // Encode
        let mut buf = BytesMut::new();
        codec.encode(frame.clone(), &mut buf).unwrap();

        // Decode
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.header.opcode, frame.header.opcode);
        assert_eq!(decoded.header.request_id, frame.header.request_id);
        assert_eq!(decoded.payload, frame.payload);
    }

    #[test]
    fn test_codec_partial_decode() {
        let mut codec = VcpCodec::new();
        let frame = Frame::new(OpCode::Set, 1, Bytes::from_static(b"test data"));

        let mut buf = BytesMut::new();
        codec.encode(frame, &mut buf).unwrap();

        // Split buffer to simulate partial reads
        let full = buf.clone();

        // Only header
        let mut partial = full.clone();
        partial.truncate(HEADER_SIZE);
        assert!(codec.decode(&mut partial).unwrap().is_none());

        // Full frame
        let mut full_buf = full;
        assert!(codec.decode(&mut full_buf).unwrap().is_some());
    }
}
