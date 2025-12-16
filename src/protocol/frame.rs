//! VCP Frame Structure
//!
//! Binary frame format with 22-byte header for efficient parsing.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io;

/// Magic bytes identifying VCP protocol: "CELX"
pub const MAGIC: [u8; 4] = [0x43, 0x45, 0x4C, 0x58];

/// Protocol version
pub const VERSION: u8 = 1;

/// Fixed header size in bytes
pub const HEADER_SIZE: usize = 22;

/// Operation codes for VCP commands
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OpCode {
    // Basic operations
    Ping = 0x01,
    Pong = 0x02,
    Get = 0x03,
    Set = 0x04,
    Del = 0x05,
    Exists = 0x06,

    // Multi-key operations (Phase 3)
    MGet = 0x07,
    MSet = 0x08,
    MDel = 0x09,

    // Atomic operations (Phase 3)
    Incr = 0x0A,
    Decr = 0x0B,
    IncrBy = 0x0C,
    DecrBy = 0x0D,

    // Keyspace operations (Phase 3)
    Scan = 0x0E,
    Keys = 0x0F,

    // Response codes
    Ok = 0x10,
    Error = 0x11,
    Value = 0x12,
    Nil = 0x13,
    Integer = 0x14,
    Array = 0x15,

    // Vector operations (Phase 4/9)
    VAdd = 0x20,
    VSearch = 0x21,
}

impl OpCode {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0x01 => Some(OpCode::Ping),
            0x02 => Some(OpCode::Pong),
            0x03 => Some(OpCode::Get),
            0x04 => Some(OpCode::Set),
            0x05 => Some(OpCode::Del),
            0x06 => Some(OpCode::Exists),
            0x07 => Some(OpCode::MGet),
            0x08 => Some(OpCode::MSet),
            0x09 => Some(OpCode::MDel),
            0x0A => Some(OpCode::Incr),
            0x0B => Some(OpCode::Decr),
            0x0C => Some(OpCode::IncrBy),
            0x0D => Some(OpCode::DecrBy),
            0x0E => Some(OpCode::Scan),
            0x0F => Some(OpCode::Keys),
            0x10 => Some(OpCode::Ok),
            0x11 => Some(OpCode::Error),
            0x12 => Some(OpCode::Value),
            0x13 => Some(OpCode::Nil),
            0x14 => Some(OpCode::Integer),
            0x15 => Some(OpCode::Array),
            0x20 => Some(OpCode::VAdd),
            0x21 => Some(OpCode::VSearch),
            _ => None,
        }
    }
}

/// VCP Frame Header (22 bytes)
///
/// ```text
/// ┌──────────┬──────────┬──────────┬──────────┬─────────────────┐
/// │  Magic   │ Version  │  OpCode  │  Flags   │  Payload Len    │
/// │ (4 bytes)│ (1 byte) │ (1 byte) │ (2 bytes)│   (4 bytes)     │
/// ├──────────┴──────────┴──────────┴──────────┴─────────────────┤
/// │  Request ID (8 bytes)  │  Reserved (2 bytes)                │
/// └─────────────────────────────────────────────────────────────┘
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameHeader {
    pub version: u8,
    pub opcode: OpCode,
    pub flags: u16,
    pub payload_len: u32,
    pub request_id: u64,
}

impl FrameHeader {
    pub fn new(opcode: OpCode, request_id: u64) -> Self {
        Self {
            version: VERSION,
            opcode,
            flags: 0,
            payload_len: 0,
            request_id,
        }
    }

    pub fn with_payload_len(mut self, len: u32) -> Self {
        self.payload_len = len;
        self
    }

    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_slice(&MAGIC);
        buf.put_u8(self.version);
        buf.put_u8(self.opcode as u8);
        buf.put_u16(self.flags);
        buf.put_u32(self.payload_len);
        buf.put_u64(self.request_id);
        buf.put_u16(0); // Reserved
    }

    pub fn decode(buf: &mut impl Buf) -> io::Result<Self> {
        // Check magic bytes
        let mut magic = [0u8; 4];
        buf.copy_to_slice(&mut magic);
        if magic != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid magic bytes",
            ));
        }

        let version = buf.get_u8();
        let opcode_byte = buf.get_u8();
        let opcode = OpCode::from_u8(opcode_byte).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, format!("Invalid opcode: {}", opcode_byte))
        })?;
        let flags = buf.get_u16();
        let payload_len = buf.get_u32();
        let request_id = buf.get_u64();
        let _reserved = buf.get_u16();

        Ok(Self {
            version,
            opcode,
            flags,
            payload_len,
            request_id,
        })
    }
}

/// Complete VCP Frame with header and payload
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub header: FrameHeader,
    pub payload: Bytes,
}

impl Frame {
    pub fn new(opcode: OpCode, request_id: u64, payload: Bytes) -> Self {
        let header = FrameHeader::new(opcode, request_id).with_payload_len(payload.len() as u32);
        Self { header, payload }
    }

    pub fn ping(request_id: u64) -> Self {
        Self::new(OpCode::Ping, request_id, Bytes::new())
    }

    pub fn pong(request_id: u64) -> Self {
        Self::new(OpCode::Pong, request_id, Bytes::new())
    }

    pub fn ok(request_id: u64) -> Self {
        Self::new(OpCode::Ok, request_id, Bytes::new())
    }

    pub fn nil(request_id: u64) -> Self {
        Self::new(OpCode::Nil, request_id, Bytes::new())
    }

    pub fn error(request_id: u64, msg: &str) -> Self {
        Self::new(OpCode::Error, request_id, Bytes::copy_from_slice(msg.as_bytes()))
    }

    pub fn value(request_id: u64, data: Bytes) -> Self {
        Self::new(OpCode::Value, request_id, data)
    }

    pub fn integer(request_id: u64, value: i64) -> Self {
        let mut buf = BytesMut::with_capacity(8);
        buf.put_i64(value);
        Self::new(OpCode::Integer, request_id, buf.freeze())
    }

    pub fn encode(&self, buf: &mut BytesMut) {
        self.header.encode(buf);
        buf.put_slice(&self.payload);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_encode_decode() {
        let header = FrameHeader::new(OpCode::Get, 12345).with_payload_len(100);
        let mut buf = BytesMut::new();
        header.encode(&mut buf);

        assert_eq!(buf.len(), HEADER_SIZE);

        let decoded = FrameHeader::decode(&mut buf.freeze()).unwrap();
        assert_eq!(decoded.opcode, OpCode::Get);
        assert_eq!(decoded.request_id, 12345);
        assert_eq!(decoded.payload_len, 100);
    }

    #[test]
    fn test_frame_encode() {
        let frame = Frame::new(
            OpCode::Set,
            42,
            Bytes::from_static(b"test"),
        );
        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        assert_eq!(buf.len(), HEADER_SIZE + 4);
    }
}
