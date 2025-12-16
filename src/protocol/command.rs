//! VCP Command Parsing
//!
//! Parses command arguments from VCP frames.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io;

use super::frame::{Frame, OpCode};

/// Parsed command from a VCP frame
#[derive(Debug, Clone)]
pub enum Command {
    /// Health check
    Ping,

    /// Get value by key
    Get { key: Bytes },

    /// Set key-value with optional TTL (seconds)
    Set {
        key: Bytes,
        value: Bytes,
        ttl: Option<u64>,
    },

    /// Delete key
    Del { key: Bytes },

    /// Check if key exists
    Exists { key: Bytes },

    /// Add vector embedding
    VAdd {
        key: Bytes,
        vector: Vec<f32>,
    },

    /// Search for similar vectors
    VSearch {
        vector: Vec<f32>,
        k: usize,
    },
}

impl Command {
    /// Parse command from a VCP frame
    pub fn from_frame(frame: &Frame) -> io::Result<Self> {
        match frame.header.opcode {
            OpCode::Ping => Ok(Command::Ping),

            OpCode::Get => {
                let key = Self::read_length_prefixed(&frame.payload)?;
                Ok(Command::Get { key })
            }

            OpCode::Set => {
                let mut payload = frame.payload.clone();
                let key = Self::read_length_prefixed_buf(&mut payload)?;
                let value = Self::read_length_prefixed_buf(&mut payload)?;
                let ttl = if payload.remaining() >= 8 {
                    let t = payload.get_u64();
                    if t > 0 {
                        Some(t)
                    } else {
                        None
                    }
                } else {
                    None
                };
                Ok(Command::Set { key, value, ttl })
            }

            OpCode::Del => {
                let key = Self::read_length_prefixed(&frame.payload)?;
                Ok(Command::Del { key })
            }

            OpCode::Exists => {
                let key = Self::read_length_prefixed(&frame.payload)?;
                Ok(Command::Exists { key })
            }

            OpCode::VAdd => {
                let mut payload = frame.payload.clone();
                let key = Self::read_length_prefixed_buf(&mut payload)?;
                let count = payload.get_u32() as usize;
                let mut vector = Vec::with_capacity(count);
                for _ in 0..count {
                    if payload.remaining() < 4 {
                        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Insufficient vector data"));
                    }
                    vector.push(payload.get_f32());
                }
                Ok(Command::VAdd { key, vector })
            }

            OpCode::VSearch => {
                let mut payload = frame.payload.clone();
                let count = payload.get_u32() as usize;
                let mut vector = Vec::with_capacity(count);
                for _ in 0..count {
                    if payload.remaining() < 4 {
                        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Insufficient vector data"));
                    }
                    vector.push(payload.get_f32());
                }
                let k = if payload.remaining() >= 4 {
                    payload.get_u32() as usize
                } else {
                    10 // Default k
                };
                Ok(Command::VSearch { vector, k })
            }

            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unexpected opcode for command: {:?}", frame.header.opcode),
            )),
        }
    }

    /// Encode command to frame payload bytes
    pub fn encode(&self) -> (OpCode, Bytes) {
        match self {
            Command::Ping => (OpCode::Ping, Bytes::new()),

            Command::Get { key } => {
                let payload = Self::write_length_prefixed(key);
                (OpCode::Get, payload)
            }

            Command::Set { key, value, ttl } => {
                let mut buf = BytesMut::new();
                Self::write_length_prefixed_buf(&mut buf, key);
                Self::write_length_prefixed_buf(&mut buf, value);
                buf.put_u64(ttl.unwrap_or(0));
                (OpCode::Set, buf.freeze())
            }

            Command::Del { key } => {
                let payload = Self::write_length_prefixed(key);
                (OpCode::Del, payload)
            }

            Command::Exists { key } => {
                let payload = Self::write_length_prefixed(key);
                (OpCode::Exists, payload)
            }

            Command::VAdd { key, vector } => {
                let mut buf = BytesMut::new();
                Self::write_length_prefixed_buf(&mut buf, key);
                buf.put_u32(vector.len() as u32);
                for &f in vector {
                    buf.put_f32(f);
                }
                (OpCode::VAdd, buf.freeze())
            }

            Command::VSearch { vector, k } => {
                let mut buf = BytesMut::new();
                buf.put_u32(vector.len() as u32);
                for &f in vector {
                    buf.put_f32(f);
                }
                buf.put_u32(*k as u32);
                (OpCode::VSearch, buf.freeze())
            }
        }
    }

    fn read_length_prefixed(data: &Bytes) -> io::Result<Bytes> {
        let mut buf = data.clone();
        Self::read_length_prefixed_buf(&mut buf)
    }

    fn read_length_prefixed_buf(buf: &mut Bytes) -> io::Result<Bytes> {
        if buf.remaining() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough data for length prefix",
            ));
        }
        let len = buf.get_u32() as usize;
        if buf.remaining() < len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough data for payload",
            ));
        }
        Ok(buf.copy_to_bytes(len))
    }

    fn write_length_prefixed(data: &Bytes) -> Bytes {
        let mut buf = BytesMut::with_capacity(4 + data.len());
        Self::write_length_prefixed_buf(&mut buf, data);
        buf.freeze()
    }

    fn write_length_prefixed_buf(buf: &mut BytesMut, data: &Bytes) {
        buf.put_u32(data.len() as u32);
        buf.put_slice(data);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ping_command() {
        let frame = Frame::ping(1);
        let cmd = Command::from_frame(&frame).unwrap();
        assert!(matches!(cmd, Command::Ping));
    }

    #[test]
    fn test_get_command() {
        let cmd = Command::Get {
            key: Bytes::from_static(b"mykey"),
        };
        let (opcode, payload) = cmd.encode();
        let frame = Frame::new(opcode, 1, payload);
        let parsed = Command::from_frame(&frame).unwrap();

        if let Command::Get { key } = parsed {
            assert_eq!(key.as_ref(), b"mykey");
        } else {
            panic!("Expected Get command");
        }
    }

    #[test]
    fn test_set_command_with_ttl() {
        let cmd = Command::Set {
            key: Bytes::from_static(b"key"),
            value: Bytes::from_static(b"value"),
            ttl: Some(3600),
        };
        let (opcode, payload) = cmd.encode();
        let frame = Frame::new(opcode, 1, payload);
        let parsed = Command::from_frame(&frame).unwrap();

        if let Command::Set { key, value, ttl } = parsed {
            assert_eq!(key.as_ref(), b"key");
            assert_eq!(value.as_ref(), b"value");
            assert_eq!(ttl, Some(3600));
        } else {
            panic!("Expected Set command");
        }
    }
}
