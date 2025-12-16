//! Extended VCP Commands (Phase 3)
//!
//! Additional commands for multi-key ops, atomic counters, and keyspace scanning.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io;

use super::frame::{Frame, OpCode};

/// Extended command set (Phase 3)
#[derive(Debug, Clone)]
pub enum ExtendedCommand {
    // Multi-key operations
    /// Get multiple keys at once
    MGet { keys: Vec<Bytes> },

    /// Set multiple key-value pairs
    MSet { pairs: Vec<(Bytes, Bytes)> },

    /// Delete multiple keys
    MDel { keys: Vec<Bytes> },

    // Atomic counter operations
    /// Increment by 1
    Incr { key: Bytes },

    /// Decrement by 1
    Decr { key: Bytes },

    /// Increment by value
    IncrBy { key: Bytes, delta: i64 },

    /// Decrement by value
    DecrBy { key: Bytes, delta: i64 },

    // Keyspace operations
    /// Scan keys with pattern and cursor
    Scan {
        cursor: u64,
        pattern: Option<Bytes>,
        count: u32,
    },

    /// Get all keys matching pattern
    Keys { pattern: Option<Bytes> },
}

impl ExtendedCommand {
    /// Parse extended command from a VCP frame
    pub fn from_frame(frame: &Frame) -> io::Result<Self> {
        match frame.header.opcode {
            OpCode::MGet => {
                let keys = Self::read_key_list(&frame.payload)?;
                Ok(ExtendedCommand::MGet { keys })
            }

            OpCode::MSet => {
                let pairs = Self::read_kv_pairs(&frame.payload)?;
                Ok(ExtendedCommand::MSet { pairs })
            }

            OpCode::MDel => {
                let keys = Self::read_key_list(&frame.payload)?;
                Ok(ExtendedCommand::MDel { keys })
            }

            OpCode::Incr => {
                let key = Self::read_single_key(&frame.payload)?;
                Ok(ExtendedCommand::Incr { key })
            }

            OpCode::Decr => {
                let key = Self::read_single_key(&frame.payload)?;
                Ok(ExtendedCommand::Decr { key })
            }

            OpCode::IncrBy => {
                let mut payload = frame.payload.clone();
                let key = Self::read_length_prefixed(&mut payload)?;
                let delta = payload.get_i64();
                Ok(ExtendedCommand::IncrBy { key, delta })
            }

            OpCode::DecrBy => {
                let mut payload = frame.payload.clone();
                let key = Self::read_length_prefixed(&mut payload)?;
                let delta = payload.get_i64();
                Ok(ExtendedCommand::DecrBy { key, delta })
            }

            OpCode::Scan => {
                let mut payload = frame.payload.clone();
                let cursor = payload.get_u64();
                let count = payload.get_u32();
                let pattern = if payload.remaining() > 0 {
                    Some(Self::read_length_prefixed(&mut payload)?)
                } else {
                    None
                };
                Ok(ExtendedCommand::Scan {
                    cursor,
                    pattern,
                    count,
                })
            }

            OpCode::Keys => {
                let pattern = if frame.payload.is_empty() {
                    None
                } else {
                    Some(Self::read_single_key(&frame.payload)?)
                };
                Ok(ExtendedCommand::Keys { pattern })
            }

            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unknown extended opcode: {:?}", frame.header.opcode),
            )),
        }
    }

    /// Encode extended command to frame payload
    pub fn encode(&self) -> (OpCode, Bytes) {
        match self {
            ExtendedCommand::MGet { keys } => {
                let payload = Self::write_key_list(keys);
                (OpCode::MGet, payload)
            }

            ExtendedCommand::MSet { pairs } => {
                let payload = Self::write_kv_pairs(pairs);
                (OpCode::MSet, payload)
            }

            ExtendedCommand::MDel { keys } => {
                let payload = Self::write_key_list(keys);
                (OpCode::MDel, payload)
            }

            ExtendedCommand::Incr { key } => {
                let payload = Self::write_single_key(key);
                (OpCode::Incr, payload)
            }

            ExtendedCommand::Decr { key } => {
                let payload = Self::write_single_key(key);
                (OpCode::Decr, payload)
            }

            ExtendedCommand::IncrBy { key, delta } => {
                let mut buf = BytesMut::new();
                Self::write_length_prefixed(&mut buf, key);
                buf.put_i64(*delta);
                (OpCode::IncrBy, buf.freeze())
            }

            ExtendedCommand::DecrBy { key, delta } => {
                let mut buf = BytesMut::new();
                Self::write_length_prefixed(&mut buf, key);
                buf.put_i64(*delta);
                (OpCode::DecrBy, buf.freeze())
            }

            ExtendedCommand::Scan {
                cursor,
                pattern,
                count,
            } => {
                let mut buf = BytesMut::new();
                buf.put_u64(*cursor);
                buf.put_u32(*count);
                if let Some(p) = pattern {
                    Self::write_length_prefixed(&mut buf, p);
                }
                (OpCode::Scan, buf.freeze())
            }

            ExtendedCommand::Keys { pattern } => {
                if let Some(p) = pattern {
                    (OpCode::Keys, Self::write_single_key(p))
                } else {
                    (OpCode::Keys, Bytes::new())
                }
            }
        }
    }

    // Helper functions
    fn read_single_key(data: &Bytes) -> io::Result<Bytes> {
        let mut buf = data.clone();
        Self::read_length_prefixed(&mut buf)
    }

    fn read_length_prefixed(buf: &mut Bytes) -> io::Result<Bytes> {
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

    fn read_key_list(data: &Bytes) -> io::Result<Vec<Bytes>> {
        let mut buf = data.clone();
        if buf.remaining() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough data for count",
            ));
        }
        let count = buf.get_u32() as usize;
        let mut keys = Vec::with_capacity(count);
        for _ in 0..count {
            keys.push(Self::read_length_prefixed(&mut buf)?);
        }
        Ok(keys)
    }

    fn read_kv_pairs(data: &Bytes) -> io::Result<Vec<(Bytes, Bytes)>> {
        let mut buf = data.clone();
        if buf.remaining() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough data for count",
            ));
        }
        let count = buf.get_u32() as usize;
        let mut pairs = Vec::with_capacity(count);
        for _ in 0..count {
            let key = Self::read_length_prefixed(&mut buf)?;
            let value = Self::read_length_prefixed(&mut buf)?;
            pairs.push((key, value));
        }
        Ok(pairs)
    }

    fn write_single_key(key: &Bytes) -> Bytes {
        let mut buf = BytesMut::with_capacity(4 + key.len());
        Self::write_length_prefixed(&mut buf, key);
        buf.freeze()
    }

    fn write_length_prefixed(buf: &mut BytesMut, data: &Bytes) {
        buf.put_u32(data.len() as u32);
        buf.put_slice(data);
    }

    fn write_key_list(keys: &[Bytes]) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u32(keys.len() as u32);
        for key in keys {
            Self::write_length_prefixed(&mut buf, key);
        }
        buf.freeze()
    }

    fn write_kv_pairs(pairs: &[(Bytes, Bytes)]) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u32(pairs.len() as u32);
        for (key, value) in pairs {
            Self::write_length_prefixed(&mut buf, key);
            Self::write_length_prefixed(&mut buf, value);
        }
        buf.freeze()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mget_command() {
        let cmd = ExtendedCommand::MGet {
            keys: vec![
                Bytes::from_static(b"key1"),
                Bytes::from_static(b"key2"),
                Bytes::from_static(b"key3"),
            ],
        };
        let (opcode, payload) = cmd.encode();
        let frame = Frame::new(opcode, 1, payload);
        let parsed = ExtendedCommand::from_frame(&frame).unwrap();

        if let ExtendedCommand::MGet { keys } = parsed {
            assert_eq!(keys.len(), 3);
            assert_eq!(keys[0].as_ref(), b"key1");
        } else {
            panic!("Expected MGet command");
        }
    }

    #[test]
    fn test_incr_command() {
        let cmd = ExtendedCommand::IncrBy {
            key: Bytes::from_static(b"counter"),
            delta: 5,
        };
        let (opcode, payload) = cmd.encode();
        let frame = Frame::new(opcode, 1, payload);
        let parsed = ExtendedCommand::from_frame(&frame).unwrap();

        if let ExtendedCommand::IncrBy { key, delta } = parsed {
            assert_eq!(key.as_ref(), b"counter");
            assert_eq!(delta, 5);
        } else {
            panic!("Expected IncrBy command");
        }
    }
}
