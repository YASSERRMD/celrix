//! VCP Response types
//!
//! Response variants for command execution results.

use bytes::Bytes;

use super::frame::{Frame, OpCode};

/// Response to a command
#[derive(Debug, Clone)]
pub enum Response {
    /// Simple OK response
    Ok,

    /// Nil/null response (key not found)
    Nil,

    /// String/bytes value
    Value(Bytes),

    /// Integer value
    Integer(i64),

    /// Error response
    Error(String),

    /// Pong response (for PING)
    Pong,

    /// Array response (list of byte arrays)
    Array(Vec<Bytes>),
}

impl Response {
    /// Convert response to a VCP frame
    pub fn to_frame(&self, request_id: u64) -> Frame {
        match self {
            Response::Ok => Frame::ok(request_id),
            Response::Nil => Frame::nil(request_id),
            Response::Value(data) => Frame::value(request_id, data.clone()),
            Response::Integer(n) => Frame::integer(request_id, *n),
            Response::Error(msg) => Frame::error(request_id, msg),
            Response::Pong => Frame::pong(request_id),
            Response::Array(items) => {
                use bytes::{BufMut, BytesMut};
                let mut buf = BytesMut::new();
                buf.put_u32(items.len() as u32);
                for item in items {
                    buf.put_u32(item.len() as u32);
                    buf.put_slice(item);
                }
                Frame::new(OpCode::Array, request_id, buf.freeze())
            }
        }
    }

    /// Parse response from a VCP frame
    pub fn from_frame(frame: &Frame) -> std::io::Result<Self> {
        match frame.header.opcode {
            OpCode::Ok => Ok(Response::Ok),
            OpCode::Nil => Ok(Response::Nil),
            OpCode::Pong => Ok(Response::Pong),
            OpCode::Value => Ok(Response::Value(frame.payload.clone())),
            OpCode::Integer => {
                if frame.payload.len() >= 8 {
                    let bytes: [u8; 8] = frame.payload[..8].try_into().unwrap();
                    Ok(Response::Integer(i64::from_be_bytes(bytes)))
                } else {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Invalid integer payload",
                    ))
                }
            }
            OpCode::Error => {
                let msg = String::from_utf8_lossy(&frame.payload).to_string();
                Ok(Response::Error(msg))
            }
            OpCode::Array => {
                if frame.payload.len() < 4 {
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid array payload"));
                }
                let mut buf = frame.payload.clone();
                use bytes::Buf;
                let count = buf.get_u32() as usize;
                let mut items = Vec::with_capacity(count);
                for _ in 0..count {
                    if buf.remaining() < 4 {
                        return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Insufficient array data"));
                    }
                    let len = buf.get_u32() as usize;
                    if buf.remaining() < len {
                        return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Insufficient item data"));
                    }
                    items.push(buf.copy_to_bytes(len));
                }
                Ok(Response::Array(items))
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Unexpected opcode for response: {:?}", frame.header.opcode),
            )),
        }
    }
}

impl std::fmt::Display for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Response::Ok => write!(f, "OK"),
            Response::Nil => write!(f, "(nil)"),
            Response::Value(data) => {
                let s = String::from_utf8_lossy(data);
                write!(f, "\"{}\"", s)
            }
            Response::Integer(n) => write!(f, "(integer) {}", n),
            Response::Error(msg) => write!(f, "(error) {}", msg),
            Response::Pong => write!(f, "PONG"),
            Response::Array(items) => {
                write!(f, "[")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 { write!(f, ", ")?; }
                    let s = String::from_utf8_lossy(item);
                    write!(f, "\"{}\"", s)?;
                }
                write!(f, "]")
            }
        }
    }
}
