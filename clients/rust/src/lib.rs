use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use thiserror::Error;

const MAGIC: [u8; 4] = [0x43, 0x45, 0x4C, 0x58]; // "CELX"
const VERSION: u8 = 1;
const HEADER_SIZE: usize = 22;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Protocol error: {0}")]
    Protocol(String),
    #[error("Server error: {0}")]
    Server(String),
    #[error("Connection closed")]
    ConnectionClosed,
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OpCode {
    Ping = 0x01,
    Pong = 0x02,
    Get = 0x03,
    Set = 0x04,
    Del = 0x05,
    Exists = 0x06,
    
    // Responses
    Ok = 0x10,
    Error = 0x11,
    Value = 0x12,
    Nil = 0x13,
    Integer = 0x14,
    Array = 0x15,

    // Vector
    VAdd = 0x20,
    VSearch = 0x21,
}

impl OpCode {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x01 => Some(OpCode::Ping),
            0x02 => Some(OpCode::Pong),
            0x03 => Some(OpCode::Get),
            0x04 => Some(OpCode::Set),
            0x05 => Some(OpCode::Del),
            0x06 => Some(OpCode::Exists),
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

#[derive(Debug)]
pub enum Response {
    Ok,
    Nil,
    Pong,
    Value(Bytes),
    Integer(i64),
    Error(String),
    Array(Vec<Response>), // Recursive support
}

pub struct Client {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
    next_req_id: u64,
}

impl Client {
    pub async fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(8192),
            next_req_id: 1,
        })
    }

    pub async fn ping(&mut self) -> Result<()> {
        self.send_frame(OpCode::Ping, Bytes::new()).await?;
        match self.read_response().await? {
            Response::Pong => Ok(()),
            Response::Error(e) => Err(Error::Server(e)),
            _ => Err(Error::Protocol("Expected PONG".into())),
        }
    }

    pub async fn set(&mut self, key: &str, value: &str, ttl: Option<u64>) -> Result<()> {
        let key_bytes = key.as_bytes();
        let val_bytes = value.as_bytes();
        
        let mut payload = BytesMut::new();
        payload.put_u32(key_bytes.len() as u32);
        payload.put_slice(key_bytes);
        payload.put_u32(val_bytes.len() as u32);
        payload.put_slice(val_bytes);
        payload.put_u64(ttl.unwrap_or(0));

        self.send_frame(OpCode::Set, payload.freeze()).await?;
        self.expect_ok().await
    }

    pub async fn get(&mut self, key: &str) -> Result<Option<String>> {
        let key_bytes = key.as_bytes();
        let mut payload = BytesMut::new();
        payload.put_u32(key_bytes.len() as u32);
        payload.put_slice(key_bytes);

        self.send_frame(OpCode::Get, payload.freeze()).await?;
        match self.read_response().await? {
            Response::Value(bytes) => Ok(Some(String::from_utf8_lossy(&bytes).into())),
            Response::Nil => Ok(None),
            Response::Error(e) => Err(Error::Server(e)),
            _ => Err(Error::Protocol("Expected Value or Nil".into())),
        }
    }

    pub async fn del(&mut self, key: &str) -> Result<bool> {
        let key_bytes = key.as_bytes();
        let mut payload = BytesMut::new();
        payload.put_u32(key_bytes.len() as u32);
        payload.put_slice(key_bytes);

        self.send_frame(OpCode::Del, payload.freeze()).await?;
        match self.read_response().await? {
            Response::Integer(n) => Ok(n > 0),
            Response::Error(e) => Err(Error::Server(e)),
            _ => Err(Error::Protocol("Expected Integer".into())),
        }
    }

    pub async fn exists(&mut self, key: &str) -> Result<bool> {
        let key_bytes = key.as_bytes();
        let mut payload = BytesMut::new();
        payload.put_u32(key_bytes.len() as u32);
        payload.put_slice(key_bytes);

        self.send_frame(OpCode::Exists, payload.freeze()).await?;
        match self.read_response().await? {
            Response::Integer(n) => Ok(n > 0),
            Response::Error(e) => Err(Error::Server(e)),
            _ => Err(Error::Protocol("Expected Integer".into())),
        }
    }

    // Vector operations

    pub async fn vadd(&mut self, key: &str, vector: &[f32]) -> Result<()> {
        let key_bytes = key.as_bytes();
        let mut payload = BytesMut::new();
        
        // [key_len][key][count][f32...]
        payload.put_u32(key_bytes.len() as u32);
        payload.put_slice(key_bytes);
        payload.put_u32(vector.len() as u32);
        for &f in vector {
            payload.put_f32(f);
        }

        self.send_frame(OpCode::VAdd, payload.freeze()).await?;
        self.expect_ok().await
    }

    pub async fn vsearch(&mut self, vector: &[f32], k: usize) -> Result<Vec<String>> {
        let mut payload = BytesMut::new();
        
        // [count][f32...][k]
        payload.put_u32(vector.len() as u32);
        for &f in vector {
            payload.put_f32(f);
        }
        payload.put_u32(k as u32);

        self.send_frame(OpCode::VSearch, payload.freeze()).await?;
        match self.read_response().await? {
            Response::Array(items) => {
                let mut keys = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                         Response::Value(bytes) => keys.push(String::from_utf8_lossy(&bytes).into()),
                         _ => return Err(Error::Protocol("Expected Value in Array".into())),
                    }
                }
                Ok(keys)
            },
            Response::Error(e) => Err(Error::Server(e)),
            _ => Err(Error::Protocol("Expected Array".into())),
        }
    }

    // Internal helpers

    async fn expect_ok(&mut self) -> Result<()> {
        match self.read_response().await? {
            Response::Ok => Ok(()),
            Response::Error(e) => Err(Error::Server(e)),
            _ => Err(Error::Protocol("Expected OK".into())),
        }
    }

    async fn send_frame(&mut self, opcode: OpCode, payload: Bytes) -> Result<()> {
        let req_id = self.next_req_id;
        self.next_req_id += 1;

        let mut header = BytesMut::with_capacity(HEADER_SIZE);
        header.put_slice(&MAGIC);
        header.put_u8(VERSION);
        header.put_u8(opcode as u8);
        header.put_u16(0); // flags
        header.put_u32(payload.len() as u32);
        header.put_u64(req_id);
        header.put_u16(0); // reserved

        self.stream.write_all(&header).await?;
        if !payload.is_empty() {
            self.stream.write_all(&payload).await?;
        }
        self.stream.flush().await?;
        Ok(())
    }

    async fn read_response(&mut self) -> Result<Response> {
        loop {
            // Check if we have enough for header
            if self.buffer.len() >= HEADER_SIZE {
                let mut buf = Cursor::new(&self.buffer[..]);
                
                // Parse header
                let mut magic = [0u8; 4];
                buf.copy_to_slice(&mut magic);
                if magic != MAGIC {
                    return Err(Error::Protocol("Invalid magic bytes".into()));
                }
                let _version = buf.get_u8();
                let opcode_byte = buf.get_u8();
                let _flags = buf.get_u16();
                let payload_len = buf.get_u32() as usize;
                let _req_id = buf.get_u64();
                let _reserved = buf.get_u16();

                // Check payload availability
                if self.buffer.len() >= HEADER_SIZE + payload_len {
                    // Consume header + payload
                    self.buffer.advance(HEADER_SIZE);
                    let payload = self.buffer.split_to(payload_len).freeze();
                    
                    let opcode = OpCode::from_u8(opcode_byte)
                        .ok_or_else(|| Error::Protocol(format!("Unknown opcode: {}", opcode_byte)))?;

                    return match opcode {
                        OpCode::Ok => Ok(Response::Ok),
                        OpCode::Nil => Ok(Response::Nil),
                        OpCode::Pong => Ok(Response::Pong),
                        OpCode::Error => {
                            let msg = String::from_utf8_lossy(&payload).into();
                            Ok(Response::Error(msg))
                        },
                        OpCode::Value => Ok(Response::Value(payload)),
                        OpCode::Integer => {
                            if payload.len() < 8 { return Err(Error::Protocol("Invalid integer len".into())); }
                            let mut p = payload.clone(); // Implements Buf
                            use bytes::Buf; // Ensure Buf trait is used
                            let val = p.get_i64();
                            Ok(Response::Integer(val))
                        },
                        OpCode::Array => {
                           // Basic array parsing: [count: u32][len1: u32][bytes1]...
                           // Note: Recursive parsing of generic Responses is harder with flat payload structure.
                           // Server currently sends OpCode::Array with payload structure:
                           // [count: u32] then items.
                           // BUT items in current server impl (Response::Array) are encoded as: 
                           // [item_len: u32][item_bytes]. This assumes items are just Bytes (Response::Value).
                           // If we need recursive types, server encoding needs to be richer.
                           // For Phase 9 verification, `VSearch` returns `Array(Vec<Bytes>)` (keys). 
                           // So this simple parsing is sufficient for now.
                           
                           let mut p = payload.clone();
                           if p.remaining() < 4 { return Ok(Response::Array(vec![])); }
                           let count = p.get_u32() as usize;
                           let mut items = Vec::with_capacity(count);
                           
                           for _ in 0..count {
                               if p.remaining() < 4 { return Err(Error::Protocol("Incomplete array".into())); }
                               let item_len = p.get_u32() as usize;
                               if p.remaining() < item_len { return Err(Error::Protocol("Incomplete array item".into())); }
                               let item_data = p.copy_to_bytes(item_len);
                               // Knowing our server only sends values in arrays for now:
                               items.push(Response::Value(item_data));
                               // To be robust we might want generic parsing but protocol above implies flat bytes for array items
                           }
                           Ok(Response::Array(items))
                        },
                        _ => Err(Error::Protocol(format!("Unexpected response opcode: {:?}", opcode))),
                    };
                }
            }

            // Need more data
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Err(Error::ConnectionClosed);
                } else {
                    return Err(Error::ConnectionClosed); // Unexpected EOF
                }
            }
        }
    }
}
