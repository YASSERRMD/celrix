use bytes::{BytesMut, Buf};
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use thiserror::Error;

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

pub struct Client {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Client {
    pub async fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(4096),
        })
    }

    pub async fn set(&mut self, key: &str, value: &str) -> Result<()> {
        self.send_command(&["SET", key, value]).await?;
        self.expect_ok().await
    }

    pub async fn get(&mut self, key: &str) -> Result<Option<String>> {
        self.send_command(&["GET", key]).await?;
        self.read_string().await
    }

    pub async fn del(&mut self, key: &str) -> Result<bool> {
        self.send_command(&["DEL", key]).await?;
        let val = self.read_int().await?;
        Ok(val > 0)
    }

    async fn send_command(&mut self, args: &[&str]) -> Result<()> {
        // VCP format: *<num_args>\r\n$<len>\r\n<arg>\r\n...
        self.stream.write_all(format!("*{}\r\n", args.len()).as_bytes()).await?;
        for arg in args {
            self.stream.write_all(format!("${}\r\n{}\r\n", arg.len(), arg).as_bytes()).await?;
        }
        self.stream.flush().await?;
        Ok(())
    }

    async fn expect_ok(&mut self) -> Result<()> {
        let res = self.read_response().await?;
        match res {
            Response::SimpleString(s) if s == "OK" => Ok(()),
            Response::Error(e) => Err(Error::Server(e)),
            _ => Err(Error::Protocol("Expected OK".to_string())),
        }
    }

    async fn read_string(&mut self) -> Result<Option<String>> {
        let res = self.read_response().await?;
        match res {
            Response::BulkString(s) => Ok(s),
            Response::Null => Ok(None),
            Response::Error(e) => Err(Error::Server(e)),
            _ => Err(Error::Protocol("Expected BulkString".to_string())),
        }
    }
    
    async fn read_int(&mut self) -> Result<i64> {
        match self.read_response().await? {
            Response::Integer(i) => Ok(i),
            Response::Error(e) => Err(Error::Server(e)),
             _ => Err(Error::Protocol("Expected Integer".to_string())),
        }
    }

    async fn read_response(&mut self) -> Result<Response> {
        loop {
            if let Some(frame) = self.parse_frame()? {
                return Ok(frame);
            }

            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Err(Error::ConnectionClosed);
                } else {
                    return Err(Error::ConnectionClosed);
                }
            }
        }
    }

    fn parse_frame(&mut self) -> Result<Option<Response>> {
        let mut buf = Cursor::new(&self.buffer[..]);

        match check(&mut buf) {
            Ok(_) => {
                let len = buf.position() as usize;
                buf.set_position(0);
                let response = parse(&mut buf)?;
                self.buffer.advance(len);
                Ok(Some(response))
            }
            Err(ParseError::Incomplete) => Ok(None),
            Err(ParseError::Other(e)) => Err(Error::Protocol(e)),
        }
    }
}

#[derive(Debug)]
enum Response {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<String>),
    Array(Vec<Response>),
    Null,
}

enum ParseError {
    Incomplete,
    Other(String),
}

fn check(buf: &mut Cursor<&[u8]>) -> std::result::Result<(), ParseError> {
    if !buf.has_remaining() {
        return Err(ParseError::Incomplete);
    }
    let src = buf.chunk();
    // Simplified VCP check (just basic line reading for now)
    // Real implementation would be more robust like the server's codec
    // For simplicity of this client MVP, assume single lines for +, -, :
    // And pairs for $
    // Full parser below handles details
    
    // We actually need the full parser logic to check boundaries properly
    // This simple check isn't enough for arrays, etc.
    // Let's rely on parse() to fail with Incomplete if needed, 
    // but standard pattern is separate check.
    // Reusing a simplified version of server's Frame::check() logic here:
    
    match get_u8(buf)? {
        b'+' => get_line(buf).map(|_| ()),
        b'-' => get_line(buf).map(|_| ()),
        b':' => get_line(buf).map(|_| ()),
        b'$' => {
            let len = get_decimal(buf)?;
            if len == -1 { return Ok(()); } // Null bulk string
            let len = len as usize;
            if buf.remaining() < len + 2 { return Err(ParseError::Incomplete); }
            buf.advance(len + 2);
            Ok(())
        }
        b'*' => {
            let len = get_decimal(buf)?;
            for _ in 0..len {
                check(buf)?;
            }
            Ok(())
        }
        _ => Err(ParseError::Other("Invalid protocol byte".to_string())),
    }
}

fn parse(buf: &mut Cursor<&[u8]>) -> Result<Response> {
    match get_u8(buf).map_err(|_| Error::Protocol("Incomplete".into()))? {
        b'+' => {
            let line = get_line(buf).map_err(|_| Error::Protocol("Incomplete".into()))?;
            Ok(Response::SimpleString(String::from_utf8_lossy(line).into()))
        },
        b'-' => {
             let line = get_line(buf).map_err(|_| Error::Protocol("Incomplete".into()))?;
             Ok(Response::Error(String::from_utf8_lossy(line).into()))
        },
        b':' => {
             let n = get_decimal(buf).map_err(|_| Error::Protocol("Incomplete".into()))?;
             Ok(Response::Integer(n))
        },
        b'$' => {
            let len = get_decimal(buf).map_err(|_| Error::Protocol("Incomplete".into()))?;
            if len == -1 {
                Ok(Response::Null)
            } else {
                let len = len as usize;
                let data = buf.chunk();
                if data.len() < len + 2 { return Err(Error::Protocol("Incomplete".into())); }
                let s = std::str::from_utf8(&data[..len]).map_err(|_| Error::Protocol("Invalid UTF-8".into()))?;
                let s = s.to_string();
                buf.advance(len + 2);
                Ok(Response::BulkString(Some(s)))
            }
        },
        b'*' => {
             let len = get_decimal(buf).map_err(|_| Error::Protocol("Incomplete".into()))?;
             let mut arr = Vec::with_capacity(len as usize);
             for _ in 0..len {
                 arr.push(parse(buf)?);
             }
             Ok(Response::Array(arr))
        }
        _ => Err(Error::Protocol("Invalid byte".into())),
    }
}

fn get_u8(buf: &mut Cursor<&[u8]>) -> std::result::Result<u8, ParseError> {
    if !buf.has_remaining() {
        return Err(ParseError::Incomplete);
    }
    Ok(buf.get_u8())
}

fn get_line<'a>(buf: &mut Cursor<&'a [u8]>) -> std::result::Result<&'a [u8], ParseError> {
    let start = buf.position() as usize;
    let end = buf.get_ref().len() - 1;

    for i in start..end {
        if buf.get_ref()[i] == b'\r' && buf.get_ref()[i + 1] == b'\n' {
            buf.set_position((i + 2) as u64);
            return Ok(&buf.get_ref()[start..i]);
        }
    }
    Err(ParseError::Incomplete)
}

fn get_decimal(buf: &mut Cursor<&[u8]>) -> std::result::Result<i64, ParseError> {
    let line = get_line(buf)?;
    let s = std::str::from_utf8(line).map_err(|_| ParseError::Other("Invalid UTF-8".into()))?;
    s.parse::<i64>().map_err(|_| ParseError::Other("Invalid integer".into()))
}
