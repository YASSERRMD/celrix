//! CELRIX CLI Client
//!
//! Interactive command-line client for CELRIX.

use bytes::Bytes;
use celrix::protocol::{Command, Frame, Response, VcpCodec};
use clap::Parser;
use futures::{SinkExt, StreamExt};
use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

/// CELRIX CLI - Interactive Client
#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Server host
    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    host: String,

    /// Server port
    #[arg(short, long, default_value_t = 6380)]
    port: u16,
}

static REQUEST_ID: AtomicU64 = AtomicU64::new(1);

fn next_request_id() -> u64 {
    REQUEST_ID.fetch_add(1, Ordering::Relaxed)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let addr = format!("{}:{}", args.host, args.port);

    println!("Connecting to CELRIX at {}...", addr);

    let stream = TcpStream::connect(&addr).await?;
    let mut framed = Framed::new(stream, VcpCodec::new());

    println!("Connected! Type 'help' for available commands, 'quit' to exit.\n");

    loop {
        print!("celrix> ");
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let input = input.trim();

        if input.is_empty() {
            continue;
        }

        if input.eq_ignore_ascii_case("quit") || input.eq_ignore_ascii_case("exit") {
            println!("Goodbye!");
            break;
        }

        if input.eq_ignore_ascii_case("help") {
            print_help();
            continue;
        }

        match parse_command(input) {
            Ok(cmd) => {
                let request_id = next_request_id();
                let (opcode, payload) = cmd.encode();
                let frame = Frame::new(opcode, request_id, payload);

                framed.send(frame).await?;

                match framed.next().await {
                    Some(Ok(response_frame)) => {
                        let response = Response::from_frame(&response_frame)?;
                        println!("{}", response);
                    }
                    Some(Err(e)) => {
                        eprintln!("Error: {}", e);
                    }
                    None => {
                        eprintln!("Connection closed by server");
                        break;
                    }
                }
            }
            Err(e) => {
                eprintln!("Error: {}", e);
            }
        }
    }

    Ok(())
}

fn parse_command(input: &str) -> anyhow::Result<Command> {
    let parts: Vec<&str> = input.split_whitespace().collect();

    if parts.is_empty() {
        anyhow::bail!("Empty command");
    }

    let cmd = parts[0].to_uppercase();

    match cmd.as_str() {
        "PING" => Ok(Command::Ping),

        "GET" => {
            if parts.len() < 2 {
                anyhow::bail!("GET requires a key: GET <key>");
            }
            Ok(Command::Get {
                key: Bytes::copy_from_slice(parts[1].as_bytes()),
            })
        }

        "SET" => {
            if parts.len() < 3 {
                anyhow::bail!("SET requires key and value: SET <key> <value> [ttl_seconds]");
            }
            let key = Bytes::copy_from_slice(parts[1].as_bytes());
            let value = Bytes::copy_from_slice(parts[2].as_bytes());
            let ttl = if parts.len() > 3 {
                Some(parts[3].parse::<u64>()?)
            } else {
                None
            };
            Ok(Command::Set { key, value, ttl })
        }

        "DEL" => {
            if parts.len() < 2 {
                anyhow::bail!("DEL requires a key: DEL <key>");
            }
            Ok(Command::Del {
                key: Bytes::copy_from_slice(parts[1].as_bytes()),
            })
        }

        "EXISTS" => {
            if parts.len() < 2 {
                anyhow::bail!("EXISTS requires a key: EXISTS <key>");
            }
            Ok(Command::Exists {
                key: Bytes::copy_from_slice(parts[1].as_bytes()),
            })
        }

        _ => anyhow::bail!("Unknown command: {}. Type 'help' for available commands.", cmd),
    }
}

fn print_help() {
    println!(
        r#"
Available commands:

  PING              - Check server connectivity
  GET <key>         - Get value for key
  SET <key> <value> [ttl] - Set key-value pair with optional TTL in seconds
  DEL <key>         - Delete a key
  EXISTS <key>      - Check if key exists

  help              - Show this help
  quit / exit       - Exit the CLI

Examples:
  SET mykey myvalue
  SET tempkey value 60   (expires in 60 seconds)
  GET mykey
  EXISTS mykey
  DEL mykey
"#
    );
}
