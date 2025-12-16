//! Connection Handler
//!
//! Processes VCP frames and dispatches commands.

use crate::metrics::Metrics;
use crate::protocol::{Command, Response, VcpCodec};
use crate::storage::Store;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tracing::debug;

/// Connection handler
pub struct Handler {
    store: Store,
    metrics: Arc<Metrics>,
}

impl Handler {
    /// Create a new handler
    pub fn new(store: Store, metrics: Arc<Metrics>) -> Self {
        Self { store, metrics }
    }

    /// Run the handler for a connection
    pub async fn run(self, mut framed: Framed<TcpStream, VcpCodec>) -> std::io::Result<()> {
        while let Some(result) = framed.next().await {
            let frame = result?;
            let start = Instant::now();

            let request_id = frame.header.request_id;
            let cmd_name = format!("{:?}", frame.header.opcode);

            let response = match Command::from_frame(&frame) {
                Ok(cmd) => self.execute(cmd),
                Err(e) => Response::Error(e.to_string()),
            };

            let response_frame = response.to_frame(request_id);
            framed.send(response_frame).await?;

            let elapsed = start.elapsed();
            self.metrics.record_operation(&cmd_name, elapsed);
            debug!(cmd = %cmd_name, latency = ?elapsed, "Command executed");
        }

        Ok(())
    }

    /// Execute a command and return response
    fn execute(&self, cmd: Command) -> Response {
        match cmd {
            Command::Ping => Response::Pong,

            Command::Get { key } => match self.store.get(&key) {
                Some(value) => Response::Value(value),
                None => Response::Nil,
            },

            Command::Set { key, value, ttl } => {
                self.store.set(key, value, ttl);
                Response::Ok
            }

            Command::Del { key } => {
                let existed = self.store.del(&key);
                Response::Integer(if existed { 1 } else { 0 })
            }

            Command::Exists { key } => {
                let exists = self.store.exists(&key);
                Response::Integer(if exists { 1 } else { 0 })
            }
        }
    }
}
