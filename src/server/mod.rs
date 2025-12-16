//! Server Module
//!
//! TCP server for handling VCP protocol connections.

mod config;
mod handler;

pub use config::Config;
pub use handler::Handler;

use crate::metrics::Metrics;
use crate::protocol::VcpCodec;
use crate::storage::{Store, TtlCleaner};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_util::codec::Framed;
use tracing::{error, info};

/// CELRIX Server
pub struct Server {
    config: Config,
    store: Store,
    metrics: Arc<Metrics>,
}

impl Server {
    /// Create a new server with the given configuration
    pub fn new(config: Config) -> Self {
        Self {
            config,
            store: Store::new(),
            metrics: Arc::new(Metrics::new()),
        }
    }

    /// Run the server
    pub async fn run(self) -> std::io::Result<()> {
        let addr = format!("{}:{}", self.config.bind, self.config.port);
        let listener = TcpListener::bind(&addr).await?;

        info!("CELRIX server listening on {}", addr);

        // Start TTL cleaner
        TtlCleaner::spawn(self.store.clone(), self.config.ttl_cleaner_interval);

        loop {
            match listener.accept().await {
                Ok((socket, peer_addr)) => {
                    info!("New connection from {}", peer_addr);

                    let store = self.store.clone();
                    let metrics = self.metrics.clone();

                    tokio::spawn(async move {
                        let framed = Framed::new(socket, VcpCodec::new());
                        let handler = Handler::new(store, metrics);

                        if let Err(e) = handler.run(framed).await {
                            error!("Connection error from {}: {}", peer_addr, e);
                        }

                        info!("Connection closed: {}", peer_addr);
                    });
                }
                Err(e) => {
                    error!("Accept error: {}", e);
                }
            }
        }
    }

    /// Get a reference to the store (for testing)
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Get metrics reference
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }
}
