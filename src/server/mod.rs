//! Server Module
//!
//! TCP server for handling VCP protocol connections.
//! Supports both single-threaded and multi-threaded modes.

mod buffer_pool;
mod command_queue;
mod config;
mod handler;
mod worker_pool;

pub use buffer_pool::BufferPool;
pub use command_queue::{CommandQueue, WorkItem, WorkResult};
pub use config::Config;
pub use handler::Handler;
pub use worker_pool::{WorkerPool, WorkerPoolConfig};

use crate::metrics::Metrics;
use crate::protocol::VcpCodec;
use crate::storage::{ConcurrentStore, ConcurrentTtlCleaner, Store, TtlCleaner};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_util::codec::Framed;
use tracing::{error, info};

/// CELRIX Server (Single-threaded mode - Phase 1 compatibility)
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

/// CELRIX Concurrent Server (Multi-threaded mode - Phase 2)
pub struct ConcurrentServer {
    config: Config,
    store: ConcurrentStore,
    metrics: Arc<Metrics>,
    worker_config: WorkerPoolConfig,
}

impl ConcurrentServer {
    /// Create a new concurrent server with the given configuration
    pub fn new(config: Config) -> Self {
        Self::with_worker_config(config, WorkerPoolConfig::default())
    }

    /// Create with custom worker pool configuration
    pub fn with_worker_config(config: Config, worker_config: WorkerPoolConfig) -> Self {
        // Create store with shard count matching worker count
        let num_shards = if worker_config.num_workers == 0 {
            num_cpus::get() * 4 // 4 shards per worker for reduced contention
        } else {
            worker_config.num_workers * 4
        };

        Self {
            config,
            store: ConcurrentStore::with_shard_amount(num_shards),
            metrics: Arc::new(Metrics::new()),
            worker_config,
        }
    }

    /// Run the concurrent server
    pub async fn run(self) -> std::io::Result<()> {
        let addr = format!("{}:{}", self.config.bind, self.config.port);
        let listener = TcpListener::bind(&addr).await?;

        let num_workers = if self.worker_config.num_workers == 0 {
            num_cpus::get()
        } else {
            self.worker_config.num_workers
        };

        info!(
            "CELRIX concurrent server listening on {} with {} workers",
            addr, num_workers
        );

        // Start TTL cleaner for concurrent store
        ConcurrentTtlCleaner::spawn(self.store.clone(), self.config.ttl_cleaner_interval);

        // Start worker pool
        let mut worker_pool = WorkerPool::new(
            self.worker_config.clone(),
            self.store.clone(),
            self.metrics.clone(),
        );
        worker_pool.start();

        // Get queue reference for handlers
        let queue = worker_pool.queue().clone();

        loop {
            match listener.accept().await {
                Ok((socket, peer_addr)) => {
                    info!("New connection from {}", peer_addr);

                    let q = queue.clone();
                    let _metrics = self.metrics.clone();

                    tokio::spawn(async move {
                        let framed = Framed::new(socket, VcpCodec::new());
                        let handler = ConcurrentHandler::new(q);

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

    /// Get a reference to the store
    pub fn store(&self) -> &ConcurrentStore {
        &self.store
    }

    /// Get metrics reference
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }
}

/// Handler for concurrent server that routes to worker pool
pub struct ConcurrentHandler {
    queue: CommandQueue,
}

impl ConcurrentHandler {
    pub fn new(queue: CommandQueue) -> Self {
        Self { queue }
    }

    pub async fn run(
        self,
        mut framed: Framed<tokio::net::TcpStream, VcpCodec>,
    ) -> std::io::Result<()> {
        use crate::protocol::{Command, Response};
        use futures::{SinkExt, StreamExt};

        while let Some(result) = framed.next().await {
            let frame = result?;
            let request_id = frame.header.request_id;

            match Command::from_frame(&frame) {
                Ok(cmd) => {
                    // Create oneshot channel for response
                    let (tx, rx) = tokio::sync::oneshot::channel();

                    let work_item = WorkItem {
                        command: cmd,
                        request_id,
                        response_tx: tx,
                    };

                    // Send to worker pool
                    if self.queue.send(work_item).is_err() {
                        let response = Response::Error("Queue full".to_string());
                        let response_frame = response.to_frame(request_id);
                        framed.send(response_frame).await?;
                        continue;
                    }

                    // Wait for response
                    match rx.await {
                        Ok(result) => {
                            let response = match result {
                                WorkResult::Ok => Response::Ok,
                                WorkResult::Value(v) => Response::Value(v),
                                WorkResult::Integer(i) => Response::Integer(i),
                                WorkResult::Nil => Response::Nil,
                                WorkResult::Error(e) => Response::Error(e),
                                WorkResult::Pong => Response::Pong,
                            };
                            let response_frame = response.to_frame(request_id);
                            framed.send(response_frame).await?;
                        }
                        Err(_) => {
                            let response = Response::Error("Worker error".to_string());
                            let response_frame = response.to_frame(request_id);
                            framed.send(response_frame).await?;
                        }
                    }
                }
                Err(e) => {
                    let response = Response::Error(e.to_string());
                    let response_frame = response.to_frame(request_id);
                    framed.send(response_frame).await?;
                }
            }
        }

        Ok(())
    }
}
