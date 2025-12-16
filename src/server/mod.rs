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
use bytes::Bytes;
use crate::storage::{ConcurrentStore, ConcurrentTtlCleaner, Store, TtlCleaner};
use crate::vector::SemanticCache;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_util::codec::Framed;
use tracing::{error, info};

/// CELRIX Server (Single-threaded mode - Phase 1 compatibility)
pub struct Server {
    config: Config,
    store: Store,
    vector_store: SemanticCache,
    metrics: Arc<Metrics>,
}

impl Server {
    /// Create a new server with the given configuration
    pub fn new(config: Config) -> Self {
        Self {
            config,
            store: Store::new(),
            vector_store: SemanticCache::with_defaults(),
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
                    let vector_store = self.vector_store.clone();
                    let metrics = self.metrics.clone();

                    tokio::spawn(async move {
                        let framed = Framed::new(socket, VcpCodec::new());
                        let handler = Handler::new(store, vector_store, metrics);

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
    vector_store: SemanticCache,
    metrics: Arc<Metrics>,
    // worker_config removed, superseded by Config fields
}

impl ConcurrentServer {
    /// Create a new concurrent server with the given configuration
    pub fn new(config: Config) -> Self {
        Self::with_worker_config(config, WorkerPoolConfig::default())
    }

    /// Create with custom worker pool configuration
    pub fn with_worker_config(config: Config, _worker_config: WorkerPoolConfig) -> Self {
        // Create store with shard count matching KV worker count or num_cpus
        let num_started_workers = if config.kv_workers == 0 { num_cpus::get() } else { config.kv_workers };
        let target_shards = num_started_workers * 4;
        
        // DashMap requires power of two
        let num_shards = target_shards.next_power_of_two();

        Self {
            config,
            store: ConcurrentStore::with_shard_amount(num_shards),
            vector_store: SemanticCache::with_defaults(),
            metrics: Arc::new(Metrics::new()),
        }
    }

    /// Run the concurrent server
    pub async fn run(self) -> std::io::Result<()> {
        let addr = format!("{}:{}", self.config.bind, self.config.port);
        let listener = TcpListener::bind(&addr).await?;

        // Determine worker counts
        let num_kv_workers = if self.config.kv_workers == 0 {
            num_cpus::get()
        } else {
            self.config.kv_workers
        };
        
        let num_vector_workers = if self.config.vector_workers == 0 {
            4 // Default safe fallback
        } else {
            self.config.vector_workers
        };

        info!(
            "CELRIX concurrent server listening on {}. KV Workers: {}, Vector Workers: {}",
            addr, num_kv_workers, num_vector_workers
        );

        // Start TTL cleaner for concurrent store
        ConcurrentTtlCleaner::spawn(self.store.clone(), self.config.ttl_cleaner_interval);

        // --- KV POOL ---
        let mut kv_pool_config = WorkerPoolConfig::default();
        kv_pool_config.num_workers = num_kv_workers;
        kv_pool_config.pin_to_cores = true; // Pin KV workers for low latency

        let mut kv_pool = WorkerPool::new(
            kv_pool_config,
            self.store.clone(),
            self.vector_store.clone(),
            self.metrics.clone(),
        );
        kv_pool.start();
        let kv_queue = kv_pool.queue().clone();

        // --- VECTOR POOL ---
        let mut vector_pool_config = WorkerPoolConfig::default();
        vector_pool_config.num_workers = num_vector_workers;
        vector_pool_config.pin_to_cores = false; // Don't pin vector workers to allow OS scheduling freedom for heavy compute

        let mut vector_pool = WorkerPool::new(
            vector_pool_config,
            self.store.clone(),
            self.vector_store.clone(),
            self.metrics.clone(),
        );
        vector_pool.start();
        let vector_queue = vector_pool.queue().clone();

        loop {
            match listener.accept().await {
                Ok((socket, peer_addr)) => {
                    info!("New connection from {}", peer_addr);

                    let kv_q = kv_queue.clone();
                    let vec_q = vector_queue.clone();
                    // ... metrics

                    tokio::spawn(async move {
                        let framed = Framed::new(socket, VcpCodec::new());
                        let handler = ConcurrentHandler::new(kv_q, vec_q);

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
    kv_queue: CommandQueue,
    vector_queue: CommandQueue,
}

impl ConcurrentHandler {
    pub fn new(kv_queue: CommandQueue, vector_queue: CommandQueue) -> Self {
        Self { kv_queue, vector_queue }
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

                    // Decide target queue before moving cmd
                    let target_queue = match cmd {
                        Command::VAdd { .. } | Command::VSearch { .. } => &self.vector_queue,
                        _ => &self.kv_queue,
                    };

                    let work_item = WorkItem {
                        command: cmd,
                        request_id,
                        response_tx: tx,
                    };

                    if target_queue.send(work_item).is_err() {
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
                                WorkResult::Array(items) => {
                                    // Map WorkResult values to Bytes for Response::Array
                                    let mut resp_items = Vec::with_capacity(items.len());
                                    for item in items {
                                        if let WorkResult::Value(val) = item {
                                            resp_items.push(val);
                                        } else {
                                            // Fallback for non-value items in array if any
                                            resp_items.push(Bytes::from(format!("{:?}", item)));
                                        }
                                    }
                                    Response::Array(resp_items)
                                }
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
