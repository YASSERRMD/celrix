//! Worker Pool
//!
//! Multi-threaded worker pool with CPU core affinity.

use std::sync::Arc;
use std::thread::{self, JoinHandle};
use tracing::{debug, info};

use crate::metrics::Metrics;
use crate::protocol::Command;
use crate::storage::ConcurrentStore;
use crate::vector::{SemanticCache, SemanticResult};

use super::command_queue::{CommandQueue, WorkItem, WorkResult};

/// Worker pool configuration
#[derive(Debug, Clone)]
pub struct WorkerPoolConfig {
    /// Number of worker threads (0 = auto-detect)
    pub num_workers: usize,
    /// Whether to pin workers to CPU cores
    pub pin_to_cores: bool,
    /// Command queue capacity
    pub queue_capacity: usize,
}

impl Default for WorkerPoolConfig {
    fn default() -> Self {
        Self {
            num_workers: num_cpus::get(),
            pin_to_cores: true,
            queue_capacity: 10000,
        }
    }
}

/// Multi-threaded worker pool
pub struct WorkerPool {
    config: WorkerPoolConfig,
    queue: CommandQueue,
    store: ConcurrentStore,
    vector_store: SemanticCache,
    metrics: Arc<Metrics>,
    handles: Vec<JoinHandle<()>>,
}

impl WorkerPool {
    /// Create a new worker pool
    pub fn new(
        config: WorkerPoolConfig,
        store: ConcurrentStore,
        vector_store: SemanticCache,
        metrics: Arc<Metrics>,
    ) -> Self {
        let queue = CommandQueue::new(config.queue_capacity);
        Self {
            config,
            queue,
            store,
            vector_store,
            metrics,
            handles: Vec::new(),
        }
    }

    /// Start the worker threads
    pub fn start(&mut self) {
        let num_workers = if self.config.num_workers == 0 {
            num_cpus::get()
        } else {
            self.config.num_workers
        };

        info!("Starting {} worker threads", num_workers);

        let core_ids = if self.config.pin_to_cores {
            core_affinity::get_core_ids().unwrap_or_default()
        } else {
            Vec::new()
        };

        for i in 0..num_workers {
            let receiver = self.queue.receiver();
                    let store = self.store.clone();
                    let vector_store = self.vector_store.clone();
                    let metrics = self.metrics.clone();
                    // ... (pinning logc)
                    let core_id = if self.config.pin_to_cores && i < core_ids.len() {
                Some(core_ids[i])
            } else {
                None
            };

            let handle = thread::Builder::new()
                .name(format!("worker-{}", i))
                .spawn(move || {
                    // Pin to core if configured
                    if let Some(core) = core_id {
                        if core_affinity::set_for_current(core) {
                            debug!("Worker {} pinned to core {:?}", i, core);
                        }
                    }

                    info!("Worker {} started", i);
                    Self::worker_loop(i, receiver, store, vector_store, metrics);
                    info!("Worker {} stopped", i);
                })
                .expect("Failed to spawn worker thread");

            self.handles.push(handle);
        }
    }

    /// Get a handle to the command queue for submitting work
    pub fn queue(&self) -> &CommandQueue {
        &self.queue
    }

    /// Worker main loop
    fn worker_loop(
        worker_id: usize,
        receiver: crossbeam::channel::Receiver<WorkItem>,
        store: ConcurrentStore,
        vector_store: SemanticCache,
        metrics: Arc<Metrics>,
    ) {
        while let Ok(work_item) = receiver.recv() {
            let start = std::time::Instant::now();
            let cmd_name = format!("{:?}", work_item.command);

            let result = Self::execute_command(&store, &vector_store, work_item.command);

            // Send response back
            if work_item.response_tx.send(result).is_err() {
                debug!("Worker {}: Response channel closed", worker_id);
            }

            let elapsed = start.elapsed();
            metrics.record_operation(&cmd_name, elapsed);
        }
    }

    /// Execute a command against the store
    fn execute_command(store: &ConcurrentStore, vector_store: &SemanticCache, cmd: Command) -> WorkResult {
        match cmd {
            Command::Ping => WorkResult::Pong,

            Command::Get { key } => match store.get(&key) {
                Some(value) => WorkResult::Value(value),
                None => WorkResult::Nil,
            },

            Command::Set { key, value, ttl } => {
                store.set(key, value, ttl);
                WorkResult::Ok
            }

            Command::Del { key } => {
                let existed = store.del(&key);
                WorkResult::Integer(if existed { 1 } else { 0 })
            }

            Command::Exists { key } => {
                let exists = store.exists(&key);
                WorkResult::Integer(if exists { 1 } else { 0 })
            }

            Command::VAdd { key, vector } => {
                // For VADD, we need a value. For now using empty value or key as value.
                // The protocol command VAdd only has key and vector.
                // EmbeddingStore assumes Set takes (key, entry).
                // SemanticCache::set takes (key, embedding, value, metadata).
                // We'll use the key as the "value" payload for now, or empty bytes.
                let value = key.clone(); 
                match vector_store.set(key, vector, value, None) {
                    Ok(_) => WorkResult::Ok,
                    Err(e) => WorkResult::Error(e),
                }
            }

            Command::VSearch { vector, k } => {
                let results = vector_store.semantic_get(&vector);
                
                // Return array of keys
                let mut array = Vec::with_capacity(results.len());
                for res in results {
                    // For now just return the key.
                    // Ideally we return [key, score] pairs, but simple key list is OK for verification.
                    array.push(WorkResult::Value(res.key));
                }
                WorkResult::Array(array)
            }
        }
    }

    /// Wait for all workers to finish
    pub fn join(self) {
        for handle in self.handles {
            let _ = handle.join();
        }
    }

    /// Get number of workers
    pub fn num_workers(&self) -> usize {
        self.handles.len()
    }
}
