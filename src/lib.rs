//! CELRIX - High-Performance In-Memory Cache Database
//!
//! A 5-10x faster Redis alternative built with lock-free architecture
//! and custom binary protocol (VCP - Velocity Cache Protocol).

pub mod metrics;
pub mod observability;
pub mod persistence;
pub mod protocol;
pub mod server;
pub mod storage;
pub mod vector;

pub use metrics::Metrics;
pub use observability::{HealthCheck, PrometheusExporter};
pub use persistence::{AofWriter, Snapshot, SnapshotConfig};
pub use protocol::{Command, ExtendedCommand, Frame, Response, VcpCodec};
pub use server::{ConcurrentServer, Config, Server, WorkerPoolConfig};
pub use storage::{ConcurrentStore, EvictionConfig, EvictionPolicy, Store};
pub use vector::{EmbeddingStore, SemanticCache};
