//! CELRIX - High-Performance In-Memory Cache Database
//!
//! A 5-10x faster Redis alternative built with lock-free architecture
//! and custom binary protocol (VCP - Velocity Cache Protocol).

pub mod cluster;
pub mod metrics;
pub mod observability;
pub mod persistence;
pub mod protocol;
pub mod security;
pub mod server;
pub mod storage;
pub mod vector;

pub use cluster::{Node, RaftNode, ReplicationManager, ShardManager};
pub use metrics::Metrics;
pub use observability::{AdminApi, Benchmark, HealthCheck, LoadTestStats, PrometheusExporter};
pub use persistence::{AofWriter, Snapshot, SnapshotConfig};
pub use protocol::{Command, ExtendedCommand, Frame, Response, VcpCodec};
pub use security::{AclManager, AuthManager, AuditLogger, TlsConfig};
pub use server::{ConcurrentServer, Config, Server, WorkerPoolConfig};
pub use storage::{ConcurrentStore, EvictionConfig, EvictionPolicy, Store};
pub use vector::{EmbeddingStore, SemanticCache};
