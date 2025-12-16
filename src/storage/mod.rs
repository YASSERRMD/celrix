//! Storage Engine
//!
//! In-memory key-value store with TTL support and eviction policies.

mod concurrent_store;
mod concurrent_ttl;
mod eviction;
mod store;
mod ttl;

pub use concurrent_store::ConcurrentStore;
pub use concurrent_ttl::ConcurrentTtlCleaner;
pub use eviction::{EvictionConfig, EvictionPolicy, LruManager};
pub use store::Store;
pub use ttl::TtlCleaner;
