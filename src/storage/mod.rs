//! Storage Engine
//!
//! In-memory key-value store with TTL support.

mod store;
mod ttl;

pub use store::Store;
pub use ttl::TtlCleaner;
