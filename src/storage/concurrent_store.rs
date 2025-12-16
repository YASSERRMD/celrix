//! Concurrent In-Memory Key-Value Store
//!
//! Lock-free hashmap using DashMap for high-concurrency operations.

use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Entry in the store with value and expiration
#[derive(Debug, Clone)]
pub struct Entry {
    pub value: Bytes,
    pub expires_at: Option<Instant>,
}

impl Entry {
    pub fn new(value: Bytes, ttl: Option<Duration>) -> Self {
        Self {
            value,
            expires_at: ttl.map(|d| Instant::now() + d),
        }
    }

    pub fn is_expired(&self) -> bool {
        self.expires_at.map(|t| Instant::now() > t).unwrap_or(false)
    }
}

/// Lock-free concurrent in-memory key-value store
/// 
/// Uses DashMap for O(1) concurrent access without global locks.
/// Each shard has its own lock, allowing parallel reads and writes
/// across different keys.
#[derive(Debug, Clone)]
pub struct ConcurrentStore {
    inner: Arc<DashMap<Bytes, Entry>>,
}

impl Default for ConcurrentStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ConcurrentStore {
    /// Create a new empty store
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
        }
    }

    /// Create with specified shard count for better concurrency
    pub fn with_shard_amount(shard_amount: usize) -> Self {
        Self {
            inner: Arc::new(DashMap::with_shard_amount(shard_amount)),
        }
    }

    /// Get value by key, returns None if key doesn't exist or is expired
    #[inline]
    pub fn get(&self, key: &Bytes) -> Option<Bytes> {
        self.inner.get(key).and_then(|entry| {
            if entry.is_expired() {
                None
            } else {
                Some(entry.value.clone())
            }
        })
    }

    /// Set key-value pair with optional TTL in seconds
    #[inline]
    pub fn set(&self, key: Bytes, value: Bytes, ttl_secs: Option<u64>) {
        let ttl = ttl_secs.map(Duration::from_secs);
        let entry = Entry::new(value, ttl);
        self.inner.insert(key, entry);
    }

    /// Delete key, returns true if key existed
    #[inline]
    pub fn del(&self, key: &Bytes) -> bool {
        self.inner.remove(key).is_some()
    }

    /// Check if key exists and is not expired
    #[inline]
    pub fn exists(&self, key: &Bytes) -> bool {
        self.inner
            .get(key)
            .map(|e| !e.is_expired())
            .unwrap_or(false)
    }

    /// Get the number of keys (including expired - approximate)
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Check if store is empty
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Remove expired keys, returns count of removed keys
    pub fn cleanup_expired(&self) -> usize {
        let mut removed = 0;
        self.inner.retain(|_, entry| {
            if entry.is_expired() {
                removed += 1;
                false
            } else {
                true
            }
        });
        removed
    }

    /// Get all keys (for debugging/testing)
    pub fn keys(&self) -> Vec<Bytes> {
        self.inner.iter().map(|r| r.key().clone()).collect()
    }

    /// Get estimated shard count for diagnostics
    pub fn shards(&self) -> usize {
        // DashMap 6.x: shards() is private, estimate based on CPU count
        num_cpus::get() * 4
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_basic_operations() {
        let store = ConcurrentStore::new();
        let key = Bytes::from_static(b"key");
        let value = Bytes::from_static(b"value");

        // Set and get
        store.set(key.clone(), value.clone(), None);
        assert_eq!(store.get(&key), Some(value.clone()));

        // Exists
        assert!(store.exists(&key));

        // Delete
        assert!(store.del(&key));
        assert!(!store.exists(&key));
        assert_eq!(store.get(&key), None);
    }

    #[test]
    fn test_ttl_expiration() {
        let store = ConcurrentStore::new();
        let key = Bytes::from_static(b"expiring");
        let value = Bytes::from_static(b"temporary");

        // Set with very short TTL
        store.set(key.clone(), value.clone(), Some(1));
        assert_eq!(store.get(&key), Some(value));

        // Wait for expiration
        thread::sleep(Duration::from_millis(1100));
        assert_eq!(store.get(&key), None);
    }

    #[test]
    fn test_concurrent_access() {
        let store = ConcurrentStore::new();
        let store_clone = store.clone();

        // Spawn multiple threads writing concurrently
        let handles: Vec<_> = (0..10)
            .map(|i| {
                let s = store_clone.clone();
                thread::spawn(move || {
                    for j in 0..100 {
                        let key = Bytes::from(format!("key-{}-{}", i, j));
                        let value = Bytes::from(format!("value-{}-{}", i, j));
                        s.set(key.clone(), value.clone(), None);
                        assert!(s.exists(&key));
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(store.len(), 1000);
    }

    #[test]
    fn test_cleanup() {
        let store = ConcurrentStore::new();

        // Add some keys with short TTL
        for i in 0..10 {
            let key = Bytes::from(format!("key{}", i));
            let value = Bytes::from(format!("value{}", i));
            store.set(key, value, Some(0)); // Expires immediately
        }

        thread::sleep(Duration::from_millis(100));
        let removed = store.cleanup_expired();
        assert_eq!(removed, 10);
        assert!(store.is_empty());
    }
}
