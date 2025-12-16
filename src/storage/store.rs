//! In-Memory Key-Value Store
//!
//! Thread-safe hashmap with TTL metadata.

use bytes::Bytes;
use hashbrown::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Entry in the store with value and expiration
#[derive(Debug, Clone)]
struct Entry {
    value: Bytes,
    expires_at: Option<Instant>,
}

impl Entry {
    fn new(value: Bytes, ttl: Option<Duration>) -> Self {
        Self {
            value,
            expires_at: ttl.map(|d| Instant::now() + d),
        }
    }

    fn is_expired(&self) -> bool {
        self.expires_at.map(|t| Instant::now() > t).unwrap_or(false)
    }
}

/// Thread-safe in-memory key-value store
#[derive(Debug, Clone)]
pub struct Store {
    inner: Arc<RwLock<HashMap<Bytes, Entry>>>,
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

impl Store {
    /// Create a new empty store
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get value by key, returns None if key doesn't exist or is expired
    pub fn get(&self, key: &Bytes) -> Option<Bytes> {
        let map = self.inner.read().unwrap();
        map.get(key).and_then(|entry| {
            if entry.is_expired() {
                None
            } else {
                Some(entry.value.clone())
            }
        })
    }

    /// Set key-value pair with optional TTL in seconds
    pub fn set(&self, key: Bytes, value: Bytes, ttl_secs: Option<u64>) {
        let ttl = ttl_secs.map(Duration::from_secs);
        let entry = Entry::new(value, ttl);
        let mut map = self.inner.write().unwrap();
        map.insert(key, entry);
    }

    /// Delete key, returns true if key existed
    pub fn del(&self, key: &Bytes) -> bool {
        let mut map = self.inner.write().unwrap();
        map.remove(key).is_some()
    }

    /// Check if key exists and is not expired
    pub fn exists(&self, key: &Bytes) -> bool {
        let map = self.inner.read().unwrap();
        map.get(key).map(|e| !e.is_expired()).unwrap_or(false)
    }

    /// Get the number of keys (including expired)
    pub fn len(&self) -> usize {
        self.inner.read().unwrap().len()
    }

    /// Check if store is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Remove expired keys, returns count of removed keys
    pub fn cleanup_expired(&self) -> usize {
        let mut map = self.inner.write().unwrap();
        let before = map.len();
        map.retain(|_, entry| !entry.is_expired());
        before - map.len()
    }

    /// Get all keys (for debugging/testing)
    pub fn keys(&self) -> Vec<Bytes> {
        let map = self.inner.read().unwrap();
        map.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_basic_operations() {
        let store = Store::new();
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
        let store = Store::new();
        let key = Bytes::from_static(b"expiring");
        let value = Bytes::from_static(b"temporary");

        // Set with 1 second TTL
        store.set(key.clone(), value.clone(), Some(1));
        assert_eq!(store.get(&key), Some(value));

        // Wait for expiration
        thread::sleep(Duration::from_millis(1100));
        assert_eq!(store.get(&key), None);
    }

    #[test]
    fn test_cleanup() {
        let store = Store::new();

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
