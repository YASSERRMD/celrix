//! Eviction Module
//!
//! LRU/LFU eviction policies with memory limits.

use bytes::Bytes;
use dashmap::DashMap;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::RwLock;
use std::time::Instant;

/// Eviction policy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPolicy {
    /// No eviction (default)
    None,
    /// Least Recently Used
    Lru,
    /// Least Frequently Used
    Lfu,
    /// Random eviction
    Random,
}

impl Default for EvictionPolicy {
    fn default() -> Self {
        Self::None
    }
}

/// Eviction configuration
#[derive(Debug, Clone)]
pub struct EvictionConfig {
    /// Maximum memory in bytes (0 = unlimited)
    pub max_memory: usize,
    /// Maximum number of keys (0 = unlimited)
    pub max_keys: usize,
    /// Eviction policy
    pub policy: EvictionPolicy,
    /// Number of samples for random eviction
    pub sample_size: usize,
}

impl Default for EvictionConfig {
    fn default() -> Self {
        Self {
            max_memory: 0,
            max_keys: 0,
            policy: EvictionPolicy::None,
            sample_size: 5,
        }
    }
}

impl EvictionConfig {
    pub fn with_max_memory(mut self, bytes: usize) -> Self {
        self.max_memory = bytes;
        self
    }

    pub fn with_max_keys(mut self, keys: usize) -> Self {
        self.max_keys = keys;
        self
    }

    pub fn with_policy(mut self, policy: EvictionPolicy) -> Self {
        self.policy = policy;
        self
    }
}

/// Entry metadata for eviction tracking
#[derive(Debug, Clone)]
pub struct EvictionMeta {
    /// Last access time
    pub last_access: Instant,
    /// Access count (for LFU)
    pub access_count: u64,
    /// Approximate size in bytes
    pub size: usize,
}

impl EvictionMeta {
    pub fn new(size: usize) -> Self {
        Self {
            last_access: Instant::now(),
            access_count: 1,
            size,
        }
    }

    pub fn touch(&mut self) {
        self.last_access = Instant::now();
        self.access_count = self.access_count.saturating_add(1);
    }
}

/// LRU Eviction Manager
pub struct LruManager {
    /// Access order tracking (most recent at back)
    order: RwLock<VecDeque<Bytes>>,
    /// Metadata per key
    meta: DashMap<Bytes, EvictionMeta>,
    /// Current memory usage
    memory_used: AtomicUsize,
    /// Configuration
    config: EvictionConfig,
}

impl LruManager {
    pub fn new(config: EvictionConfig) -> Self {
        Self {
            order: RwLock::new(VecDeque::new()),
            meta: DashMap::new(),
            memory_used: AtomicUsize::new(0),
            config,
        }
    }

    /// Record a key access (for LRU ordering)
    pub fn touch(&self, key: &Bytes, size: usize) {
        // Update metadata
        if let Some(mut entry) = self.meta.get_mut(key) {
            entry.touch();
        } else {
            self.meta.insert(key.clone(), EvictionMeta::new(size));
            self.memory_used.fetch_add(size, Ordering::Relaxed);
        }

        // Update LRU order
        let mut order = self.order.write().unwrap();
        // Remove old position
        order.retain(|k| k != key);
        // Add to back (most recent)
        order.push_back(key.clone());
    }

    /// Record key removal
    pub fn remove(&self, key: &Bytes) {
        if let Some((_, meta)) = self.meta.remove(key) {
            self.memory_used.fetch_sub(meta.size, Ordering::Relaxed);
        }
        let mut order = self.order.write().unwrap();
        order.retain(|k| k != key);
    }

    /// Check if eviction is needed
    pub fn needs_eviction(&self) -> bool {
        let key_count = self.meta.len();
        let memory = self.memory_used.load(Ordering::Relaxed);

        (self.config.max_keys > 0 && key_count >= self.config.max_keys)
            || (self.config.max_memory > 0 && memory >= self.config.max_memory)
    }

    /// Get keys to evict (returns up to `count` keys)
    pub fn get_eviction_candidates(&self, count: usize) -> Vec<Bytes> {
        match self.config.policy {
            EvictionPolicy::None => Vec::new(),
            EvictionPolicy::Lru => self.get_lru_candidates(count),
            EvictionPolicy::Lfu => self.get_lfu_candidates(count),
            EvictionPolicy::Random => self.get_random_candidates(count),
        }
    }

    fn get_lru_candidates(&self, count: usize) -> Vec<Bytes> {
        let order = self.order.read().unwrap();
        order.iter().take(count).cloned().collect()
    }

    fn get_lfu_candidates(&self, count: usize) -> Vec<Bytes> {
        let mut entries: Vec<_> = self
            .meta
            .iter()
            .map(|r| (r.key().clone(), r.value().access_count))
            .collect();
        entries.sort_by_key(|(_, count)| *count);
        entries.into_iter().take(count).map(|(k, _)| k).collect()
    }

    fn get_random_candidates(&self, count: usize) -> Vec<Bytes> {
        let keys: Vec<_> = self.meta.iter().take(count).map(|r| r.key().clone()).collect();
        keys
    }

    /// Get current memory usage
    pub fn memory_used(&self) -> usize {
        self.memory_used.load(Ordering::Relaxed)
    }

    /// Get number of tracked keys
    pub fn key_count(&self) -> usize {
        self.meta.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_ordering() {
        let config = EvictionConfig::default()
            .with_max_keys(3)
            .with_policy(EvictionPolicy::Lru);
        let manager = LruManager::new(config);

        // Add keys
        manager.touch(&Bytes::from_static(b"a"), 10);
        manager.touch(&Bytes::from_static(b"b"), 10);
        manager.touch(&Bytes::from_static(b"c"), 10);

        // Touch 'a' again (making it most recent)
        manager.touch(&Bytes::from_static(b"a"), 10);

        // LRU should be 'b' (oldest)
        let candidates = manager.get_eviction_candidates(1);
        assert_eq!(candidates[0].as_ref(), b"b");
    }

    #[test]
    fn test_needs_eviction() {
        let config = EvictionConfig::default()
            .with_max_keys(2)
            .with_policy(EvictionPolicy::Lru);
        let manager = LruManager::new(config);

        manager.touch(&Bytes::from_static(b"a"), 10);
        assert!(!manager.needs_eviction());

        manager.touch(&Bytes::from_static(b"b"), 10);
        assert!(manager.needs_eviction());
    }
}
