//! Replication Manager
//!
//! Manages data replication between nodes.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use super::node::{NodeId, NodeRole};

/// Replication mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationMode {
    /// Asynchronous replication (fast, eventual consistency)
    Async,
    /// Semi-synchronous (wait for at least one replica)
    SemiSync,
    /// Synchronous (wait for all replicas)
    Sync,
}

impl Default for ReplicationMode {
    fn default() -> Self {
        Self::Async
    }
}

/// Replication configuration
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    /// Replication mode
    pub mode: ReplicationMode,
    /// Minimum replicas for semi-sync
    pub min_replicas: usize,
    /// Replication timeout in ms
    pub timeout_ms: u64,
    /// Batch size for replication
    pub batch_size: usize,
    /// Buffer size for replication stream
    pub buffer_size: usize,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            mode: ReplicationMode::Async,
            min_replicas: 1,
            timeout_ms: 1000,
            batch_size: 1000,
            buffer_size: 16 * 1024 * 1024, // 16MB
        }
    }
}

impl ReplicationConfig {
    pub fn with_mode(mut self, mode: ReplicationMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn with_min_replicas(mut self, n: usize) -> Self {
        self.min_replicas = n;
        self
    }
}

/// Replication stream entry
#[derive(Debug, Clone)]
pub struct ReplicationEntry {
    /// Unique sequence number
    pub seq: u64,
    /// Operation type
    pub op: ReplicationOp,
    /// Timestamp
    pub timestamp_ms: u64,
    /// Serialized data
    pub data: Vec<u8>,
}

/// Replication operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationOp {
    Set,
    Del,
    Expire,
}

/// Replica state
#[derive(Debug, Clone)]
pub struct ReplicaState {
    pub node_id: NodeId,
    pub offset: u64,
    pub lag: u64,
    pub last_ack: Instant,
    pub connected: bool,
}

impl ReplicaState {
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            offset: 0,
            lag: 0,
            last_ack: Instant::now(),
            connected: false,
        }
    }

    pub fn update_offset(&mut self, offset: u64, leader_offset: u64) {
        self.offset = offset;
        self.lag = leader_offset.saturating_sub(offset);
        self.last_ack = Instant::now();
    }
}

/// Replication manager
pub struct ReplicationManager {
    /// Configuration
    config: ReplicationConfig,
    /// Current offset (write position)
    offset: AtomicU64,
    /// Replica states
    replicas: RwLock<HashMap<NodeId, ReplicaState>>,
    /// Replication buffer
    buffer: RwLock<Vec<ReplicationEntry>>,
    /// Am I the leader?
    is_leader: RwLock<bool>,
}

impl ReplicationManager {
    pub fn new(config: ReplicationConfig) -> Self {
        Self {
            config,
            offset: AtomicU64::new(0),
            replicas: RwLock::new(HashMap::new()),
            buffer: RwLock::new(Vec::new()),
            is_leader: RwLock::new(false),
        }
    }

    /// Get current offset
    pub fn offset(&self) -> u64 {
        self.offset.load(Ordering::SeqCst)
    }

    /// Add a replica
    pub fn add_replica(&self, node_id: NodeId) {
        let mut replicas = self.replicas.write().unwrap();
        replicas.insert(node_id, ReplicaState::new(node_id));
    }

    /// Remove a replica
    pub fn remove_replica(&self, node_id: NodeId) {
        let mut replicas = self.replicas.write().unwrap();
        replicas.remove(&node_id);
    }

    /// Record a write operation
    pub fn record(&self, op: ReplicationOp, data: Vec<u8>) -> u64 {
        let seq = self.offset.fetch_add(1, Ordering::SeqCst) + 1;

        let entry = ReplicationEntry {
            seq,
            op,
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            data,
        };

        let mut buffer = self.buffer.write().unwrap();
        buffer.push(entry);

        // Trim buffer if too large
        if buffer.len() > self.config.buffer_size / 100 {
            let trim_to = buffer.len() - self.config.buffer_size / 200;
            buffer.drain(0..trim_to);
        }

        seq
    }

    /// Get entries from offset for replication
    pub fn get_entries(&self, from_offset: u64, limit: usize) -> Vec<ReplicationEntry> {
        let buffer = self.buffer.read().unwrap();
        buffer
            .iter()
            .filter(|e| e.seq > from_offset)
            .take(limit)
            .cloned()
            .collect()
    }

    /// Acknowledge replication from a replica
    pub fn ack(&self, node_id: NodeId, offset: u64) {
        let leader_offset = self.offset();
        let mut replicas = self.replicas.write().unwrap();
        if let Some(replica) = replicas.get_mut(&node_id) {
            replica.update_offset(offset, leader_offset);
        }
    }

    /// Get replication lag for a replica
    pub fn get_lag(&self, node_id: NodeId) -> Option<u64> {
        let replicas = self.replicas.read().unwrap();
        replicas.get(&node_id).map(|r| r.lag)
    }

    /// Get total replication lag across all replicas
    pub fn total_lag(&self) -> u64 {
        let replicas = self.replicas.read().unwrap();
        replicas.values().map(|r| r.lag).sum()
    }

    /// Get minimum confirmed offset (for sync replication)
    pub fn min_confirmed_offset(&self) -> u64 {
        let replicas = self.replicas.read().unwrap();
        replicas.values().map(|r| r.offset).min().unwrap_or(0)
    }

    /// Check if writes are durable based on mode
    pub fn is_durable(&self, offset: u64) -> bool {
        match self.config.mode {
            ReplicationMode::Async => true,
            ReplicationMode::SemiSync => {
                let replicas = self.replicas.read().unwrap();
                let confirmed = replicas.values().filter(|r| r.offset >= offset).count();
                confirmed >= self.config.min_replicas
            }
            ReplicationMode::Sync => {
                let replicas = self.replicas.read().unwrap();
                replicas.values().all(|r| r.offset >= offset)
            }
        }
    }

    /// Get replica count
    pub fn replica_count(&self) -> usize {
        self.replicas.read().unwrap().len()
    }

    /// Get healthy replica count
    pub fn healthy_replica_count(&self) -> usize {
        let replicas = self.replicas.read().unwrap();
        replicas.values().filter(|r| r.connected).count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replication_manager() {
        let manager = ReplicationManager::new(ReplicationConfig::default());

        // Record some operations
        let seq1 = manager.record(ReplicationOp::Set, b"key1=value1".to_vec());
        let seq2 = manager.record(ReplicationOp::Set, b"key2=value2".to_vec());

        assert_eq!(seq1, 1);
        assert_eq!(seq2, 2);
        assert_eq!(manager.offset(), 2);
    }

    #[test]
    fn test_replication_lag() {
        let manager = ReplicationManager::new(ReplicationConfig::default());
        manager.add_replica(1);
        manager.add_replica(2);

        // Leader writes
        manager.record(ReplicationOp::Set, b"data".to_vec());
        manager.record(ReplicationOp::Set, b"data".to_vec());

        // Replica 1 acks
        manager.ack(1, 2);
        assert_eq!(manager.get_lag(1), Some(0));

        // Replica 2 hasn't acked
        assert_eq!(manager.get_lag(2), Some(2));
    }

    #[test]
    fn test_durability_check() {
        let config = ReplicationConfig::default().with_mode(ReplicationMode::SemiSync);
        let manager = ReplicationManager::new(config);

        manager.add_replica(1);
        manager.add_replica(2);

        let offset = manager.record(ReplicationOp::Set, b"data".to_vec());

        // Not durable yet
        assert!(!manager.is_durable(offset));

        // One replica acks
        manager.ack(1, offset);
        assert!(manager.is_durable(offset));
    }
}
