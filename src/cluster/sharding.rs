//! Sharding Module
//!
//! Consistent hashing with 16384 slots (Redis-compatible).

use std::collections::HashMap;
use std::sync::RwLock;

use super::node::NodeId;

/// Total number of slots (Redis-compatible)
pub const TOTAL_SLOTS: u16 = 16384;

/// A single slot
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Slot(pub u16);

impl Slot {
    pub fn new(slot: u16) -> Self {
        debug_assert!(slot < TOTAL_SLOTS);
        Self(slot)
    }

    /// Calculate slot from key using CRC16
    pub fn from_key(key: &[u8]) -> Self {
        let hash = crc16(key);
        Self(hash % TOTAL_SLOTS)
    }
}

/// CRC16 implementation (XMODEM)
fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for byte in data {
        crc ^= (*byte as u16) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

/// Slot range (inclusive)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotRange {
    pub start: u16,
    pub end: u16,
}

impl SlotRange {
    pub fn new(start: u16, end: u16) -> Self {
        debug_assert!(start <= end);
        debug_assert!(end < TOTAL_SLOTS);
        Self { start, end }
    }

    pub fn contains(&self, slot: Slot) -> bool {
        slot.0 >= self.start && slot.0 <= self.end
    }

    pub fn count(&self) -> u16 {
        self.end - self.start + 1
    }
}

/// Slot assignment to a node
#[derive(Debug, Clone)]
pub struct SlotAssignment {
    pub node_id: NodeId,
    pub range: SlotRange,
    /// Is this slot migrating?
    pub migrating_to: Option<NodeId>,
    /// Is this slot importing?
    pub importing_from: Option<NodeId>,
}

/// Shard manager
pub struct ShardManager {
    /// Slot to node mapping
    slots: RwLock<[Option<NodeId>; TOTAL_SLOTS as usize]>,
    /// Node to slot ranges mapping
    node_slots: RwLock<HashMap<NodeId, Vec<SlotRange>>>,
    /// Slots being migrated
    migrations: RwLock<HashMap<u16, NodeId>>,
    /// Cluster epoch
    epoch: RwLock<u64>,
}

impl ShardManager {
    pub fn new() -> Self {
        Self {
            slots: RwLock::new([None; TOTAL_SLOTS as usize]),
            node_slots: RwLock::new(HashMap::new()),
            migrations: RwLock::new(HashMap::new()),
            epoch: RwLock::new(0),
        }
    }

    /// Get node for a key
    pub fn get_node_for_key(&self, key: &[u8]) -> Option<NodeId> {
        let slot = Slot::from_key(key);
        self.get_node_for_slot(slot)
    }

    /// Get node for a slot
    pub fn get_node_for_slot(&self, slot: Slot) -> Option<NodeId> {
        let slots = self.slots.read().unwrap();
        slots[slot.0 as usize]
    }

    /// Assign slots to a node
    pub fn assign_slots(&self, node_id: NodeId, range: SlotRange) {
        let mut slots = self.slots.write().unwrap();
        let mut node_slots = self.node_slots.write().unwrap();

        for slot in range.start..=range.end {
            slots[slot as usize] = Some(node_id);
        }

        node_slots.entry(node_id).or_default().push(range);
        *self.epoch.write().unwrap() += 1;
    }

    /// Distribute slots evenly across nodes
    pub fn distribute_slots(&self, nodes: &[NodeId]) {
        if nodes.is_empty() {
            return;
        }

        let slots_per_node = TOTAL_SLOTS as usize / nodes.len();
        let mut extra = TOTAL_SLOTS as usize % nodes.len();

        let mut start = 0u16;
        for &node_id in nodes {
            let count = slots_per_node + if extra > 0 { extra -= 1; 1 } else { 0 };
            let end = start + count as u16 - 1;
            self.assign_slots(node_id, SlotRange::new(start, end));
            start = end + 1;
        }
    }

    /// Start slot migration
    pub fn start_migration(&self, slot: u16, to_node: NodeId) {
        let mut migrations = self.migrations.write().unwrap();
        migrations.insert(slot, to_node);
    }

    /// Complete slot migration
    pub fn complete_migration(&self, slot: u16) {
        let mut migrations = self.migrations.write().unwrap();
        if let Some(to_node) = migrations.remove(&slot) {
            let mut slots = self.slots.write().unwrap();
            slots[slot as usize] = Some(to_node);
            *self.epoch.write().unwrap() += 1;
        }
    }

    /// Cancel slot migration
    pub fn cancel_migration(&self, slot: u16) {
        let mut migrations = self.migrations.write().unwrap();
        migrations.remove(&slot);
    }

    /// Check if slot is migrating
    pub fn is_migrating(&self, slot: u16) -> Option<NodeId> {
        let migrations = self.migrations.read().unwrap();
        migrations.get(&slot).copied()
    }

    /// Get all slots for a node
    pub fn get_node_slots(&self, node_id: NodeId) -> Vec<u16> {
        let slots = self.slots.read().unwrap();
        slots
            .iter()
            .enumerate()
            .filter(|(_, &n)| n == Some(node_id))
            .map(|(i, _)| i as u16)
            .collect()
    }

    /// Get slot count for a node
    pub fn get_node_slot_count(&self, node_id: NodeId) -> usize {
        self.get_node_slots(node_id).len()
    }

    /// Get current epoch
    pub fn epoch(&self) -> u64 {
        *self.epoch.read().unwrap()
    }

    /// Get number of assigned slots
    pub fn assigned_slot_count(&self) -> usize {
        let slots = self.slots.read().unwrap();
        slots.iter().filter(|s| s.is_some()).count()
    }
}

impl Default for ShardManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slot_from_key() {
        let slot = Slot::from_key(b"hello");
        assert!(slot.0 < TOTAL_SLOTS);
    }

    #[test]
    fn test_slot_range() {
        let range = SlotRange::new(0, 5460);
        assert!(range.contains(Slot(0)));
        assert!(range.contains(Slot(5460)));
        assert!(!range.contains(Slot(5461)));
        assert_eq!(range.count(), 5461);
    }

    #[test]
    fn test_distribute_slots() {
        let manager = ShardManager::new();
        manager.distribute_slots(&[1, 2, 3]);

        // All slots should be assigned
        assert_eq!(manager.assigned_slot_count(), TOTAL_SLOTS as usize);

        // Each node should have roughly equal slots
        let node1_slots = manager.get_node_slot_count(1);
        let node2_slots = manager.get_node_slot_count(2);
        let node3_slots = manager.get_node_slot_count(3);

        assert!(node1_slots > 5000);
        assert!(node2_slots > 5000);
        assert!(node3_slots > 5000);
    }

    #[test]
    fn test_migration() {
        let manager = ShardManager::new();
        manager.assign_slots(1, SlotRange::new(0, 100));

        assert_eq!(manager.get_node_for_slot(Slot(50)), Some(1));

        // Start migration
        manager.start_migration(50, 2);
        assert_eq!(manager.is_migrating(50), Some(2));

        // Complete migration
        manager.complete_migration(50);
        assert_eq!(manager.get_node_for_slot(Slot(50)), Some(2));
        assert_eq!(manager.is_migrating(50), None);
    }
}
