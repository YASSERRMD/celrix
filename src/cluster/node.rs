//! Cluster Node
//!
//! Represents a node in the CELRIX cluster.

use std::net::SocketAddr;
use std::time::Instant;

/// Unique node identifier
pub type NodeId = u64;

/// Node role in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    /// Leader node (handles writes)
    Leader,
    /// Follower node (replica)
    Follower,
    /// Candidate (during election)
    Candidate,
    /// Learner (non-voting replica)
    Learner,
}

impl Default for NodeRole {
    fn default() -> Self {
        Self::Follower
    }
}

/// Node health state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    /// Node is healthy and responding
    Healthy,
    /// Node is suspected to be down
    Suspect,
    /// Node is confirmed down
    Down,
    /// Node is syncing data
    Syncing,
    /// Node is in maintenance mode
    Maintenance,
}

impl Default for NodeState {
    fn default() -> Self {
        Self::Healthy
    }
}

/// Cluster node information
#[derive(Debug, Clone)]
pub struct Node {
    /// Unique node ID
    pub id: NodeId,
    /// Node address for client connections
    pub addr: SocketAddr,
    /// Address for cluster communication
    pub cluster_addr: SocketAddr,
    /// Current role
    pub role: NodeRole,
    /// Current state
    pub state: NodeState,
    /// Last heartbeat received
    pub last_heartbeat: Instant,
    /// Replication offset (bytes replicated)
    pub replication_offset: u64,
    /// Node priority (higher = more likely to become leader)
    pub priority: u32,
    /// Node tags/labels
    pub tags: Vec<String>,
}

impl Node {
    pub fn new(id: NodeId, addr: SocketAddr, cluster_addr: SocketAddr) -> Self {
        Self {
            id,
            addr,
            cluster_addr,
            role: NodeRole::Follower,
            state: NodeState::Healthy,
            last_heartbeat: Instant::now(),
            replication_offset: 0,
            priority: 100,
            tags: Vec::new(),
        }
    }

    /// Create a leader node
    pub fn leader(id: NodeId, addr: SocketAddr, cluster_addr: SocketAddr) -> Self {
        let mut node = Self::new(id, addr, cluster_addr);
        node.role = NodeRole::Leader;
        node
    }

    /// Check if node is leader
    pub fn is_leader(&self) -> bool {
        self.role == NodeRole::Leader
    }

    /// Check if node is healthy
    pub fn is_healthy(&self) -> bool {
        self.state == NodeState::Healthy
    }

    /// Update heartbeat timestamp
    pub fn heartbeat(&mut self) {
        self.last_heartbeat = Instant::now();
        if self.state == NodeState::Suspect {
            self.state = NodeState::Healthy;
        }
    }

    /// Check if node is suspected down (no heartbeat in timeout)
    pub fn is_suspect(&self, timeout_ms: u64) -> bool {
        self.last_heartbeat.elapsed().as_millis() as u64 > timeout_ms
    }

    /// Mark node as down
    pub fn mark_down(&mut self) {
        self.state = NodeState::Down;
    }

    /// Calculate replication lag from leader offset
    pub fn replication_lag(&self, leader_offset: u64) -> u64 {
        leader_offset.saturating_sub(self.replication_offset)
    }

    /// Add a tag
    pub fn with_tag(mut self, tag: &str) -> Self {
        self.tags.push(tag.to_string());
        self
    }

    /// Set priority
    pub fn with_priority(mut self, priority: u32) -> Self {
        self.priority = priority;
        self
    }
}

/// Cluster topology
#[derive(Debug, Clone)]
pub struct ClusterTopology {
    /// All nodes in the cluster
    pub nodes: Vec<Node>,
    /// Current leader ID (if known)
    pub leader_id: Option<NodeId>,
    /// Cluster epoch (incremented on topology changes)
    pub epoch: u64,
}

impl ClusterTopology {
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            leader_id: None,
            epoch: 0,
        }
    }

    /// Add a node
    pub fn add_node(&mut self, node: Node) {
        if node.is_leader() {
            self.leader_id = Some(node.id);
        }
        self.nodes.push(node);
        self.epoch += 1;
    }

    /// Remove a node
    pub fn remove_node(&mut self, node_id: NodeId) -> Option<Node> {
        if let Some(pos) = self.nodes.iter().position(|n| n.id == node_id) {
            self.epoch += 1;
            if self.leader_id == Some(node_id) {
                self.leader_id = None;
            }
            Some(self.nodes.remove(pos))
        } else {
            None
        }
    }

    /// Get node by ID
    pub fn get_node(&self, node_id: NodeId) -> Option<&Node> {
        self.nodes.iter().find(|n| n.id == node_id)
    }

    /// Get mutable node by ID
    pub fn get_node_mut(&mut self, node_id: NodeId) -> Option<&mut Node> {
        self.nodes.iter_mut().find(|n| n.id == node_id)
    }

    /// Get leader node
    pub fn leader(&self) -> Option<&Node> {
        self.leader_id.and_then(|id| self.get_node(id))
    }

    /// Get all healthy followers
    pub fn healthy_followers(&self) -> Vec<&Node> {
        self.nodes
            .iter()
            .filter(|n| n.role == NodeRole::Follower && n.is_healthy())
            .collect()
    }

    /// Count healthy nodes
    pub fn healthy_count(&self) -> usize {
        self.nodes.iter().filter(|n| n.is_healthy()).count()
    }

    /// Check if quorum is available
    pub fn has_quorum(&self) -> bool {
        let total = self.nodes.len();
        let healthy = self.healthy_count();
        healthy > total / 2
    }
}

impl Default for ClusterTopology {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn test_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
    }

    #[test]
    fn test_node_roles() {
        let mut node = Node::new(1, test_addr(6380), test_addr(16380));
        assert_eq!(node.role, NodeRole::Follower);
        assert!(!node.is_leader());

        node.role = NodeRole::Leader;
        assert!(node.is_leader());
    }

    #[test]
    fn test_cluster_topology() {
        let mut topology = ClusterTopology::new();

        topology.add_node(Node::leader(1, test_addr(6380), test_addr(16380)));
        topology.add_node(Node::new(2, test_addr(6381), test_addr(16381)));
        topology.add_node(Node::new(3, test_addr(6382), test_addr(16382)));

        assert_eq!(topology.leader_id, Some(1));
        assert_eq!(topology.nodes.len(), 3);
        assert!(topology.has_quorum());
    }

    #[test]
    fn test_replication_lag() {
        let mut node = Node::new(1, test_addr(6380), test_addr(16380));
        node.replication_offset = 1000;

        assert_eq!(node.replication_lag(1500), 500);
        assert_eq!(node.replication_lag(1000), 0);
        assert_eq!(node.replication_lag(500), 0); // Can't be negative
    }
}
