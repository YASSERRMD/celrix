//! Cluster Module
//!
//! Distributed cluster support with replication and consensus.

pub mod node;
pub mod raft;
pub mod replication;
pub mod sharding;

pub use node::{Node, NodeId, NodeRole, NodeState};
pub use raft::{RaftNode, RaftConfig, RaftState};
pub use replication::{ReplicationManager, ReplicationConfig, ReplicationMode};
pub use sharding::{ShardManager, Slot, SlotRange};
