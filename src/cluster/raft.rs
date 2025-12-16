//! Raft Consensus
//!
//! Leader election and log replication using Raft protocol.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use super::node::{NodeId, NodeRole};

/// Raft configuration
#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// Election timeout range (min, max) in ms
    pub election_timeout: (u64, u64),
    /// Heartbeat interval in ms
    pub heartbeat_interval: u64,
    /// Log entries per batch
    pub batch_size: usize,
    /// Pre-vote enabled (prevents disruptions)
    pub pre_vote: bool,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout: (150, 300),
            heartbeat_interval: 50,
            batch_size: 100,
            pre_vote: true,
        }
    }
}

/// Raft state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftState {
    Follower,
    Candidate,
    Leader,
    PreCandidate,
}

impl Default for RaftState {
    fn default() -> Self {
        Self::Follower
    }
}

/// Log entry
#[derive(Debug, Clone)]
pub struct LogEntry {
    /// Term when entry was created
    pub term: u64,
    /// Index in the log
    pub index: u64,
    /// Entry type
    pub entry_type: LogEntryType,
    /// Serialized command data
    pub data: Vec<u8>,
}

/// Log entry types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogEntryType {
    /// Normal command
    Command,
    /// Configuration change
    ConfigChange,
    /// No-op (for leader election)
    NoOp,
}

/// Vote request
#[derive(Debug, Clone)]
pub struct VoteRequest {
    pub term: u64,
    pub candidate_id: NodeId,
    pub last_log_index: u64,
    pub last_log_term: u64,
    pub pre_vote: bool,
}

/// Vote response
#[derive(Debug, Clone)]
pub struct VoteResponse {
    pub term: u64,
    pub vote_granted: bool,
    pub pre_vote: bool,
}

/// Append entries request
#[derive(Debug, Clone)]
pub struct AppendEntriesRequest {
    pub term: u64,
    pub leader_id: NodeId,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

/// Append entries response
#[derive(Debug, Clone)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    pub match_index: u64,
}

/// Raft node state machine
pub struct RaftNode {
    /// Node ID
    pub id: NodeId,
    /// Current term
    pub current_term: AtomicU64,
    /// Current state
    pub state: RwLock<RaftState>,
    /// Voted for in current term
    pub voted_for: RwLock<Option<NodeId>>,
    /// Log entries
    pub log: RwLock<Vec<LogEntry>>,
    /// Commit index
    pub commit_index: AtomicU64,
    /// Last applied
    pub last_applied: AtomicU64,
    /// Leader ID (if known)
    pub leader_id: RwLock<Option<NodeId>>,
    /// Next index for each follower (leader only)
    pub next_index: RwLock<HashMap<NodeId, u64>>,
    /// Match index for each follower (leader only)
    pub match_index: RwLock<HashMap<NodeId, u64>>,
    /// Configuration
    pub config: RaftConfig,
    /// Last heartbeat time
    pub last_heartbeat: RwLock<Instant>,
    /// Election deadline
    pub election_deadline: RwLock<Instant>,
}

impl RaftNode {
    pub fn new(id: NodeId, config: RaftConfig) -> Self {
        let now = Instant::now();
        Self {
            id,
            current_term: AtomicU64::new(0),
            state: RwLock::new(RaftState::Follower),
            voted_for: RwLock::new(None),
            log: RwLock::new(Vec::new()),
            commit_index: AtomicU64::new(0),
            last_applied: AtomicU64::new(0),
            leader_id: RwLock::new(None),
            next_index: RwLock::new(HashMap::new()),
            match_index: RwLock::new(HashMap::new()),
            config,
            last_heartbeat: RwLock::new(now),
            election_deadline: RwLock::new(now),
        }
    }

    /// Get current term
    pub fn term(&self) -> u64 {
        self.current_term.load(Ordering::SeqCst)
    }

    /// Get current state
    pub fn get_state(&self) -> RaftState {
        *self.state.read().unwrap()
    }

    /// Check if this node is leader
    pub fn is_leader(&self) -> bool {
        self.get_state() == RaftState::Leader
    }

    /// Get last log index
    pub fn last_log_index(&self) -> u64 {
        self.log.read().unwrap().last().map(|e| e.index).unwrap_or(0)
    }

    /// Get last log term
    pub fn last_log_term(&self) -> u64 {
        self.log.read().unwrap().last().map(|e| e.term).unwrap_or(0)
    }

    /// Transition to candidate state
    pub fn become_candidate(&self) {
        let mut state = self.state.write().unwrap();
        *state = RaftState::Candidate;
        self.current_term.fetch_add(1, Ordering::SeqCst);
        *self.voted_for.write().unwrap() = Some(self.id);
    }

    /// Transition to leader state
    pub fn become_leader(&self) {
        let mut state = self.state.write().unwrap();
        *state = RaftState::Leader;
        *self.leader_id.write().unwrap() = Some(self.id);
    }

    /// Transition to follower state
    pub fn become_follower(&self, term: u64, leader: Option<NodeId>) {
        self.current_term.store(term, Ordering::SeqCst);
        let mut state = self.state.write().unwrap();
        *state = RaftState::Follower;
        *self.voted_for.write().unwrap() = None;
        *self.leader_id.write().unwrap() = leader;
    }

    /// Handle vote request
    pub fn handle_vote_request(&self, req: &VoteRequest) -> VoteResponse {
        let current_term = self.term();

        // If request term is older, reject
        if req.term < current_term {
            return VoteResponse {
                term: current_term,
                vote_granted: false,
                pre_vote: req.pre_vote,
            };
        }

        // If request term is newer, become follower
        if req.term > current_term && !req.pre_vote {
            self.become_follower(req.term, None);
        }

        let voted_for = self.voted_for.read().unwrap();
        let can_vote = voted_for.is_none() || *voted_for == Some(req.candidate_id);

        // Check log is up-to-date
        let last_index = self.last_log_index();
        let last_term = self.last_log_term();
        let log_ok = req.last_log_term > last_term
            || (req.last_log_term == last_term && req.last_log_index >= last_index);

        let vote_granted = can_vote && log_ok;

        if vote_granted && !req.pre_vote {
            *self.voted_for.write().unwrap() = Some(req.candidate_id);
        }

        VoteResponse {
            term: self.term(),
            vote_granted,
            pre_vote: req.pre_vote,
        }
    }

    /// Handle append entries request
    pub fn handle_append_entries(&self, req: &AppendEntriesRequest) -> AppendEntriesResponse {
        let current_term = self.term();

        // Reject if term is older
        if req.term < current_term {
            return AppendEntriesResponse {
                term: current_term,
                success: false,
                match_index: 0,
            };
        }

        // Update term and become follower
        if req.term > current_term {
            self.become_follower(req.term, Some(req.leader_id));
        }

        // Update leader and heartbeat
        *self.leader_id.write().unwrap() = Some(req.leader_id);
        *self.last_heartbeat.write().unwrap() = Instant::now();

        // Check log consistency
        let log = self.log.read().unwrap();
        if req.prev_log_index > 0 {
            if let Some(entry) = log.get(req.prev_log_index as usize - 1) {
                if entry.term != req.prev_log_term {
                    return AppendEntriesResponse {
                        term: self.term(),
                        success: false,
                        match_index: 0,
                    };
                }
            } else if req.prev_log_index > log.len() as u64 {
                return AppendEntriesResponse {
                    term: self.term(),
                    success: false,
                    match_index: log.len() as u64,
                };
            }
        }
        drop(log);

        // Append new entries
        if !req.entries.is_empty() {
            let mut log = self.log.write().unwrap();
            for entry in &req.entries {
                if (entry.index as usize) <= log.len() {
                    // Overwrite conflicting entry
                    log[entry.index as usize - 1] = entry.clone();
                } else {
                    log.push(entry.clone());
                }
            }
        }

        // Update commit index
        if req.leader_commit > self.commit_index.load(Ordering::SeqCst) {
            let new_commit = std::cmp::min(req.leader_commit, self.last_log_index());
            self.commit_index.store(new_commit, Ordering::SeqCst);
        }

        AppendEntriesResponse {
            term: self.term(),
            success: true,
            match_index: self.last_log_index(),
        }
    }

    /// Append a command to the log (leader only)
    pub fn append_command(&self, data: Vec<u8>) -> Option<u64> {
        if !self.is_leader() {
            return None;
        }

        let mut log = self.log.write().unwrap();
        let index = log.len() as u64 + 1;
        let term = self.term();

        log.push(LogEntry {
            term,
            index,
            entry_type: LogEntryType::Command,
            data,
        });

        Some(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raft_node_creation() {
        let node = RaftNode::new(1, RaftConfig::default());
        assert_eq!(node.id, 1);
        assert_eq!(node.term(), 0);
        assert_eq!(node.get_state(), RaftState::Follower);
    }

    #[test]
    fn test_become_candidate() {
        let node = RaftNode::new(1, RaftConfig::default());
        node.become_candidate();

        assert_eq!(node.get_state(), RaftState::Candidate);
        assert_eq!(node.term(), 1);
        assert_eq!(*node.voted_for.read().unwrap(), Some(1));
    }

    #[test]
    fn test_become_leader() {
        let node = RaftNode::new(1, RaftConfig::default());
        node.become_candidate();
        node.become_leader();

        assert!(node.is_leader());
        assert_eq!(*node.leader_id.read().unwrap(), Some(1));
    }

    #[test]
    fn test_vote_request() {
        let node = RaftNode::new(1, RaftConfig::default());

        let req = VoteRequest {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
            pre_vote: false,
        };

        let resp = node.handle_vote_request(&req);
        assert!(resp.vote_granted);
    }
}
