//! Failover Manager
//!
//! Automatic failover with quorum voting and split-brain prevention.

use std::collections::HashMap;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailoverState {
    Normal,
    InProgress,
    Completed,
    SplitBrain,
}

#[derive(Debug, Clone)]
pub struct FailoverConfig {
    pub quorum_size: usize,
    pub timeout: Duration,
    pub cooldown: Duration,
    pub auto_failover: bool,
}

impl Default for FailoverConfig {
    fn default() -> Self {
        Self {
            quorum_size: 2,
            timeout: Duration::from_secs(30),
            cooldown: Duration::from_secs(300),
            auto_failover: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FailoverVote {
    pub node_id: u64,
    pub candidate_id: u64,
    pub approved: bool,
}

pub struct FailoverManager {
    config: FailoverConfig,
    state: FailoverState,
    current_primary: Option<u64>,
    votes: HashMap<u64, FailoverVote>,
    last_failover: Option<Instant>,
}

impl FailoverManager {
    pub fn new(config: FailoverConfig) -> Self {
        Self {
            config,
            state: FailoverState::Normal,
            current_primary: None,
            votes: HashMap::new(),
            last_failover: None,
        }
    }

    pub fn state(&self) -> FailoverState { self.state }
    pub fn primary(&self) -> Option<u64> { self.current_primary }
    pub fn set_primary(&mut self, id: u64) { self.current_primary = Some(id); }

    pub fn can_failover(&self) -> bool {
        if let Some(last) = self.last_failover {
            if last.elapsed() < self.config.cooldown { return false; }
        }
        self.config.auto_failover && self.state == FailoverState::Normal
    }

    pub fn start_failover(&mut self) -> bool {
        if !self.can_failover() { return false; }
        self.state = FailoverState::InProgress;
        self.votes.clear();
        true
    }

    pub fn vote(&mut self, vote: FailoverVote) {
        self.votes.insert(vote.node_id, vote);
    }

    pub fn has_quorum(&self, candidate_id: u64) -> bool {
        self.votes.values()
            .filter(|v| v.candidate_id == candidate_id && v.approved)
            .count() >= self.config.quorum_size
    }

    pub fn complete_failover(&mut self, new_primary: u64) {
        self.current_primary = Some(new_primary);
        self.state = FailoverState::Normal;
        self.last_failover = Some(Instant::now());
        self.votes.clear();
    }

    pub fn detect_split_brain(&mut self, primaries: &[u64]) -> bool {
        if primaries.len() > 1 {
            self.state = FailoverState::SplitBrain;
            true
        } else { false }
    }
}

impl Default for FailoverManager {
    fn default() -> Self { Self::new(FailoverConfig::default()) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_failover() {
        let mut fm = FailoverManager::default();
        fm.set_primary(1);
        assert!(fm.can_failover());
        assert!(fm.start_failover());
        fm.vote(FailoverVote { node_id: 1, candidate_id: 2, approved: true });
        fm.vote(FailoverVote { node_id: 3, candidate_id: 2, approved: true });
        assert!(fm.has_quorum(2));
    }

    #[test]
    fn test_split_brain() {
        let mut fm = FailoverManager::default();
        assert!(fm.detect_split_brain(&[1, 2]));
        assert_eq!(fm.state(), FailoverState::SplitBrain);
    }
}
