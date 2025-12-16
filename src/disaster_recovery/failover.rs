//! Failover Manager
//!
//! Automatic failover with quorum voting and split-brain prevention.

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Failover state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailoverState {
    /// Normal operation
    Normal,
    /// Failover in progress
    InProgress,
    /// Failover completed
    Completed,
    /// Manual intervention required
    ManualRequired,
    /// Split-brain detected
    SplitBrain,
}

/// Failover configuration
#[derive(Debug, Clone)]
pub struct FailoverConfig {
    /// Minimum nodes for quorum
    pub quorum_size: usize,
    /// Failover timeout
    pub timeout: Duration,
    /// Cooldown between failovers
    pub cooldown: Duration,
    /// Auto-failover enabled
    pub auto_failover: bool,
    /// Max failovers per hour
    pub max_failovers_per_hour: u32,
}

impl Default for FailoverConfig {
    fn default() -> Self {
        Self {
            quorum_size: 2,
            timeout: Duration::from_secs(30),
            cooldown: Duration::from_secs(300),
            auto_failover: true,
            max_failovers_per_hour: 3,
        }
    }
}

impl FailoverConfig {
    pub fn with_quorum(mut self, size: usize) -> Self {
        self.quorum_size = size;
        self
    }

    pub fn manual_only(mut self) -> Self {
        self.auto_failover = false;
        self
    }
}

/// Vote from a node
#[derive(Debug, Clone)]
pub struct FailoverVote {
    pub node_id: u64,
    pub candidate_id: u64,
    pub timestamp: Instant,
    pub approved: bool,
}

/// Failover manager
pub struct FailoverManager {
    config: FailoverConfig,
    state: FailoverState,
    current_primary: Option<u64>,
    votes: HashMap<u64, FailoverVote>,
    last_failover: Option<Instant>,
    failover_count: u32,
    failover_count_reset: Instant,
}

impl FailoverManager {
    pub fn new(config: FailoverConfig) -> Self {
        Self {
            config,
            state: FailoverState::Normal,
            current_primary: None,
            votes: HashMap::new(),
            last_failover: None,
            failover_count: 0,
            failover_count_reset: Instant::now(),
        }
    }

    /// Get current state
    pub fn state(&self) -> FailoverState {
        self.state
    }

    /// Set current primary
    pub fn set_primary(&mut self, node_id: u64) {
        self.current_primary = Some(node_id);
    }

    /// Get current primary
    pub fn primary(&self) -> Option<u64> {
        self.current_primary
    }

    /// Check if failover is allowed
    pub fn can_failover(&self) -> bool {
        // Check cooldown
        if let Some(last) = self.last_failover {
            if last.elapsed() < self.config.cooldown {
                return false;
            }
        }

        // Check rate limit
        if self.failover_count >= self.config.max_failovers_per_hour {
            return false;
        }

        self.config.auto_failover && self.state == FailoverState::Normal
    }

    /// Start failover process
    pub fn start_failover(&mut self, _candidate_id: u64) -> bool {
        if !self.can_failover() {
            return false;
        }

        self.state = FailoverState::InProgress;
        self.votes.clear();
        true
    }

    /// Register a vote
    pub fn vote(&mut self, vote: FailoverVote) {
        self.votes.insert(vote.node_id, vote);
    }

    /// Check if quorum reached
    pub fn has_quorum(&self, candidate_id: u64) -> bool {
        let approvals = self.votes.values()
            .filter(|v| v.candidate_id == candidate_id && v.approved)
            .count();
        approvals >= self.config.quorum_size
    }

    /// Complete failover
    pub fn complete_failover(&mut self, new_primary: u64) {
        self.current_primary = Some(new_primary);
        self.state = FailoverState::Completed;
        self.last_failover = Some(Instant::now());
        self.failover_count += 1;
        self.votes.clear();

        // Reset to normal after completion
        self.state = FailoverState::Normal;
    }

    /// Detect split-brain condition
    pub fn detect_split_brain(&mut self, primaries: &[u64]) -> bool {
        if primaries.len() > 1 {
            self.state = FailoverState::SplitBrain;
            true
        } else {
            false
        }
    }

    /// Reset failover count (called hourly)
    pub fn reset_rate_limit(&mut self) {
        if self.failover_count_reset.elapsed() >= Duration::from_secs(3600) {
            self.failover_count = 0;
            self.failover_count_reset = Instant::now();
        }
    }

    /// Get vote count
    pub fn vote_count(&self) -> usize {
        self.votes.len()
    }
}

impl Default for FailoverManager {
    fn default() -> Self {
        Self::new(FailoverConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_failover_manager() {
        let mut fm = FailoverManager::new(FailoverConfig::default().with_quorum(2));
        fm.set_primary(1);

        assert_eq!(fm.state(), FailoverState::Normal);
        assert!(fm.can_failover());
    }

    #[test]
    fn test_quorum_voting() {
        let mut fm = FailoverManager::new(FailoverConfig::default().with_quorum(2));
        fm.start_failover(2);

        fm.vote(FailoverVote {
            node_id: 1,
            candidate_id: 2,
            timestamp: Instant::now(),
            approved: true,
        });

        assert!(!fm.has_quorum(2)); // Need 2 votes

        fm.vote(FailoverVote {
            node_id: 3,
            candidate_id: 2,
            timestamp: Instant::now(),
            approved: true,
        });

        assert!(fm.has_quorum(2)); // Now have quorum
    }

    #[test]
    fn test_split_brain_detection() {
        let mut fm = FailoverManager::default();
        
        assert!(!fm.detect_split_brain(&[1]));
        assert!(fm.detect_split_brain(&[1, 2]));
        assert_eq!(fm.state(), FailoverState::SplitBrain);
    }
}
