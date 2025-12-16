//! Point-in-Time Recovery

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct RecoveryPoint {
    pub id: String,
    pub timestamp_ms: u64,
    pub snapshot_path: Option<PathBuf>,
    pub aof_offset: u64,
    pub description: String,
}

impl RecoveryPoint {
    pub fn new(id: &str, timestamp_ms: u64) -> Self {
        Self {
            id: id.to_string(),
            timestamp_ms,
            snapshot_path: None,
            aof_offset: 0,
            description: String::new(),
        }
    }

    pub fn with_description(mut self, desc: &str) -> Self {
        self.description = desc.to_string();
        self
    }
}

#[derive(Debug, Clone)]
pub struct PitrConfig {
    pub retention: Duration,
    pub max_points: usize,
}

impl Default for PitrConfig {
    fn default() -> Self {
        Self {
            retention: Duration::from_secs(7 * 24 * 3600),
            max_points: 168,
        }
    }
}

pub struct PointInTimeRecovery {
    config: PitrConfig,
    points: BTreeMap<u64, RecoveryPoint>,
}

impl PointInTimeRecovery {
    pub fn new(config: PitrConfig) -> Self {
        Self { config, points: BTreeMap::new() }
    }

    fn now_ms() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
    }

    pub fn create_point(&mut self, description: &str) -> RecoveryPoint {
        let ts = Self::now_ms();
        let point = RecoveryPoint::new(&format!("rp_{}", ts), ts).with_description(description);
        self.points.insert(ts, point.clone());
        self.cleanup();
        point
    }

    pub fn add_point(&mut self, point: RecoveryPoint) {
        self.points.insert(point.timestamp_ms, point);
        self.cleanup();
    }

    pub fn get_point_at(&self, ts: u64) -> Option<&RecoveryPoint> {
        self.points.range(..=ts).next_back().map(|(_, p)| p)
    }

    pub fn latest(&self) -> Option<&RecoveryPoint> {
        self.points.values().next_back()
    }

    pub fn list(&self) -> Vec<&RecoveryPoint> {
        self.points.values().collect()
    }

    pub fn count(&self) -> usize { self.points.len() }

    fn cleanup(&mut self) {
        let cutoff = Self::now_ms() - self.config.retention.as_millis() as u64;
        self.points.retain(|ts, _| *ts >= cutoff);
        while self.points.len() > self.config.max_points {
            if let Some((&oldest, _)) = self.points.iter().next() {
                self.points.remove(&oldest);
            }
        }
    }
}

impl Default for PointInTimeRecovery {
    fn default() -> Self { Self::new(PitrConfig::default()) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pitr() {
        let mut pitr = PointInTimeRecovery::default();
        let p = pitr.create_point("backup");
        assert!(!p.id.is_empty());
        assert_eq!(pitr.count(), 1);
    }
}
