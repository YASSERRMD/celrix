//! Point-in-Time Recovery
//!
//! Recovery to specific points in time using snapshots and AOF.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Recovery point
#[derive(Debug, Clone)]
pub struct RecoveryPoint {
    /// Unique identifier
    pub id: String,
    /// Timestamp (unix millis)
    pub timestamp_ms: u64,
    /// Snapshot file path (if any)
    pub snapshot_path: Option<PathBuf>,
    /// AOF offset at this point
    pub aof_offset: u64,
    /// Description
    pub description: String,
    /// Size in bytes
    pub size_bytes: u64,
}

impl RecoveryPoint {
    pub fn new(id: &str, timestamp_ms: u64) -> Self {
        Self {
            id: id.to_string(),
            timestamp_ms,
            snapshot_path: None,
            aof_offset: 0,
            description: String::new(),
            size_bytes: 0,
        }
    }

    pub fn with_snapshot(mut self, path: PathBuf) -> Self {
        self.snapshot_path = Some(path);
        self
    }

    pub fn with_aof_offset(mut self, offset: u64) -> Self {
        self.aof_offset = offset;
        self
    }

    pub fn with_description(mut self, desc: &str) -> Self {
        self.description = desc.to_string();
        self
    }

    /// Format as human-readable time
    pub fn formatted_time(&self) -> String {
        let secs = self.timestamp_ms / 1000;
        format!("{}", secs)
    }
}

/// Point-in-time recovery configuration
#[derive(Debug, Clone)]
pub struct PitrConfig {
    /// Retention period for recovery points
    pub retention: Duration,
    /// Minimum interval between recovery points
    pub min_interval: Duration,
    /// Maximum number of recovery points
    pub max_points: usize,
    /// Data directory
    pub data_dir: PathBuf,
}

impl Default for PitrConfig {
    fn default() -> Self {
        Self {
            retention: Duration::from_secs(7 * 24 * 3600), // 7 days
            min_interval: Duration::from_secs(3600),       // 1 hour
            max_points: 168,                               // 7 days * 24 hours
            data_dir: PathBuf::from("./data/pitr"),
        }
    }
}

/// Point-in-time recovery manager
pub struct PointInTimeRecovery {
    config: PitrConfig,
    /// Recovery points sorted by timestamp
    points: BTreeMap<u64, RecoveryPoint>,
}

impl PointInTimeRecovery {
    pub fn new(config: PitrConfig) -> Self {
        Self {
            config,
            points: BTreeMap::new(),
        }
    }

    /// Create a new recovery point
    pub fn create_point(&mut self, description: &str) -> RecoveryPoint {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        let id = format!("rp_{}", timestamp);
        let point = RecoveryPoint::new(&id, timestamp)
            .with_description(description);
        
        self.points.insert(timestamp, point.clone());
        self.cleanup_old_points();
        
        point
    }

    /// Add an existing recovery point
    pub fn add_point(&mut self, point: RecoveryPoint) {
        self.points.insert(point.timestamp_ms, point);
        self.cleanup_old_points();
    }

    /// Get recovery point closest to timestamp
    pub fn get_point_at(&self, timestamp_ms: u64) -> Option<&RecoveryPoint> {
        // Find the latest point before or at the timestamp
        self.points
            .range(..=timestamp_ms)
            .next_back()
            .map(|(_, p)| p)
    }

    /// Get the latest recovery point
    pub fn latest(&self) -> Option<&RecoveryPoint> {
        self.points.values().next_back()
    }

    /// Get all recovery points
    pub fn list(&self) -> Vec<&RecoveryPoint> {
        self.points.values().collect()
    }

    /// Get recovery points in time range
    pub fn range(&self, start_ms: u64, end_ms: u64) -> Vec<&RecoveryPoint> {
        self.points
            .range(start_ms..=end_ms)
            .map(|(_, p)| p)
            .collect()
    }

    /// Delete a recovery point
    pub fn delete(&mut self, timestamp_ms: u64) -> Option<RecoveryPoint> {
        self.points.remove(&timestamp_ms)
    }

    /// Cleanup old recovery points
    fn cleanup_old_points(&mut self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        let cutoff = now - self.config.retention.as_millis() as u64;
        
        // Remove points older than retention
        self.points.retain(|ts, _| *ts >= cutoff);
        
        // Remove excess points (keep newest)
        while self.points.len() > self.config.max_points {
            if let Some((&oldest, _)) = self.points.iter().next() {
                self.points.remove(&oldest);
            }
        }
    }

    /// Get total size of recovery points
    pub fn total_size(&self) -> u64 {
        self.points.values().map(|p| p.size_bytes).sum()
    }

    /// Get point count
    pub fn count(&self) -> usize {
        self.points.len()
    }
}

impl Default for PointInTimeRecovery {
    fn default() -> Self {
        Self::new(PitrConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    #[test]
    fn test_create_recovery_point() {
        let mut pitr = PointInTimeRecovery::default();
        let point = pitr.create_point("Test backup");

        assert!(!point.id.is_empty());
        assert!(point.timestamp_ms > 0);
        assert_eq!(pitr.count(), 1);
    }

    #[test]
    fn test_get_point_at() {
        let mut pitr = PointInTimeRecovery::default();
        let base = now_ms();
        
        let p1 = RecoveryPoint::new("p1", base);
        let p2 = RecoveryPoint::new("p2", base + 1000);
        let p3 = RecoveryPoint::new("p3", base + 2000);
        
        pitr.add_point(p1);
        pitr.add_point(p2);
        pitr.add_point(p3);

        // Get point at base+1500 should return p2
        let point = pitr.get_point_at(base + 1500).unwrap();
        assert_eq!(point.id, "p2");
    }

    #[test]
    fn test_list_and_range() {
        let mut pitr = PointInTimeRecovery::default();
        let base = now_ms();
        
        pitr.add_point(RecoveryPoint::new("p1", base));
        pitr.add_point(RecoveryPoint::new("p2", base + 1000));
        pitr.add_point(RecoveryPoint::new("p3", base + 2000));

        assert_eq!(pitr.list().len(), 3);
        assert_eq!(pitr.range(base + 500, base + 1500).len(), 1);
    }
}
