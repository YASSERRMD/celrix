//! Geo-Replication
//!
//! Multi-region and cross-datacenter replication.

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Geographic region
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GeoRegion {
    pub id: String,
    pub name: String,
    pub datacenter: String,
    pub priority: u32,
    pub is_primary: bool,
}

impl GeoRegion {
    pub fn new(id: &str, name: &str, datacenter: &str) -> Self {
        Self {
            id: id.to_string(),
            name: name.to_string(),
            datacenter: datacenter.to_string(),
            priority: 100,
            is_primary: false,
        }
    }

    pub fn primary(mut self) -> Self {
        self.is_primary = true;
        self.priority = 0;
        self
    }

    pub fn with_priority(mut self, priority: u32) -> Self {
        self.priority = priority;
        self
    }
}

/// Geo-replication configuration
#[derive(Debug, Clone)]
pub struct GeoConfig {
    pub mode: GeoReplicationMode,
    pub max_lag_ms: u64,
    pub health_check_interval: Duration,
    pub failover_timeout: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeoReplicationMode {
    ActivePassive,
    ActiveActive,
    ReadReplicas,
}

impl Default for GeoConfig {
    fn default() -> Self {
        Self {
            mode: GeoReplicationMode::ActivePassive,
            max_lag_ms: 5000,
            health_check_interval: Duration::from_secs(1),
            failover_timeout: Duration::from_secs(30),
        }
    }
}

/// Region status
#[derive(Debug, Clone)]
pub struct RegionStatus {
    pub region: GeoRegion,
    pub healthy: bool,
    pub replication_lag_ms: u64,
    pub last_heartbeat: Instant,
}

impl RegionStatus {
    pub fn new(region: GeoRegion) -> Self {
        Self {
            region,
            healthy: true,
            replication_lag_ms: 0,
            last_heartbeat: Instant::now(),
        }
    }
}

/// Geo-replication manager
pub struct GeoReplication {
    config: GeoConfig,
    regions: HashMap<String, RegionStatus>,
    primary_id: Option<String>,
}

impl GeoReplication {
    pub fn new(config: GeoConfig) -> Self {
        Self { config, regions: HashMap::new(), primary_id: None }
    }

    pub fn add_region(&mut self, region: GeoRegion) {
        if region.is_primary {
            self.primary_id = Some(region.id.clone());
        }
        self.regions.insert(region.id.clone(), RegionStatus::new(region));
    }

    pub fn primary(&self) -> Option<&RegionStatus> {
        self.primary_id.as_ref().and_then(|id| self.regions.get(id))
    }

    pub fn healthy_regions(&self) -> Vec<&RegionStatus> {
        self.regions.values().filter(|r| r.healthy).collect()
    }

    pub fn heartbeat(&mut self, region_id: &str, lag_ms: u64) {
        if let Some(status) = self.regions.get_mut(region_id) {
            status.last_heartbeat = Instant::now();
            status.replication_lag_ms = lag_ms;
            status.healthy = lag_ms <= self.config.max_lag_ms;
        }
    }

    pub fn region_count(&self) -> usize {
        self.regions.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_geo_replication() {
        let mut geo = GeoReplication::new(GeoConfig::default());
        geo.add_region(GeoRegion::new("us-1", "us-east", "dc1").primary());
        geo.add_region(GeoRegion::new("eu-1", "eu-west", "dc2"));
        assert_eq!(geo.region_count(), 2);
        assert!(geo.primary().is_some());
    }
}
