//! Geo-Replication
//!
//! Multi-region and cross-datacenter replication.

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Geographic region
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GeoRegion {
    /// Region identifier
    pub id: String,
    /// Region name (e.g., "us-east-1")
    pub name: String,
    /// Datacenter location
    pub datacenter: String,
    /// Region priority (lower = higher priority)
    pub priority: u32,
    /// Is this the primary region?
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
    /// Replication mode
    pub mode: GeoReplicationMode,
    /// Max allowed lag before failover
    pub max_lag_ms: u64,
    /// Health check interval
    pub health_check_interval: Duration,
    /// Failover timeout
    pub failover_timeout: Duration,
}

/// Geo-replication modes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeoReplicationMode {
    /// Active-passive (one primary, rest standby)
    ActivePassive,
    /// Active-active (all regions accept writes)
    ActiveActive,
    /// Read replicas (primary writes, others read-only)
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
    pub connections: u32,
}

impl RegionStatus {
    pub fn new(region: GeoRegion) -> Self {
        Self {
            region,
            healthy: true,
            replication_lag_ms: 0,
            last_heartbeat: Instant::now(),
            connections: 0,
        }
    }

    pub fn update_heartbeat(&mut self) {
        self.last_heartbeat = Instant::now();
        self.healthy = true;
    }

    pub fn is_stale(&self, timeout: Duration) -> bool {
        self.last_heartbeat.elapsed() > timeout
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
        Self {
            config,
            regions: HashMap::new(),
            primary_id: None,
        }
    }

    /// Add a region
    pub fn add_region(&mut self, region: GeoRegion) {
        if region.is_primary {
            self.primary_id = Some(region.id.clone());
        }
        self.regions.insert(region.id.clone(), RegionStatus::new(region));
    }

    /// Get primary region
    pub fn primary(&self) -> Option<&RegionStatus> {
        self.primary_id.as_ref().and_then(|id| self.regions.get(id))
    }

    /// Get all healthy regions
    pub fn healthy_regions(&self) -> Vec<&RegionStatus> {
        self.regions.values().filter(|r| r.healthy).collect()
    }

    /// Update region health
    pub fn heartbeat(&mut self, region_id: &str, lag_ms: u64) {
        if let Some(status) = self.regions.get_mut(region_id) {
            status.update_heartbeat();
            status.replication_lag_ms = lag_ms;
            status.healthy = lag_ms <= self.config.max_lag_ms;
        }
    }

    /// Check for stale regions
    pub fn check_health(&mut self) {
        let timeout = self.config.health_check_interval * 3;
        for status in self.regions.values_mut() {
            if status.is_stale(timeout) {
                status.healthy = false;
            }
        }
    }

    /// Elect new primary (returns new primary ID if changed)
    pub fn elect_primary(&mut self) -> Option<String> {
        // Check if current primary is healthy
        if let Some(ref primary_id) = self.primary_id {
            if let Some(status) = self.regions.get(primary_id) {
                if status.healthy {
                    return None; // Current primary is fine
                }
            }
        }

        // Find best candidate (collect to avoid borrow issues)
        let mut candidates: Vec<_> = self.regions.values()
            .filter(|r| r.healthy)
            .map(|r| (r.region.id.clone(), r.region.priority))
            .collect();
        
        candidates.sort_by_key(|(_, priority)| *priority);

        if let Some((new_id, _)) = candidates.first() {
            self.primary_id = Some(new_id.clone());
            return Some(new_id.clone());
        }

        None
    }

    /// Get region count
    pub fn region_count(&self) -> usize {
        self.regions.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_geo_region() {
        let region = GeoRegion::new("us-1", "us-east-1", "dc-virginia")
            .primary();
        
        assert!(region.is_primary);
        assert_eq!(region.priority, 0);
    }

    #[test]
    fn test_geo_replication() {
        let mut geo = GeoReplication::new(GeoConfig::default());
        
        geo.add_region(GeoRegion::new("us-1", "us-east", "dc1").primary());
        geo.add_region(GeoRegion::new("eu-1", "eu-west", "dc2").with_priority(10));
        
        assert_eq!(geo.region_count(), 2);
        assert!(geo.primary().is_some());
    }

    #[test]
    fn test_health_check() {
        let mut geo = GeoReplication::new(GeoConfig::default());
        geo.add_region(GeoRegion::new("r1", "region1", "dc1").primary());
        
        geo.heartbeat("r1", 100);
        assert_eq!(geo.healthy_regions().len(), 1);
        
        geo.heartbeat("r1", 10000); // Over max lag
        assert_eq!(geo.healthy_regions().len(), 0);
    }
}
