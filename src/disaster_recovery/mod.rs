//! Disaster Recovery Module
//!
//! Multi-region replication, geo-redundancy, and split-brain prevention.

pub mod geo;
pub mod failover;
pub mod recovery;

pub use geo::{GeoRegion, GeoReplication, GeoConfig};
pub use failover::{FailoverManager, FailoverConfig, FailoverState};
pub use recovery::{PointInTimeRecovery, RecoveryPoint};
