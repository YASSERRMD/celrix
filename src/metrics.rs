//! Basic Metrics
//!
//! Operations counters and latency tracking.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::Duration;

/// Metrics collector
#[derive(Debug)]
pub struct Metrics {
    /// Total operations count
    total_ops: AtomicU64,

    /// Operations per command type
    ops_by_command: RwLock<HashMap<String, u64>>,

    /// Latency tracking (simplified)
    latency_sum_us: AtomicU64,
    latency_count: AtomicU64,
    latency_min_us: AtomicU64,
    latency_max_us: AtomicU64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Create new metrics collector
    pub fn new() -> Self {
        Self {
            total_ops: AtomicU64::new(0),
            ops_by_command: RwLock::new(HashMap::new()),
            latency_sum_us: AtomicU64::new(0),
            latency_count: AtomicU64::new(0),
            latency_min_us: AtomicU64::new(u64::MAX),
            latency_max_us: AtomicU64::new(0),
        }
    }

    /// Record an operation
    pub fn record_operation(&self, command: &str, latency: Duration) {
        // Increment total ops
        self.total_ops.fetch_add(1, Ordering::Relaxed);

        // Increment per-command counter
        {
            let mut ops = self.ops_by_command.write().unwrap();
            *ops.entry(command.to_string()).or_insert(0) += 1;
        }

        // Record latency
        let latency_us = latency.as_micros() as u64;
        self.latency_sum_us.fetch_add(latency_us, Ordering::Relaxed);
        self.latency_count.fetch_add(1, Ordering::Relaxed);

        // Update min (atomic min)
        let mut current_min = self.latency_min_us.load(Ordering::Relaxed);
        while latency_us < current_min {
            match self.latency_min_us.compare_exchange_weak(
                current_min,
                latency_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(c) => current_min = c,
            }
        }

        // Update max (atomic max)
        let mut current_max = self.latency_max_us.load(Ordering::Relaxed);
        while latency_us > current_max {
            match self.latency_max_us.compare_exchange_weak(
                current_max,
                latency_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(c) => current_max = c,
            }
        }
    }

    /// Get total operations count
    pub fn total_ops(&self) -> u64 {
        self.total_ops.load(Ordering::Relaxed)
    }

    /// Get operations by command
    pub fn ops_by_command(&self) -> HashMap<String, u64> {
        self.ops_by_command.read().unwrap().clone()
    }

    /// Get average latency in microseconds
    pub fn avg_latency_us(&self) -> f64 {
        let count = self.latency_count.load(Ordering::Relaxed);
        if count == 0 {
            return 0.0;
        }
        let sum = self.latency_sum_us.load(Ordering::Relaxed);
        sum as f64 / count as f64
    }

    /// Get min latency in microseconds
    pub fn min_latency_us(&self) -> u64 {
        let min = self.latency_min_us.load(Ordering::Relaxed);
        if min == u64::MAX {
            0
        } else {
            min
        }
    }

    /// Get max latency in microseconds
    pub fn max_latency_us(&self) -> u64 {
        self.latency_max_us.load(Ordering::Relaxed)
    }

    /// Get a summary of metrics
    pub fn summary(&self) -> String {
        format!(
            "Operations: {} | Latency (µs): avg={:.1}, min={}, max={}",
            self.total_ops(),
            self.avg_latency_us(),
            self.min_latency_us(),
            self.max_latency_us()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics() {
        let metrics = Metrics::new();

        metrics.record_operation("GET", Duration::from_micros(100));
        metrics.record_operation("GET", Duration::from_micros(200));
        metrics.record_operation("SET", Duration::from_micros(150));

        assert_eq!(metrics.total_ops(), 3);
        assert_eq!(metrics.min_latency_us(), 100);
        assert_eq!(metrics.max_latency_us(), 200);
        assert!((metrics.avg_latency_us() - 150.0).abs() < 0.1);

        let by_cmd = metrics.ops_by_command();
        assert_eq!(by_cmd.get("GET"), Some(&2));
        assert_eq!(by_cmd.get("SET"), Some(&1));
    }
}
