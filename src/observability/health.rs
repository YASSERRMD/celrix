//! Health Checks
//!
//! Server health status and diagnostics.

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Health status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

impl std::fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HealthStatus::Healthy => write!(f, "healthy"),
            HealthStatus::Degraded => write!(f, "degraded"),
            HealthStatus::Unhealthy => write!(f, "unhealthy"),
        }
    }
}

/// Individual health check result
#[derive(Debug, Clone)]
pub struct CheckResult {
    pub name: String,
    pub status: HealthStatus,
    pub message: Option<String>,
    pub duration: Duration,
}

/// System health summary
#[derive(Debug, Clone)]
pub struct SystemHealth {
    pub overall: HealthStatus,
    pub checks: Vec<CheckResult>,
    pub uptime: Duration,
    pub version: String,
}

impl SystemHealth {
    /// Serialize to JSON format
    pub fn to_json(&self) -> String {
        let checks_json: Vec<String> = self
            .checks
            .iter()
            .map(|c| {
                format!(
                    r#"{{"name":"{}","status":"{}","duration_ms":{}{}}}"#,
                    c.name,
                    c.status,
                    c.duration.as_millis(),
                    c.message
                        .as_ref()
                        .map(|m| format!(r#","message":"{}""#, m))
                        .unwrap_or_default()
                )
            })
            .collect();

        format!(
            r#"{{"status":"{}","uptime_secs":{},"version":"{}","checks":[{}]}}"#,
            self.overall,
            self.uptime.as_secs(),
            self.version,
            checks_json.join(",")
        )
    }
}

/// Health check function type
pub type CheckFn = Box<dyn Fn() -> (HealthStatus, Option<String>) + Send + Sync>;

/// Health check manager
pub struct HealthCheck {
    checks: HashMap<String, CheckFn>,
    start_time: Instant,
}

impl Default for HealthCheck {
    fn default() -> Self {
        Self::new()
    }
}

impl HealthCheck {
    pub fn new() -> Self {
        Self {
            checks: HashMap::new(),
            start_time: Instant::now(),
        }
    }

    /// Register a health check
    pub fn register<F>(&mut self, name: &str, check: F)
    where
        F: Fn() -> (HealthStatus, Option<String>) + Send + Sync + 'static,
    {
        self.checks.insert(name.to_string(), Box::new(check));
    }

    /// Run all health checks
    pub fn check(&self) -> SystemHealth {
        let mut results = Vec::new();
        let mut overall = HealthStatus::Healthy;

        for (name, check_fn) in &self.checks {
            let start = Instant::now();
            let (status, message) = check_fn();
            let duration = start.elapsed();

            // Update overall status
            match status {
                HealthStatus::Unhealthy => overall = HealthStatus::Unhealthy,
                HealthStatus::Degraded if overall == HealthStatus::Healthy => {
                    overall = HealthStatus::Degraded;
                }
                _ => {}
            }

            results.push(CheckResult {
                name: name.clone(),
                status,
                message,
                duration,
            });
        }

        SystemHealth {
            overall,
            checks: results,
            uptime: self.start_time.elapsed(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    /// Simple liveness check
    pub fn liveness(&self) -> bool {
        true // If we can execute this, we're alive
    }

    /// Readiness check (all checks healthy)
    pub fn readiness(&self) -> bool {
        self.check().overall == HealthStatus::Healthy
    }

    /// Get uptime
    pub fn uptime(&self) -> Duration {
        self.start_time.elapsed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_check() {
        let mut health = HealthCheck::new();

        health.register("test_ok", || (HealthStatus::Healthy, None));
        health.register("test_degraded", || {
            (HealthStatus::Degraded, Some("High latency".to_string()))
        });

        let result = health.check();
        assert_eq!(result.overall, HealthStatus::Degraded);
        assert_eq!(result.checks.len(), 2);
    }

    #[test]
    fn test_health_json() {
        let mut health = HealthCheck::new();
        health.register("store", || (HealthStatus::Healthy, None));

        let result = health.check();
        let json = result.to_json();

        assert!(json.contains("\"status\":\"healthy\""));
        assert!(json.contains("\"version\":"));
    }

    #[test]
    fn test_liveness_readiness() {
        let health = HealthCheck::new();
        assert!(health.liveness());
        assert!(health.readiness());
    }
}
