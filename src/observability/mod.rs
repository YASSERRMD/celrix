//! Observability Module
//!
//! Prometheus metrics, health checks, and diagnostics.

mod prometheus_metrics;
mod health;

pub use prometheus_metrics::{PrometheusExporter, MetricsRegistry};
pub use health::{HealthCheck, HealthStatus, SystemHealth};
