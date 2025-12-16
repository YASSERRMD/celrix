//! Observability Module
//!
//! Prometheus metrics, health checks, admin API, and diagnostics.

mod admin;
mod health;
mod loadtest;
mod prometheus_metrics;

pub use admin::{AdminApi, AdminConfig, AdminRequest, AdminResponse};
pub use health::{HealthCheck, HealthStatus, SystemHealth};
pub use loadtest::{Benchmark, BenchmarkResult, LoadTestConfig, LoadTestStats};
pub use prometheus_metrics::{MetricsRegistry, PrometheusExporter};
