//! Prometheus Metrics Export
//!
//! Export metrics in Prometheus text format.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

/// Metric type
#[derive(Debug, Clone, Copy)]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
}

/// A single metric
#[derive(Debug)]
pub struct Metric {
    pub name: String,
    pub help: String,
    pub metric_type: MetricType,
    pub value: AtomicU64,
    pub labels: Vec<(String, String)>,
}

impl Metric {
    pub fn counter(name: &str, help: &str) -> Self {
        Self {
            name: name.to_string(),
            help: help.to_string(),
            metric_type: MetricType::Counter,
            value: AtomicU64::new(0),
            labels: Vec::new(),
        }
    }

    pub fn gauge(name: &str, help: &str) -> Self {
        Self {
            name: name.to_string(),
            help: help.to_string(),
            metric_type: MetricType::Gauge,
            value: AtomicU64::new(0),
            labels: Vec::new(),
        }
    }

    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(&self, v: u64) {
        self.value.fetch_add(v, Ordering::Relaxed);
    }

    pub fn set(&self, v: u64) {
        self.value.store(v, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

/// Metrics registry
pub struct MetricsRegistry {
    metrics: RwLock<HashMap<String, Metric>>,
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsRegistry {
    pub fn new() -> Self {
        let registry = Self {
            metrics: RwLock::new(HashMap::new()),
        };

        // Register default CELRIX metrics
        registry.register(Metric::counter(
            "celrix_commands_total",
            "Total commands processed",
        ));
        registry.register(Metric::counter(
            "celrix_commands_get_total",
            "Total GET commands",
        ));
        registry.register(Metric::counter(
            "celrix_commands_set_total",
            "Total SET commands",
        ));
        registry.register(Metric::counter(
            "celrix_commands_del_total",
            "Total DEL commands",
        ));
        registry.register(Metric::gauge(
            "celrix_keys_total",
            "Current number of keys",
        ));
        registry.register(Metric::gauge(
            "celrix_memory_bytes",
            "Estimated memory usage",
        ));
        registry.register(Metric::gauge(
            "celrix_connections_active",
            "Active client connections",
        ));
        registry.register(Metric::counter(
            "celrix_connections_total",
            "Total connections accepted",
        ));
        registry.register(Metric::gauge(
            "celrix_uptime_seconds",
            "Server uptime in seconds",
        ));

        registry
    }

    /// Register a metric
    pub fn register(&self, metric: Metric) {
        let mut metrics = self.metrics.write().unwrap();
        metrics.insert(metric.name.clone(), metric);
    }

    /// Get a metric by name
    pub fn get(&self, name: &str) -> Option<u64> {
        let metrics = self.metrics.read().unwrap();
        metrics.get(name).map(|m| m.get())
    }

    /// Increment a counter
    pub fn inc(&self, name: &str) {
        if let Some(metric) = self.metrics.read().unwrap().get(name) {
            metric.inc();
        }
    }

    /// Set a gauge value
    pub fn set(&self, name: &str, value: u64) {
        if let Some(metric) = self.metrics.read().unwrap().get(name) {
            metric.set(value);
        }
    }

    /// Export all metrics in Prometheus format
    pub fn export(&self) -> String {
        let metrics = self.metrics.read().unwrap();
        let mut output = String::new();

        for metric in metrics.values() {
            // Type line
            let type_str = match metric.metric_type {
                MetricType::Counter => "counter",
                MetricType::Gauge => "gauge",
                MetricType::Histogram => "histogram",
            };
            output.push_str(&format!("# HELP {} {}\n", metric.name, metric.help));
            output.push_str(&format!("# TYPE {} {}\n", metric.name, type_str));

            // Value line
            if metric.labels.is_empty() {
                output.push_str(&format!("{} {}\n", metric.name, metric.get()));
            } else {
                let labels: Vec<_> = metric
                    .labels
                    .iter()
                    .map(|(k, v)| format!("{}=\"{}\"", k, v))
                    .collect();
                output.push_str(&format!(
                    "{}{{{}}} {}\n",
                    metric.name,
                    labels.join(","),
                    metric.get()
                ));
            }
        }

        output
    }
}

/// Prometheus metrics exporter
pub struct PrometheusExporter {
    registry: MetricsRegistry,
}

impl Default for PrometheusExporter {
    fn default() -> Self {
        Self::new()
    }
}

impl PrometheusExporter {
    pub fn new() -> Self {
        Self {
            registry: MetricsRegistry::new(),
        }
    }

    pub fn registry(&self) -> &MetricsRegistry {
        &self.registry
    }

    /// Export metrics in Prometheus text format
    pub fn export(&self) -> String {
        self.registry.export()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_registry() {
        let registry = MetricsRegistry::new();

        registry.inc("celrix_commands_total");
        registry.inc("celrix_commands_total");
        registry.set("celrix_keys_total", 42);

        assert_eq!(registry.get("celrix_commands_total"), Some(2));
        assert_eq!(registry.get("celrix_keys_total"), Some(42));
    }

    #[test]
    fn test_prometheus_export() {
        let exporter = PrometheusExporter::new();
        exporter.registry().inc("celrix_commands_total");

        let output = exporter.export();
        assert!(output.contains("celrix_commands_total"));
        assert!(output.contains("# TYPE"));
        assert!(output.contains("counter"));
    }
}
