//! Load Testing Framework
//!
//! Benchmarking and stress testing utilities.

use std::time::{Duration, Instant};

/// Load test configuration
#[derive(Debug, Clone)]
pub struct LoadTestConfig {
    /// Number of concurrent clients
    pub concurrency: usize,
    /// Total number of requests
    pub total_requests: u64,
    /// Request timeout
    pub timeout: Duration,
    /// Target host
    pub host: String,
    /// Target port
    pub port: u16,
}

impl Default for LoadTestConfig {
    fn default() -> Self {
        Self {
            concurrency: 10,
            total_requests: 10000,
            timeout: Duration::from_secs(5),
            host: "127.0.0.1".to_string(),
            port: 6380,
        }
    }
}

impl LoadTestConfig {
    pub fn with_concurrency(mut self, n: usize) -> Self {
        self.concurrency = n;
        self
    }

    pub fn with_requests(mut self, n: u64) -> Self {
        self.total_requests = n;
        self
    }

    pub fn with_target(mut self, host: &str, port: u16) -> Self {
        self.host = host.to_string();
        self.port = port;
        self
    }
}

/// Load test statistics
#[derive(Debug, Clone)]
pub struct LoadTestStats {
    /// Total requests completed
    pub completed: u64,
    /// Successful requests
    pub success: u64,
    /// Failed requests
    pub failed: u64,
    /// Total duration
    pub duration: Duration,
    /// Average latency
    pub avg_latency: Duration,
    /// P50 latency
    pub p50_latency: Duration,
    /// P99 latency
    pub p99_latency: Duration,
    /// Min latency
    pub min_latency: Duration,
    /// Max latency
    pub max_latency: Duration,
    /// Requests per second
    pub rps: f64,
}

impl LoadTestStats {
    pub fn new() -> Self {
        Self {
            completed: 0,
            success: 0,
            failed: 0,
            duration: Duration::ZERO,
            avg_latency: Duration::ZERO,
            p50_latency: Duration::ZERO,
            p99_latency: Duration::ZERO,
            min_latency: Duration::MAX,
            max_latency: Duration::ZERO,
            rps: 0.0,
        }
    }

    /// Calculate from latency samples
    pub fn from_latencies(latencies: &[Duration], duration: Duration) -> Self {
        if latencies.is_empty() {
            return Self::new();
        }

        let mut sorted = latencies.to_vec();
        sorted.sort();

        let success = latencies.len() as u64;
        let sum: Duration = latencies.iter().sum();
        let avg = sum / latencies.len() as u32;

        let p50_idx = latencies.len() / 2;
        let p99_idx = (latencies.len() as f64 * 0.99) as usize;

        Self {
            completed: success,
            success,
            failed: 0,
            duration,
            avg_latency: avg,
            p50_latency: sorted[p50_idx],
            p99_latency: sorted[p99_idx.min(sorted.len() - 1)],
            min_latency: *sorted.first().unwrap(),
            max_latency: *sorted.last().unwrap(),
            rps: success as f64 / duration.as_secs_f64(),
        }
    }

    /// Format as report
    pub fn report(&self) -> String {
        format!(
            r#"Load Test Results
================
Completed: {}
Success: {}
Failed: {}
Duration: {:.2}s
RPS: {:.0}

Latency:
  Avg: {:.2}ms
  P50: {:.2}ms
  P99: {:.2}ms
  Min: {:.2}ms
  Max: {:.2}ms
"#,
            self.completed,
            self.success,
            self.failed,
            self.duration.as_secs_f64(),
            self.rps,
            self.avg_latency.as_secs_f64() * 1000.0,
            self.p50_latency.as_secs_f64() * 1000.0,
            self.p99_latency.as_secs_f64() * 1000.0,
            self.min_latency.as_secs_f64() * 1000.0,
            self.max_latency.as_secs_f64() * 1000.0,
        )
    }

    /// Export as JSON
    pub fn to_json(&self) -> String {
        format!(
            r#"{{"completed":{},"success":{},"failed":{},"duration_secs":{:.2},"rps":{:.0},"latency":{{"avg_ms":{:.2},"p50_ms":{:.2},"p99_ms":{:.2},"min_ms":{:.2},"max_ms":{:.2}}}}}"#,
            self.completed,
            self.success,
            self.failed,
            self.duration.as_secs_f64(),
            self.rps,
            self.avg_latency.as_secs_f64() * 1000.0,
            self.p50_latency.as_secs_f64() * 1000.0,
            self.p99_latency.as_secs_f64() * 1000.0,
            self.min_latency.as_secs_f64() * 1000.0,
            self.max_latency.as_secs_f64() * 1000.0,
        )
    }
}

impl Default for LoadTestStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Benchmark runner for quick performance tests
pub struct Benchmark {
    name: String,
    iterations: u64,
    warmup: u64,
}

impl Benchmark {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            iterations: 1000,
            warmup: 100,
        }
    }

    pub fn iterations(mut self, n: u64) -> Self {
        self.iterations = n;
        self
    }

    pub fn warmup(mut self, n: u64) -> Self {
        self.warmup = n;
        self
    }

    /// Run a benchmark
    pub fn run<F>(&self, mut f: F) -> BenchmarkResult
    where
        F: FnMut(),
    {
        // Warmup
        for _ in 0..self.warmup {
            f();
        }

        // Measure
        let mut latencies = Vec::with_capacity(self.iterations as usize);
        let start = Instant::now();

        for _ in 0..self.iterations {
            let iter_start = Instant::now();
            f();
            latencies.push(iter_start.elapsed());
        }

        let total_duration = start.elapsed();

        latencies.sort();
        let sum: Duration = latencies.iter().sum();
        let avg = sum / self.iterations as u32;

        BenchmarkResult {
            name: self.name.clone(),
            iterations: self.iterations,
            total_duration,
            avg_duration: avg,
            min_duration: *latencies.first().unwrap(),
            max_duration: *latencies.last().unwrap(),
            ops_per_sec: self.iterations as f64 / total_duration.as_secs_f64(),
        }
    }
}

/// Benchmark result
#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    pub name: String,
    pub iterations: u64,
    pub total_duration: Duration,
    pub avg_duration: Duration,
    pub min_duration: Duration,
    pub max_duration: Duration,
    pub ops_per_sec: f64,
}

impl BenchmarkResult {
    pub fn report(&self) -> String {
        format!(
            "{}: {:.0} ops/sec (avg: {:.2}μs, min: {:.2}μs, max: {:.2}μs)",
            self.name,
            self.ops_per_sec,
            self.avg_duration.as_secs_f64() * 1_000_000.0,
            self.min_duration.as_secs_f64() * 1_000_000.0,
            self.max_duration.as_secs_f64() * 1_000_000.0,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_test_stats() {
        let latencies: Vec<Duration> = (1..=100)
            .map(|i| Duration::from_micros(i * 10))
            .collect();
        let stats = LoadTestStats::from_latencies(&latencies, Duration::from_secs(1));

        assert_eq!(stats.completed, 100);
        assert!(stats.rps > 0.0);
        assert!(stats.p99_latency > stats.p50_latency);
    }

    #[test]
    fn test_benchmark() {
        let result = Benchmark::new("test_op")
            .iterations(100)
            .warmup(10)
            .run(|| {
                let _ = 1 + 1;
            });

        assert_eq!(result.iterations, 100);
        assert!(result.ops_per_sec > 0.0);
    }

    #[test]
    fn test_stats_report() {
        let latencies = vec![
            Duration::from_micros(100),
            Duration::from_micros(200),
            Duration::from_micros(150),
        ];
        let stats = LoadTestStats::from_latencies(&latencies, Duration::from_millis(100));
        let report = stats.report();

        assert!(report.contains("Load Test Results"));
        assert!(report.contains("RPS"));
    }
}
