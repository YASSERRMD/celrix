//! Load Testing Framework

use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct LoadTestStats {
    pub completed: u64,
    pub success: u64,
    pub duration: Duration,
    pub avg_latency: Duration,
    pub p99_latency: Duration,
    pub rps: f64,
}

impl LoadTestStats {
    pub fn from_latencies(latencies: &[Duration], duration: Duration) -> Self {
        if latencies.is_empty() {
            return Self { completed: 0, success: 0, duration, avg_latency: Duration::ZERO, p99_latency: Duration::ZERO, rps: 0.0 };
        }
        let mut sorted = latencies.to_vec();
        sorted.sort();
        let sum: Duration = latencies.iter().sum();
        let avg = sum / latencies.len() as u32;
        let p99_idx = (latencies.len() as f64 * 0.99) as usize;
        Self {
            completed: latencies.len() as u64,
            success: latencies.len() as u64,
            duration,
            avg_latency: avg,
            p99_latency: sorted[p99_idx.min(sorted.len() - 1)],
            rps: latencies.len() as f64 / duration.as_secs_f64(),
        }
    }

    pub fn report(&self) -> String {
        format!("Completed: {}, RPS: {:.0}, Avg: {:.2}ms, P99: {:.2}ms",
            self.completed, self.rps,
            self.avg_latency.as_secs_f64() * 1000.0,
            self.p99_latency.as_secs_f64() * 1000.0)
    }
}

pub struct Benchmark {
    name: String,
    iterations: u64,
}

impl Benchmark {
    pub fn new(name: &str) -> Self { Self { name: name.to_string(), iterations: 1000 } }
    pub fn iterations(mut self, n: u64) -> Self { self.iterations = n; self }

    pub fn run<F: FnMut()>(&self, mut f: F) -> BenchmarkResult {
        let start = Instant::now();
        let mut latencies = Vec::with_capacity(self.iterations as usize);
        for _ in 0..self.iterations {
            let t = Instant::now();
            f();
            latencies.push(t.elapsed());
        }
        let total = start.elapsed();
        latencies.sort();
        BenchmarkResult {
            name: self.name.clone(),
            iterations: self.iterations,
            total_duration: total,
            ops_per_sec: self.iterations as f64 / total.as_secs_f64(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    pub name: String,
    pub iterations: u64,
    pub total_duration: Duration,
    pub ops_per_sec: f64,
}

impl BenchmarkResult {
    pub fn report(&self) -> String {
        format!("{}: {:.0} ops/sec", self.name, self.ops_per_sec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_benchmark() {
        let result = Benchmark::new("test").iterations(100).run(|| { let _ = 1 + 1; });
        assert!(result.ops_per_sec > 0.0);
    }
}
