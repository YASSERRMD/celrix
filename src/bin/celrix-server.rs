//! CELRIX Server Binary
//!
//! High-performance in-memory cache server.
//! Supports both single-threaded and multi-threaded concurrent modes.

use celrix::server::{Config, WorkerPoolConfig};
use celrix::{ConcurrentServer, Server};
use clap::Parser;
use tracing::info;
use tracing_subscriber::{fmt, EnvFilter};

/// CELRIX Server - High-Performance In-Memory Cache
#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Bind address
    #[arg(short, long, default_value = "0.0.0.0")]
    bind: String,

    /// Port number
    #[arg(short, long, default_value_t = 6380)]
    port: u16,

    /// TTL cleaner interval in seconds
    #[arg(long, default_value_t = 10)]
    ttl_interval: u64,

    /// Number of KV worker threads (0 = auto-detect based on CPU cores)
    #[arg(long, default_value_t = 0)]
    kv_workers: usize,

    /// Number of Vector worker threads (0 = auto-detect, default 4)
    #[arg(long, default_value_t = 4)]
    vector_workers: usize,

    /// Enable concurrent/multi-threaded mode (Phase 2)
    #[arg(long, default_value_t = true)]
    concurrent: bool,

    /// Command queue capacity
    #[arg(long, default_value_t = 10000)]
    queue_capacity: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("celrix=info".parse()?))
        .init();

    let args = Args::parse();

    let mut config = Config::default()
        .with_bind(&args.bind)
        .with_port(args.port)
        .with_ttl_interval(args.ttl_interval);

    config.kv_workers = args.kv_workers;
    config.vector_workers = args.vector_workers;

    if args.concurrent {
        info!(
            "Starting CELRIX concurrent server on {}:{} with {} KV workers and {} Vector workers",
            args.bind, args.port, 
            if args.kv_workers == 0 { num_cpus::get() } else { args.kv_workers },
            args.vector_workers
        );

        let worker_config = WorkerPoolConfig {
            num_workers: args.kv_workers, // Ignored
            pin_to_cores: true, // Ignored
            queue_capacity: args.queue_capacity,
        };

        let server = ConcurrentServer::with_worker_config(config, worker_config);
        server.run().await?;
    } else {
        info!(
            "Starting CELRIX single-threaded server on {}:{}",
            args.bind, args.port
        );

        let server = Server::new(config);
        server.run().await?;
    }

    Ok(())
}
