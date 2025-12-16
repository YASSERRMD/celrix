//! CELRIX Server Binary
//!
//! High-performance in-memory cache server.

use celrix::server::Config;
use celrix::Server;
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("celrix=info".parse()?))
        .init();

    let args = Args::parse();

    info!(
        "Starting CELRIX server on {}:{}",
        args.bind, args.port
    );

    let config = Config::default()
        .with_bind(&args.bind)
        .with_port(args.port)
        .with_ttl_interval(args.ttl_interval);

    let server = Server::new(config);
    server.run().await?;

    Ok(())
}
