//! TTL Cleaner
//!
//! Background task that periodically removes expired keys.

use std::time::Duration;
use tokio::time::interval;
use tracing::{debug, info};

use super::Store;

/// Background TTL cleanup task
pub struct TtlCleaner {
    store: Store,
    interval: Duration,
}

impl TtlCleaner {
    /// Create a new TTL cleaner
    pub fn new(store: Store, interval_secs: u64) -> Self {
        Self {
            store,
            interval: Duration::from_secs(interval_secs),
        }
    }

    /// Run the cleaner (should be spawned as a task)
    pub async fn run(self) {
        let mut ticker = interval(self.interval);
        info!(
            "TTL cleaner started, interval: {:?}",
            self.interval
        );

        loop {
            ticker.tick().await;
            let removed = self.store.cleanup_expired();
            if removed > 0 {
                debug!(removed = removed, "Cleaned up expired keys");
            }
        }
    }

    /// Spawn the cleaner as a background task
    pub fn spawn(store: Store, interval_secs: u64) -> tokio::task::JoinHandle<()> {
        let cleaner = Self::new(store, interval_secs);
        tokio::spawn(cleaner.run())
    }
}
