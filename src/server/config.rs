//! Server Configuration

/// Server configuration
#[derive(Debug, Clone)]
pub struct Config {
    /// Bind address
    pub bind: String,

    /// Port number
    pub port: u16,

    /// Number of KV worker threads (0 = auto-detect)
    pub kv_workers: usize,

    /// Number of Vector worker threads (0 = auto-detect)
    pub vector_workers: usize,

    /// TTL cleaner interval in seconds
    pub ttl_cleaner_interval: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0".to_string(),
            port: 6380,
            kv_workers: 0,     // Auto-detect (typically num_cores)
            vector_workers: 4, // Conservative default for heavy vector ops
            ttl_cleaner_interval: 10,
        }
    }
}

impl Config {
    /// Create a new config with custom port
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Create a new config with custom bind address
    pub fn with_bind(mut self, bind: impl Into<String>) -> Self {
        self.bind = bind.into();
        self
    }

    /// Set TTL cleaner interval
    pub fn with_ttl_interval(mut self, interval: u64) -> Self {
        self.ttl_cleaner_interval = interval;
        self
    }
}
