//! Append-Only File Persistence
//!
//! Write-ahead logging for durability.

use bytes::{BufMut, Bytes, BytesMut};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

/// AOF configuration
#[derive(Debug, Clone)]
pub struct AofConfig {
    /// AOF file path
    pub path: PathBuf,
    /// Sync mode
    pub sync_mode: AofSyncMode,
    /// Rewrite threshold (number of entries before compaction)
    pub rewrite_threshold: usize,
}

/// AOF sync modes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AofSyncMode {
    /// No sync (fastest, least safe)
    No,
    /// Sync every second
    EverySecond,
    /// Sync on every write (slowest, safest)
    Always,
}

impl Default for AofConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("./data/celrix.aof"),
            sync_mode: AofSyncMode::EverySecond,
            rewrite_threshold: 100_000,
        }
    }
}

impl AofConfig {
    pub fn with_path<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.path = path.into();
        self
    }

    pub fn with_sync_mode(mut self, mode: AofSyncMode) -> Self {
        self.sync_mode = mode;
        self
    }
}

/// AOF entry type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AofOpType {
    Set = 1,
    Del = 2,
}

/// AOF entry
#[derive(Debug, Clone)]
pub struct AofEntry {
    pub op: AofOpType,
    pub key: Bytes,
    pub value: Option<Bytes>,
    pub ttl_ms: Option<u64>,
    pub timestamp_ms: u64,
}

impl AofEntry {
    pub fn set(key: Bytes, value: Bytes, ttl_ms: Option<u64>) -> Self {
        Self {
            op: AofOpType::Set,
            key,
            value: Some(value),
            ttl_ms,
            timestamp_ms: Self::now_ms(),
        }
    }

    pub fn del(key: Bytes) -> Self {
        Self {
            op: AofOpType::Del,
            key,
            value: None,
            ttl_ms: None,
            timestamp_ms: Self::now_ms(),
        }
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    /// Encode entry to bytes
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();

        // Op type (1 byte)
        buf.put_u8(self.op as u8);

        // Timestamp (8 bytes)
        buf.put_u64_le(self.timestamp_ms);

        // Key length + key
        buf.put_u32_le(self.key.len() as u32);
        buf.put_slice(&self.key);

        // Value (if SET)
        if let Some(ref value) = self.value {
            buf.put_u32_le(value.len() as u32);
            buf.put_slice(value);
        } else {
            buf.put_u32_le(0);
        }

        // TTL (8 bytes, 0 = no expiry)
        buf.put_u64_le(self.ttl_ms.unwrap_or(0));

        buf.freeze()
    }
}

/// AOF writer (thread-safe)
pub struct AofWriter {
    config: AofConfig,
    writer: Arc<Mutex<BufWriter<File>>>,
    entry_count: Arc<std::sync::atomic::AtomicUsize>,
}

impl AofWriter {
    /// Create or open AOF file
    pub fn open(config: AofConfig) -> io::Result<Self> {
        if let Some(parent) = config.path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&config.path)?;

        Ok(Self {
            config,
            writer: Arc::new(Mutex::new(BufWriter::new(file))),
            entry_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        })
    }

    /// Append an entry to the AOF
    pub fn append(&self, entry: &AofEntry) -> io::Result<()> {
        let encoded = entry.encode();
        let mut writer = self.writer.lock().unwrap();

        // Write length prefix + data
        writer.write_all(&(encoded.len() as u32).to_le_bytes())?;
        writer.write_all(&encoded)?;

        // Sync based on config
        if self.config.sync_mode == AofSyncMode::Always {
            writer.flush()?;
        }

        self.entry_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        Ok(())
    }

    /// Log a SET operation
    pub fn log_set(&self, key: Bytes, value: Bytes, ttl_ms: Option<u64>) -> io::Result<()> {
        let entry = AofEntry::set(key, value, ttl_ms);
        self.append(&entry)
    }

    /// Log a DEL operation
    pub fn log_del(&self, key: Bytes) -> io::Result<()> {
        let entry = AofEntry::del(key);
        self.append(&entry)
    }

    /// Flush buffered writes
    pub fn flush(&self) -> io::Result<()> {
        let mut writer = self.writer.lock().unwrap();
        writer.flush()
    }

    /// Get entry count
    pub fn entry_count(&self) -> usize {
        self.entry_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Check if rewrite is needed
    pub fn needs_rewrite(&self) -> bool {
        self.entry_count() >= self.config.rewrite_threshold
    }
}

impl Clone for AofWriter {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            writer: self.writer.clone(),
            entry_count: self.entry_count.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_aof_write() {
        let dir = tempdir().unwrap();
        let config = AofConfig::default().with_path(dir.path().join("test.aof"));
        let aof = AofWriter::open(config).unwrap();

        aof.log_set(
            Bytes::from_static(b"key1"),
            Bytes::from_static(b"value1"),
            None,
        )
        .unwrap();

        aof.log_del(Bytes::from_static(b"key1")).unwrap();
        aof.flush().unwrap();

        assert_eq!(aof.entry_count(), 2);
    }

    #[test]
    fn test_aof_entry_encode() {
        let entry = AofEntry::set(
            Bytes::from_static(b"key"),
            Bytes::from_static(b"value"),
            Some(60000),
        );
        let encoded = entry.encode();
        assert!(!encoded.is_empty());
        assert_eq!(encoded[0], AofOpType::Set as u8);
    }
}
