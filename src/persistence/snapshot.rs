//! Snapshot Persistence
//!
//! Point-in-time snapshot for data recovery.

use bytes::Bytes;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Snapshot configuration
#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    /// Directory for snapshot files
    pub dir: PathBuf,
    /// Snapshot interval in seconds (0 = disabled)
    pub interval_secs: u64,
    /// Maximum number of snapshots to keep
    pub max_snapshots: usize,
    /// Compression enabled
    pub compress: bool,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("./data/snapshots"),
            interval_secs: 300, // 5 minutes
            max_snapshots: 5,
            compress: false,
        }
    }
}

impl SnapshotConfig {
    pub fn with_dir<P: Into<PathBuf>>(mut self, dir: P) -> Self {
        self.dir = dir.into();
        self
    }

    pub fn with_interval(mut self, secs: u64) -> Self {
        self.interval_secs = secs;
        self
    }
}

/// Snapshot file format:
/// - Magic: 4 bytes "CELX"
/// - Version: 1 byte
/// - Timestamp: 8 bytes (unix millis)
/// - Entry count: 4 bytes
/// - Entries: [key_len (4) + key + value_len (4) + value + ttl (8)]*

const SNAPSHOT_MAGIC: &[u8] = b"CELS";
const SNAPSHOT_VERSION: u8 = 1;

/// Snapshot entry
#[derive(Debug, Clone)]
pub struct SnapshotEntry {
    pub key: Bytes,
    pub value: Bytes,
    pub expires_at_ms: Option<u64>,
}

/// Snapshot writer/reader
pub struct Snapshot {
    config: SnapshotConfig,
}

impl Snapshot {
    pub fn new(config: SnapshotConfig) -> io::Result<Self> {
        fs::create_dir_all(&config.dir)?;
        Ok(Self { config })
    }

    /// Generate snapshot filename with timestamp
    fn snapshot_filename(&self) -> PathBuf {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        self.config.dir.join(format!("snapshot_{}.cel", timestamp))
    }

    /// Write a snapshot
    pub fn save(&self, entries: &[SnapshotEntry]) -> io::Result<PathBuf> {
        let path = self.snapshot_filename();
        let file = File::create(&path)?;
        let mut writer = BufWriter::new(file);

        // Write header
        writer.write_all(SNAPSHOT_MAGIC)?;
        writer.write_all(&[SNAPSHOT_VERSION])?;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        writer.write_all(&timestamp.to_le_bytes())?;
        writer.write_all(&(entries.len() as u32).to_le_bytes())?;

        // Write entries
        for entry in entries {
            // Key
            writer.write_all(&(entry.key.len() as u32).to_le_bytes())?;
            writer.write_all(&entry.key)?;

            // Value
            writer.write_all(&(entry.value.len() as u32).to_le_bytes())?;
            writer.write_all(&entry.value)?;

            // TTL (0 = no expiry)
            let ttl = entry.expires_at_ms.unwrap_or(0);
            writer.write_all(&ttl.to_le_bytes())?;
        }

        writer.flush()?;
        self.cleanup_old_snapshots()?;

        Ok(path)
    }

    /// Load the latest snapshot
    pub fn load_latest(&self) -> io::Result<Option<Vec<SnapshotEntry>>> {
        let latest = self.find_latest_snapshot()?;
        match latest {
            Some(path) => Ok(Some(self.load(&path)?)),
            None => Ok(None),
        }
    }

    /// Load a specific snapshot file
    pub fn load(&self, path: &Path) -> io::Result<Vec<SnapshotEntry>> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        // Read header
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if magic != SNAPSHOT_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid snapshot magic",
            ));
        }

        let mut version = [0u8; 1];
        reader.read_exact(&mut version)?;
        if version[0] != SNAPSHOT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unsupported snapshot version: {}", version[0]),
            ));
        }

        let mut timestamp_buf = [0u8; 8];
        reader.read_exact(&mut timestamp_buf)?;

        let mut count_buf = [0u8; 4];
        reader.read_exact(&mut count_buf)?;
        let count = u32::from_le_bytes(count_buf) as usize;

        // Read entries
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            // Key
            let mut key_len_buf = [0u8; 4];
            reader.read_exact(&mut key_len_buf)?;
            let key_len = u32::from_le_bytes(key_len_buf) as usize;
            let mut key_buf = vec![0u8; key_len];
            reader.read_exact(&mut key_buf)?;

            // Value
            let mut value_len_buf = [0u8; 4];
            reader.read_exact(&mut value_len_buf)?;
            let value_len = u32::from_le_bytes(value_len_buf) as usize;
            let mut value_buf = vec![0u8; value_len];
            reader.read_exact(&mut value_buf)?;

            // TTL
            let mut ttl_buf = [0u8; 8];
            reader.read_exact(&mut ttl_buf)?;
            let ttl = u64::from_le_bytes(ttl_buf);

            entries.push(SnapshotEntry {
                key: Bytes::from(key_buf),
                value: Bytes::from(value_buf),
                expires_at_ms: if ttl > 0 { Some(ttl) } else { None },
            });
        }

        Ok(entries)
    }

    /// Find the latest snapshot file
    fn find_latest_snapshot(&self) -> io::Result<Option<PathBuf>> {
        let mut latest: Option<(PathBuf, SystemTime)> = None;

        for entry in fs::read_dir(&self.config.dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map(|e| e == "cel").unwrap_or(false) {
                let metadata = entry.metadata()?;
                let modified = metadata.modified()?;
                if latest.as_ref().map(|(_, t)| modified > *t).unwrap_or(true) {
                    latest = Some((path, modified));
                }
            }
        }

        Ok(latest.map(|(p, _)| p))
    }

    /// Remove old snapshots beyond max_snapshots
    fn cleanup_old_snapshots(&self) -> io::Result<()> {
        let mut snapshots: Vec<_> = fs::read_dir(&self.config.dir)?
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "cel")
                    .unwrap_or(false)
            })
            .filter_map(|e| {
                e.metadata()
                    .ok()
                    .and_then(|m| m.modified().ok())
                    .map(|t| (e.path(), t))
            })
            .collect();

        snapshots.sort_by(|a, b| b.1.cmp(&a.1)); // Newest first

        for (path, _) in snapshots.into_iter().skip(self.config.max_snapshots) {
            fs::remove_file(path)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_snapshot_save_load() {
        let dir = tempdir().unwrap();
        let config = SnapshotConfig::default().with_dir(dir.path());
        let snapshot = Snapshot::new(config).unwrap();

        let entries = vec![
            SnapshotEntry {
                key: Bytes::from_static(b"key1"),
                value: Bytes::from_static(b"value1"),
                expires_at_ms: None,
            },
            SnapshotEntry {
                key: Bytes::from_static(b"key2"),
                value: Bytes::from_static(b"value2"),
                expires_at_ms: Some(1234567890000),
            },
        ];

        let path = snapshot.save(&entries).unwrap();
        let loaded = snapshot.load(&path).unwrap();

        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].key.as_ref(), b"key1");
        assert_eq!(loaded[1].expires_at_ms, Some(1234567890000));
    }
}
