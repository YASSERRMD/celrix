//! Embedding Store
//!
//! Storage for vector embeddings keyed by cache keys.

use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::Instant;

/// An embedding entry with metadata
#[derive(Debug, Clone)]
pub struct EmbeddingEntry {
    /// The embedding vector
    pub embedding: Vec<f32>,
    /// Associated cached value (optional)
    pub value: Option<Bytes>,
    /// Metadata/context
    pub metadata: Option<String>,
    /// Creation time
    pub created_at: Instant,
    /// Last access time
    pub last_accessed: Instant,
}

impl EmbeddingEntry {
    pub fn new(embedding: Vec<f32>) -> Self {
        let now = Instant::now();
        Self {
            embedding,
            value: None,
            metadata: None,
            created_at: now,
            last_accessed: now,
        }
    }

    pub fn with_value(mut self, value: Bytes) -> Self {
        self.value = Some(value);
        self
    }

    pub fn with_metadata(mut self, metadata: String) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Update last accessed time
    pub fn touch(&mut self) {
        self.last_accessed = Instant::now();
    }

    /// Get embedding dimension
    pub fn dim(&self) -> usize {
        self.embedding.len()
    }
}

/// Concurrent embedding store
#[derive(Clone)]
pub struct EmbeddingStore {
    /// Key -> Embedding mapping
    embeddings: Arc<DashMap<Bytes, EmbeddingEntry>>,
    /// Expected embedding dimension
    dimension: usize,
}

impl EmbeddingStore {
    /// Create a new embedding store
    pub fn new(dimension: usize) -> Self {
        Self {
            embeddings: Arc::new(DashMap::new()),
            dimension,
        }
    }

    /// Get embedding dimension
    pub fn dimension(&self) -> usize {
        self.dimension
    }

    /// Store an embedding
    pub fn set(&self, key: Bytes, mut entry: EmbeddingEntry) -> Result<(), String> {
        if entry.dim() != self.dimension {
            return Err(format!(
                "Dimension mismatch: expected {}, got {}",
                self.dimension,
                entry.dim()
            ));
        }
        entry.touch();
        self.embeddings.insert(key, entry);
        Ok(())
    }

    /// Get an embedding
    pub fn get(&self, key: &Bytes) -> Option<EmbeddingEntry> {
        self.embeddings.get(key).map(|e| {
            let mut entry = e.value().clone();
            entry.touch();
            entry
        })
    }

    /// Get just the vector
    pub fn get_vector(&self, key: &Bytes) -> Option<Vec<f32>> {
        self.embeddings.get(key).map(|e| e.embedding.clone())
    }

    /// Delete an embedding
    pub fn del(&self, key: &Bytes) -> bool {
        self.embeddings.remove(key).is_some()
    }

    /// Check if key exists
    pub fn exists(&self, key: &Bytes) -> bool {
        self.embeddings.contains_key(key)
    }

    /// Get number of stored embeddings
    pub fn len(&self) -> usize {
        self.embeddings.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.embeddings.is_empty()
    }

    /// Find K nearest neighbors to query embedding
    pub fn find_nearest(&self, query: &[f32], k: usize, threshold: f32) -> Vec<(Bytes, f32)> {
        use super::similarity::cosine_similarity;

        let mut results: Vec<(Bytes, f32)> = self
            .embeddings
            .iter()
            .filter_map(|entry| {
                let sim = cosine_similarity(query, &entry.embedding);
                if sim >= threshold {
                    Some((entry.key().clone(), sim))
                } else {
                    None
                }
            })
            .collect();

        // Sort by similarity (descending)
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // Take top K
        results.truncate(k);
        results
    }

    /// Get all keys
    pub fn keys(&self) -> Vec<Bytes> {
        self.embeddings.iter().map(|e| e.key().clone()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_embedding_store() {
        let store = EmbeddingStore::new(4);

        let entry = EmbeddingEntry::new(vec![1.0, 0.0, 0.0, 0.0])
            .with_value(Bytes::from_static(b"value1"));

        store.set(Bytes::from_static(b"key1"), entry).unwrap();

        assert!(store.exists(&Bytes::from_static(b"key1")));
        assert_eq!(store.len(), 1);

        let retrieved = store.get(&Bytes::from_static(b"key1")).unwrap();
        assert_eq!(retrieved.embedding, vec![1.0, 0.0, 0.0, 0.0]);
    }

    #[test]
    fn test_dimension_mismatch() {
        let store = EmbeddingStore::new(4);
        let entry = EmbeddingEntry::new(vec![1.0, 0.0, 0.0]); // Wrong dimension

        let result = store.set(Bytes::from_static(b"key1"), entry);
        assert!(result.is_err());
    }

    #[test]
    fn test_find_nearest() {
        let store = EmbeddingStore::new(3);

        // Add some embeddings
        store
            .set(
                Bytes::from_static(b"a"),
                EmbeddingEntry::new(vec![1.0, 0.0, 0.0]),
            )
            .unwrap();
        store
            .set(
                Bytes::from_static(b"b"),
                EmbeddingEntry::new(vec![0.9, 0.1, 0.0]),
            )
            .unwrap();
        store
            .set(
                Bytes::from_static(b"c"),
                EmbeddingEntry::new(vec![0.0, 1.0, 0.0]),
            )
            .unwrap();

        // Query similar to 'a'
        let results = store.find_nearest(&[1.0, 0.0, 0.0], 2, 0.5);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0.as_ref(), b"a"); // Most similar
    }
}
