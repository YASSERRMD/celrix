//! Semantic Cache
//!
//! Cache with semantic similarity lookup for AI/LLM responses.

use bytes::Bytes;

use super::embedding_store::{EmbeddingEntry, EmbeddingStore};

/// Result of semantic cache lookup
#[derive(Debug, Clone)]
pub struct SemanticResult {
    /// The cached key
    pub key: Bytes,
    /// The cached value
    pub value: Option<Bytes>,
    /// Similarity score
    pub similarity: f32,
    /// Original metadata
    pub metadata: Option<String>,
}

/// Semantic cache configuration
#[derive(Debug, Clone)]
pub struct SemanticCacheConfig {
    /// Minimum similarity threshold for cache hits
    pub similarity_threshold: f32,
    /// Maximum number of results to return
    pub max_results: usize,
    /// Embedding dimension
    pub dimension: usize,
}

impl Default for SemanticCacheConfig {
    fn default() -> Self {
        Self {
            similarity_threshold: 0.85,
            max_results: 5,
            dimension: 1536, // OpenAI ada-002 dimension
        }
    }
}

impl SemanticCacheConfig {
    pub fn with_threshold(mut self, threshold: f32) -> Self {
        self.similarity_threshold = threshold;
        self
    }

    pub fn with_dimension(mut self, dim: usize) -> Self {
        self.dimension = dim;
        self
    }

    pub fn with_max_results(mut self, max: usize) -> Self {
        self.max_results = max;
        self
    }
}

/// Semantic cache for AI/LLM query caching
#[derive(Clone)]
pub struct SemanticCache {
    /// Underlying embedding store
    store: EmbeddingStore,
    /// Configuration
    config: SemanticCacheConfig,
}

impl SemanticCache {
    /// Create a new semantic cache
    pub fn new(config: SemanticCacheConfig) -> Self {
        Self {
            store: EmbeddingStore::new(config.dimension),
            config,
        }
    }

    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(SemanticCacheConfig::default())
    }

    /// Store a query-response pair with embedding
    pub fn set(
        &self,
        key: Bytes,
        embedding: Vec<f32>,
        value: Bytes,
        metadata: Option<String>,
    ) -> Result<(), String> {
        let mut entry = EmbeddingEntry::new(embedding).with_value(value);
        if let Some(m) = metadata {
            entry = entry.with_metadata(m);
        }
        self.store.set(key, entry)
    }

    /// Get exact match by key
    pub fn get(&self, key: &Bytes) -> Option<SemanticResult> {
        self.store.get(key).map(|entry| SemanticResult {
            key: key.clone(),
            value: entry.value,
            similarity: 1.0,
            metadata: entry.metadata,
        })
    }

    /// Semantic lookup by embedding similarity
    pub fn semantic_get(&self, query_embedding: &[f32]) -> Vec<SemanticResult> {
        let nearest = self.store.find_nearest(
            query_embedding,
            self.config.max_results,
            self.config.similarity_threshold,
        );

        nearest
            .into_iter()
            .filter_map(|(key, similarity)| {
                self.store.get(&key).map(|entry| SemanticResult {
                    key,
                    value: entry.value,
                    similarity,
                    metadata: entry.metadata,
                })
            })
            .collect()
    }

    /// Check if there's a semantic match above threshold
    pub fn has_semantic_match(&self, query_embedding: &[f32]) -> bool {
        let nearest = self.store.find_nearest(query_embedding, 1, self.config.similarity_threshold);
        !nearest.is_empty()
    }

    /// Get the best semantic match
    pub fn best_match(&self, query_embedding: &[f32]) -> Option<SemanticResult> {
        self.semantic_get(query_embedding).into_iter().next()
    }

    /// Delete by key
    pub fn del(&self, key: &Bytes) -> bool {
        self.store.del(key)
    }

    /// Get cache size
    pub fn len(&self) -> usize {
        self.store.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.store.is_empty()
    }

    /// Get configuration
    pub fn config(&self) -> &SemanticCacheConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_cache() -> SemanticCache {
        let config = SemanticCacheConfig::default()
            .with_dimension(3)
            .with_threshold(0.8);
        SemanticCache::new(config)
    }

    #[test]
    fn test_semantic_cache_set_get() {
        let cache = create_test_cache();

        cache
            .set(
                Bytes::from_static(b"query1"),
                vec![1.0, 0.0, 0.0],
                Bytes::from_static(b"response1"),
                Some("test metadata".to_string()),
            )
            .unwrap();

        let result = cache.get(&Bytes::from_static(b"query1")).unwrap();
        assert_eq!(result.value, Some(Bytes::from_static(b"response1")));
        assert!((result.similarity - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_semantic_lookup() {
        let cache = create_test_cache();

        // Add some entries
        cache
            .set(
                Bytes::from_static(b"q1"),
                vec![1.0, 0.0, 0.0],
                Bytes::from_static(b"r1"),
                None,
            )
            .unwrap();
        cache
            .set(
                Bytes::from_static(b"q2"),
                vec![0.0, 1.0, 0.0],
                Bytes::from_static(b"r2"),
                None,
            )
            .unwrap();

        // Query similar to q1
        let results = cache.semantic_get(&[0.95, 0.1, 0.0]);
        assert!(!results.is_empty());
        assert_eq!(results[0].key.as_ref(), b"q1");
        assert!(results[0].similarity > 0.9);
    }

    #[test]
    fn test_best_match() {
        let cache = create_test_cache();

        cache
            .set(
                Bytes::from_static(b"q1"),
                vec![1.0, 0.0, 0.0],
                Bytes::from_static(b"r1"),
                None,
            )
            .unwrap();

        let best = cache.best_match(&[0.99, 0.01, 0.0]);
        assert!(best.is_some());
        assert_eq!(best.unwrap().key.as_ref(), b"q1");
    }

    #[test]
    fn test_no_match_below_threshold() {
        let cache = create_test_cache();

        cache
            .set(
                Bytes::from_static(b"q1"),
                vec![1.0, 0.0, 0.0],
                Bytes::from_static(b"r1"),
                None,
            )
            .unwrap();

        // Orthogonal query - should not match
        let results = cache.semantic_get(&[0.0, 1.0, 0.0]);
        assert!(results.is_empty());
    }
}
