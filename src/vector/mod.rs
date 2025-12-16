//! Vector Module
//!
//! SIMD-accelerated vector operations and embedding storage.

mod embedding_store;
mod similarity;
mod semantic;

pub use embedding_store::{EmbeddingStore, EmbeddingEntry};
pub use similarity::{cosine_similarity, dot_product, euclidean_distance, SimdOps};
pub use semantic::{SemanticCache, SemanticResult};
