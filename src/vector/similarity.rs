//! Vector Similarity Functions
//!
//! SIMD-accelerated similarity computations.

/// SIMD operations trait for vectors
pub trait SimdOps {
    fn dot(&self, other: &Self) -> f32;
    fn magnitude(&self) -> f32;
    fn normalize(&mut self);
}

impl SimdOps for Vec<f32> {
    #[inline]
    fn dot(&self, other: &Self) -> f32 {
        dot_product(self, other)
    }

    #[inline]
    fn magnitude(&self) -> f32 {
        self.iter().map(|x| x * x).sum::<f32>().sqrt()
    }

    fn normalize(&mut self) {
        let mag = self.magnitude();
        if mag > 0.0 {
            for x in self.iter_mut() {
                *x /= mag;
            }
        }
    }
}

/// Compute dot product of two vectors
/// 
/// Uses unrolled loop for better CPU performance.
#[inline]
pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");
    
    let len = a.len();
    let mut sum = 0.0f32;
    
    // Process 4 elements at a time (manual unrolling)
    let chunks = len / 4;
    let remainder = len % 4;
    
    for i in 0..chunks {
        let idx = i * 4;
        sum += a[idx] * b[idx];
        sum += a[idx + 1] * b[idx + 1];
        sum += a[idx + 2] * b[idx + 2];
        sum += a[idx + 3] * b[idx + 3];
    }
    
    // Handle remainder
    for i in (len - remainder)..len {
        sum += a[i] * b[i];
    }
    
    sum
}

/// Compute cosine similarity between two vectors
/// 
/// Returns value in range [-1, 1] where 1 means identical direction.
#[inline]
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");
    
    let dot = dot_product(a, b);
    let mag_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let mag_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    
    let denom = mag_a * mag_b;
    if denom > 0.0 {
        dot / denom
    } else {
        0.0
    }
}

/// Compute Euclidean distance between two vectors
#[inline]
pub fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");
    
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y).powi(2))
        .sum::<f32>()
        .sqrt()
}

/// Normalize a vector in place
#[allow(dead_code)]
pub fn normalize_vector(v: &mut [f32]) {
    let mag: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if mag > 0.0 {
        for x in v.iter_mut() {
            *x /= mag;
        }
    }
}

/// Normalize and return a new vector
#[allow(dead_code)]
pub fn normalized(v: &[f32]) -> Vec<f32> {
    let mut result = v.to_vec();
    normalize_vector(&mut result);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dot_product() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        assert!((dot_product(&a, &b) - 32.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_identical() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        assert!((cosine_similarity(&a, &b) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_orthogonal() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];
        assert!(cosine_similarity(&a, &b).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_opposite() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![-1.0, 0.0, 0.0];
        assert!((cosine_similarity(&a, &b) + 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_euclidean_distance() {
        let a = vec![0.0, 0.0, 0.0];
        let b = vec![3.0, 4.0, 0.0];
        assert!((euclidean_distance(&a, &b) - 5.0).abs() < 1e-6);
    }

    #[test]
    fn test_normalize() {
        let v = vec![3.0, 4.0, 0.0];
        let n = normalized(&v);
        assert!((n[0] - 0.6).abs() < 1e-6);
        assert!((n[1] - 0.8).abs() < 1e-6);
        assert!(n[2].abs() < 1e-6);
    }
}
