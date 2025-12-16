//! Buffer Pool
//!
//! Pre-allocated buffer pool for zero-allocation hot path.

use bytes::BytesMut;
use crossbeam::queue::ArrayQueue;
use std::sync::Arc;

/// Default buffer size
const DEFAULT_BUFFER_SIZE: usize = 4096;

/// Pool of pre-allocated buffers
#[derive(Clone)]
pub struct BufferPool {
    pool: Arc<ArrayQueue<BytesMut>>,
    buffer_size: usize,
}

impl BufferPool {
    /// Create a new buffer pool with given capacity and buffer size
    pub fn new(capacity: usize, buffer_size: usize) -> Self {
        let pool = ArrayQueue::new(capacity);

        // Pre-allocate buffers
        for _ in 0..capacity {
            let buf = BytesMut::with_capacity(buffer_size);
            let _ = pool.push(buf);
        }

        Self {
            pool: Arc::new(pool),
            buffer_size,
        }
    }

    /// Create with default settings
    pub fn with_defaults() -> Self {
        Self::new(1024, DEFAULT_BUFFER_SIZE)
    }

    /// Get a buffer from the pool, or allocate a new one
    #[inline]
    pub fn get(&self) -> BytesMut {
        self.pool
            .pop()
            .unwrap_or_else(|| BytesMut::with_capacity(self.buffer_size))
    }

    /// Return a buffer to the pool
    #[inline]
    pub fn put(&self, mut buf: BytesMut) {
        buf.clear();
        // Only return if pool isn't full
        let _ = self.pool.push(buf);
    }

    /// Get current pool size
    pub fn len(&self) -> usize {
        self.pool.len()
    }

    /// Check if pool is empty
    pub fn is_empty(&self) -> bool {
        self.pool.is_empty()
    }

    /// Get the buffer size
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pool() {
        let pool = BufferPool::new(10, 1024);

        // Get all buffers
        let mut buffers: Vec<_> = (0..10).map(|_| pool.get()).collect();
        assert!(pool.is_empty());

        // Get one more (should allocate new)
        let extra = pool.get();
        assert_eq!(extra.capacity(), 1024);

        // Return all buffers
        for buf in buffers.drain(..) {
            pool.put(buf);
        }
        pool.put(extra);

        // Pool should have 10 (max capacity)
        assert_eq!(pool.len(), 10);
    }

    #[test]
    fn test_buffer_reuse() {
        let pool = BufferPool::new(1, 1024);

        let mut buf = pool.get();
        buf.extend_from_slice(b"test data");
        assert!(!buf.is_empty());

        pool.put(buf);

        // Get again - should be cleared
        let buf2 = pool.get();
        assert!(buf2.is_empty());
    }
}
