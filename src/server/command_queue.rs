//! Command Queue
//!
//! MPMC bounded queue for routing commands from network tasks to workers.

use crossbeam::channel::{self, Receiver, Sender, TrySendError};
use std::time::Duration;

use crate::protocol::Command;
use bytes::Bytes;

/// Work item sent through the command queue
#[derive(Debug)]
pub struct WorkItem {
    /// The command to execute
    pub command: Command,
    /// Request ID for response matching
    pub request_id: u64,
    /// Response channel to send result back
    pub response_tx: tokio::sync::oneshot::Sender<WorkResult>,
}

/// Result of command execution
#[derive(Debug)]
pub enum WorkResult {
    /// Successful response
    Ok,
    /// Value response
    Value(Bytes),
    /// Integer response
    Integer(i64),
    /// Nil response
    Nil,
    /// Error response
    Error(String),
    /// Pong response
    Pong,
}

/// Bounded MPMC command queue
/// 
/// Uses crossbeam-channel for high-performance bounded queue.
/// Multiple producers (network handlers) can send work items,
/// and multiple consumers (workers) can process them.
#[derive(Clone)]
pub struct CommandQueue {
    sender: Sender<WorkItem>,
    receiver: Receiver<WorkItem>,
    capacity: usize,
}

impl CommandQueue {
    /// Create a new command queue with given capacity
    pub fn new(capacity: usize) -> Self {
        let (sender, receiver) = channel::bounded(capacity);
        Self {
            sender,
            receiver,
            capacity,
        }
    }

    /// Get a sender handle for producers
    pub fn sender(&self) -> Sender<WorkItem> {
        self.sender.clone()
    }

    /// Get a receiver handle for consumers
    pub fn receiver(&self) -> Receiver<WorkItem> {
        self.receiver.clone()
    }

    /// Try to send a work item without blocking
    pub fn try_send(&self, item: WorkItem) -> Result<(), TrySendError<WorkItem>> {
        self.sender.try_send(item)
    }

    /// Send a work item, blocking if queue is full
    pub fn send(&self, item: WorkItem) -> Result<(), channel::SendError<WorkItem>> {
        self.sender.send(item)
    }

    /// Send with timeout
    pub fn send_timeout(
        &self,
        item: WorkItem,
        timeout: Duration,
    ) -> Result<(), channel::SendTimeoutError<WorkItem>> {
        self.sender.send_timeout(item, timeout)
    }

    /// Receive a work item, blocking until available
    pub fn recv(&self) -> Result<WorkItem, channel::RecvError> {
        self.receiver.recv()
    }

    /// Try to receive without blocking
    pub fn try_recv(&self) -> Result<WorkItem, channel::TryRecvError> {
        self.receiver.try_recv()
    }

    /// Get current queue length (approximate)
    pub fn len(&self) -> usize {
        self.sender.len()
    }

    /// Check if queue is empty
    pub fn is_empty(&self) -> bool {
        self.sender.is_empty()
    }

    /// Check if queue is full
    pub fn is_full(&self) -> bool {
        self.sender.is_full()
    }

    /// Get queue capacity
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::thread;

    #[test]
    fn test_queue_send_recv() {
        let queue = CommandQueue::new(10);
        let (tx, _rx) = tokio::sync::oneshot::channel();

        let item = WorkItem {
            command: Command::Ping,
            request_id: 1,
            response_tx: tx,
        };

        queue.send(item).unwrap();
        let received = queue.recv().unwrap();
        assert_eq!(received.request_id, 1);
    }

    #[test]
    fn test_queue_mpmc() {
        let queue = CommandQueue::new(100);

        // Spawn producers
        let producers: Vec<_> = (0..4)
            .map(|i| {
                let q = queue.clone();
                thread::spawn(move || {
                    for j in 0..25 {
                        let (tx, _rx) = tokio::sync::oneshot::channel();
                        let item = WorkItem {
                            command: Command::Get {
                                key: Bytes::from(format!("key-{}-{}", i, j)),
                            },
                            request_id: (i * 25 + j) as u64,
                            response_tx: tx,
                        };
                        q.send(item).unwrap();
                    }
                })
            })
            .collect();

        // Spawn consumers
        let consumers: Vec<_> = (0..2)
            .map(|_| {
                let q = queue.clone();
                thread::spawn(move || {
                    let mut count = 0;
                    while count < 50 {
                        if q.try_recv().is_ok() {
                            count += 1;
                        }
                    }
                    count
                })
            })
            .collect();

        for p in producers {
            p.join().unwrap();
        }

        let total: usize = consumers.into_iter().map(|c| c.join().unwrap()).sum();
        assert_eq!(total, 100);
    }
}
