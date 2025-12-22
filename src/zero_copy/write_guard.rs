use crate::zero_copy::message::WriterGuard;
use crate::zero_copy::ZeroCopySharedMessage;

/// RAII guard for zero-copy writes to shared memory.
/// Provides mutable access to a buffer, ensuring proper publication
/// and writer mutex release when dropped.
pub struct MessageWriteGuard<'a> {
    message: &'a ZeroCopySharedMessage,
    guard: WriterGuard<'a>,
    buffer_idx: bool,
}

impl<'a> MessageWriteGuard<'a> {
    pub(crate) fn new(
        message: &'a ZeroCopySharedMessage,
        guard: WriterGuard<'a>,
        buffer_idx: bool,
    ) -> Self {
        Self {
            message,
            guard,
            buffer_idx,
        }
    }
    
    /// Get mutable access to the write buffer.
    pub fn buffer_mut(&mut self) -> &mut [u8] {
        self.message.buffer_mut(self.buffer_idx)
    }
    
    /// Get the maximum buffer size.
    pub fn capacity(&self) -> usize {
        self.message.buffer_size()
    }
    
    /// Publish the written data with the given size.
    /// Returns the new sequence number if successful, None if stopped.
    /// This consumes the guard, publishes the data atomically, and releases the writer mutex.
    pub fn publish(self, size: usize) -> Option<u64> {
        if size > self.capacity() {
            return None;
        }
        
        let result = self.message.publish_buffer(self.buffer_idx, self.guard, size);

        result
    }
}
