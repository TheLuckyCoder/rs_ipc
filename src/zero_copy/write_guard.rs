use crate::zero_copy::ZeroCopySharedMessage;

/// RAII guard for zero-copy writes to shared memory.
/// Provides mutable access to a buffer, ensuring proper publication
/// when dropped.
pub struct WriteGuard<'a> {
    message: &'a ZeroCopySharedMessage,
    buffer_idx: u8,
    published: bool,
}

impl<'a> WriteGuard<'a> {
    pub(crate) fn new(
        message: &'a ZeroCopySharedMessage,
        buffer_idx: u8,
    ) -> Self {
        Self {
            message,
            buffer_idx,
            published: false,
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
    /// This consumes the guard and publishes the data atomically.
    pub fn publish(mut self, size: usize) -> Option<u64> {
        if size > self.capacity() {
            return None;
        }
        
        let result = self.message.publish_buffer(self.buffer_idx, size);
        if result.is_some() {
            self.published = true;
        }
        result
    }
}

impl<'a> Drop for WriteGuard<'a> {
    fn drop(&mut self) {
        // If the guard is dropped without publishing, that's okay
        // The buffer won't be published and will be available for the next write
        if !self.published {
            // Just drop without publishing
        }
    }
}
