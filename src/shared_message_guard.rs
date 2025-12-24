use crate::SharedMessage;
use crate::shared_message::PayloadWriteGuard;

/// Holds a reference count on a buffer, ensuring it won't be overwritten
/// while being read.
pub struct MessageReadGuard<'a> {
    message: &'a SharedMessage,
    sequence: u64,
}

impl<'a> MessageReadGuard<'a> {
    pub(crate) fn new(message: &'a SharedMessage, sequence: u64) -> Self {
        Self { message, sequence }
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn data(&self) -> &[u8] {
        self.message.payload_ref()
    }
}

impl<'a> AsRef<[u8]> for MessageReadGuard<'a> {
    fn as_ref(&self) -> &[u8] {
        self.data()
    }
}

impl<'a> Drop for MessageReadGuard<'a> {
    fn drop(&mut self) {
        self.message.release_active_reader();
    }
}

/// RAII guard for zero-copy writes to shared memory.
/// Provides mutable access to a buffer, ensuring proper publication
/// and writer mutex release when dropped.
pub struct MessageWriteGuard<'a> {
    message: &'a SharedMessage,
    guard: PayloadWriteGuard<'a>,
}

impl<'a> MessageWriteGuard<'a> {
    pub(crate) fn new(message: &'a SharedMessage, guard: PayloadWriteGuard<'a>) -> Self {
        Self { message, guard }
    }

    pub fn buffer_mut(&mut self) -> &mut [u8] {
        self.message.payload_mut(&self.guard)
    }

    /// Get the maximum buffer size.
    pub fn capacity(&self) -> usize {
        self.message.capacity()
    }

    /// Publish the written data with the given size.
    /// Returns the new sequence number if successful, None if stopped.
    /// This consumes the guard, publishes the data atomically, and releases the writer mutex.
    pub fn publish(self, size: usize) -> Option<u64> {
        if size > self.capacity() {
            return None;
        }

        self.message.publish_write(self.guard, size)
    }
}
