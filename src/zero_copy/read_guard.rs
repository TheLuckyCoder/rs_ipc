use crate::zero_copy::ZeroCopySharedMessage;

/// RAII guard for zero-copy reads from shared memory.
/// Holds a reference count on a buffer, ensuring it won't be overwritten
/// while being read.
pub struct MessageReadGuard<'a> {
    message: &'a ZeroCopySharedMessage,
    buffer_idx: bool,
    sequence: u64,
}

impl<'a> MessageReadGuard<'a> {
    pub(crate) fn new(message: &'a ZeroCopySharedMessage, buffer_idx: bool, sequence: u64) -> Self {
        Self {
            message,
            buffer_idx,
            sequence,
        }
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn data(&self) -> &[u8] {
        self.message.buffer(self.buffer_idx)
    }
}

impl<'a> AsRef<[u8]> for MessageReadGuard<'a> {
    fn as_ref(&self) -> &[u8] {
        self.data()
    }
}

impl<'a> Drop for MessageReadGuard<'a> {
    fn drop(&mut self) {
        // Release the reader reference when the guard is dropped
        self.message.release_reader(self.buffer_idx);
    }
}
