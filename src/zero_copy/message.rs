use crate::memory_mapper::{SharedMemoryMapper, SlicePtrCast};
use crate::sync::futex::{futex_wait, futex_wake_all, Futex};
use crate::zero_copy::ReadGuard;
use std::ffi::c_void;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU16, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering};

/// Simple condition variable for lock-free synchronization.
/// Unlike SharedCondvar, this doesn't require a mutex.
#[repr(transparent)]
struct LockFreeCondvar(Futex);

impl LockFreeCondvar {
    /// Wait on the condition variable if the value matches the expected value
    #[inline]
    fn wait(&self, expected: u32) {
        futex_wait(&self.0, expected);
    }

    /// Notify all threads waiting on this condition variable
    #[inline]
    fn notify_all(&self) {
        self.0.fetch_add(1, Ordering::Release);
        futex_wake_all(&self.0);
    }

    /// Load the current futex value for wait operations
    #[inline]
    fn load(&self, ordering: Ordering) -> u32 {
        self.0.load(ordering)
    }
}

impl Default for LockFreeCondvar {
    fn default() -> Self {
        Self(AtomicU32::new(0))
    }
}

#[repr(C)]
pub struct ZeroCopySharedMessage<T: ?Sized = [u8]> {
    // Versioning & state
    sequence: AtomicU64,
    stopped: AtomicU32,
    
    // Buffer management
    latest_buffer_idx: AtomicU8,
    data_sizes: [AtomicUsize; 2],
    
    // Reader tracking (per buffer)
    reader_counts: [AtomicU32; 2],
    
    // Synchronization (lock-free condition variables)
    writer_futex: LockFreeCondvar,     // Readers wait here for new data
    reader_done_futex: LockFreeCondvar, // Writer waits here for readers to finish
    
    // Reader wait policy
    target_read_count: AtomicU16,
    consumer_count: AtomicU16,
    
    // Buffer size (each buffer is half of remaining space)
    buffer_size: usize,
    
    // Double buffers follow the header in memory
    buffers: T,
}

impl ZeroCopySharedMessage {
    pub(crate) const fn size_of_fields() -> usize {
        size_of::<ZeroCopySharedMessage<[u8; 0]>>()
    }
    
    /// Write data to shared memory with zero-copy pattern.
    /// Returns the new sequence number if successful, None if stopped.
    pub fn write(&self, data: &[u8]) -> Option<u64> {
        if self.is_stopped() {
            return None;
        }
        
        // 1. Determine which buffer to write to (the non-latest one)
        let write_idx = 1 - self.latest_buffer_idx.load(Ordering::Acquire);
        
        // 2. Always wait for readers to finish with this buffer
        while self.reader_counts[write_idx as usize].load(Ordering::Acquire) > 0 {
            if self.is_stopped() {
                return None;
            }
            let futex_val = self.reader_done_futex.load(Ordering::Relaxed);
            self.reader_done_futex.wait(futex_val);
        }
        
        // 3. Optionally wait for readers to consume from the other buffer (based on policy)
        let target_count = self.target_read_count.load(Ordering::Relaxed);
        if target_count > 0 {
            let other_idx = write_idx ^ 1;
            let consumer_count = self.consumer_count.load(Ordering::Relaxed);
            let effective_target = target_count.min(consumer_count);
            
            if effective_target > 0 {
                let mut consumed = self.reader_counts[other_idx as usize].load(Ordering::Acquire);
                while consumed < effective_target as u32 {
                    if self.is_stopped() {
                        return None;
                    }
                    let futex_val = self.writer_futex.load(Ordering::Relaxed);
                    self.writer_futex.wait(futex_val);
                    consumed = self.reader_counts[other_idx as usize].load(Ordering::Acquire);
                }
            }
        }
        
        // 4. Write to buffer (this is the ONLY copy)
        let buffer = self.buffer_mut(write_idx);
        let len = data.len().min(buffer.len());
        buffer[..len].copy_from_slice(&data[..len]);
        self.data_sizes[write_idx as usize].store(len, Ordering::Release);
        
        // 5. Reset reader count for the buffer we're about to publish
        self.reader_counts[write_idx as usize].store(0, Ordering::Release);
        
        // 6. Publish: make this the latest buffer
        self.latest_buffer_idx.store(write_idx, Ordering::Release);
        
        // 7. Increment sequence and wake readers
        let new_seq = self.sequence.fetch_add(1, Ordering::Release) + 1;
        self.writer_futex.notify_all();
        
        Some(new_seq)
    }
    
    /// Try to read the next message without blocking.
    /// Returns a ReadGuard if new data is available, None otherwise.
    pub fn try_read(&self, last_seen_seq: u64) -> Option<ReadGuard<'_>> {
        self.read_internal(last_seen_seq, false)
    }
    
    /// Read the next message, blocking until new data is available or stopped.
    /// Returns a ReadGuard if successful, None if stopped with no new data.
    pub fn read(&self, last_seen_seq: u64) -> Option<ReadGuard<'_>> {
        self.read_internal(last_seen_seq, true)
    }
    
    fn read_internal(&self, last_seen_seq: u64, block: bool) -> Option<ReadGuard<'_>> {
        loop {
            if self.is_stopped() && !self.has_new_data(last_seen_seq) {
                return None;
            }
            
            let current_seq = self.sequence.load(Ordering::Acquire);
            
            // Check if new data available
            if current_seq == last_seen_seq {
                if !block {
                    return None;
                }
                // Sleep until new data or stop
                let futex_val = self.writer_futex.load(Ordering::Relaxed);
                self.writer_futex.wait(futex_val);
                continue;
            }
            
            // Get latest buffer index
            let buffer_idx = self.latest_buffer_idx.load(Ordering::Acquire);
            
            // Register as reader BEFORE accessing buffer
            self.reader_counts[buffer_idx as usize].fetch_add(1, Ordering::AcqRel);
            
            // Verify buffer didn't change while registering
            let verify_idx = self.latest_buffer_idx.load(Ordering::Acquire);
            if verify_idx != buffer_idx {
                // Buffer changed - unregister and retry
                self.release_reader(buffer_idx);
                continue;
            }
            
            // Return guard with direct pointer to buffer (ZERO COPY)
            return Some(ReadGuard::new(self, buffer_idx, current_seq));
        }
    }
    
    /// Release a reader reference for the given buffer index.
    pub(crate) fn release_reader(&self, buffer_idx: u8) {
        let old_count = self.reader_counts[buffer_idx as usize].fetch_sub(1, Ordering::AcqRel);
        
        // If we were the last reader on this buffer, wake any waiting writer
        if old_count == 1 {
            self.reader_done_futex.notify_all();
        }
    }
    
    /// Get the current sequence number.
    pub fn current_sequence(&self) -> u64 {
        self.sequence.load(Ordering::Acquire)
    }
    
    /// Check if there's new data compared to the given sequence.
    pub fn has_new_data(&self, last_seen_seq: u64) -> bool {
        self.sequence.load(Ordering::Acquire) != last_seen_seq
    }
    
    /// Check if the shared memory has been stopped.
    #[inline]
    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Relaxed) != 0
    }
    
    /// Signal that no more writes will occur.
    pub fn stop(&self) {
        self.stopped.store(1, Ordering::Relaxed);
        
        // Wake all waiting readers and writers
        self.writer_futex.notify_all();
        self.reader_done_futex.notify_all();
    }
    
    /// Set the target read count for writer wait policy.
    pub fn set_target_read_count(&self, count: u16) {
        self.target_read_count.store(count, Ordering::Relaxed);
    }
    
    /// Get the current target read count.
    pub fn get_target_read_count(&self) -> u16 {
        self.target_read_count.load(Ordering::Relaxed)
    }
    
    /// Register a new reader (increment consumer count).
    pub fn add_reader(&self) {
        self.consumer_count.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Unregister a reader (decrement consumer count).
    pub fn remove_reader(&self) {
        self.consumer_count.fetch_sub(1, Ordering::Relaxed);
    }
    
    /// Get the size of a specific buffer.
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }
    
    /// Get the data size for a specific buffer.
    pub(crate) fn data_size(&self, buffer_idx: u8) -> usize {
        self.data_sizes[buffer_idx as usize].load(Ordering::Acquire)
    }
    
    /// Get a reference to a specific buffer.
    pub(crate) fn buffer(&self, buffer_idx: u8) -> &[u8] {
        let offset = buffer_idx as usize * self.buffer_size;
        let size = self.data_size(buffer_idx);
        &self.buffers[offset..offset + size]
    }
    
    /// Get a mutable reference to a specific buffer (for writing).
    fn buffer_mut(&self, buffer_idx: u8) -> &mut [u8] {
        let offset = buffer_idx as usize * self.buffer_size;
        unsafe {
            let ptr = self.buffers.as_ptr().add(offset) as *mut u8;
            std::slice::from_raw_parts_mut(ptr, self.buffer_size)
        }
    }
}

unsafe impl SlicePtrCast for ZeroCopySharedMessage {
    unsafe fn cast_from_void_ptr(
        ptr: NonNull<c_void>,
        memory_size: usize,
    ) -> Option<NonNull<Self>> {
        let header_size = Self::size_of_fields();
        let buffer_space = memory_size.saturating_sub(header_size);
        
        // Need space for at least two buffers
        if buffer_space < 2 {
            return None;
        }
        
        // Each buffer gets half of the available space
        let buffer_size = buffer_space / 2;
        
        // Cast to our struct with the buffer space
        let slice_ptr: *mut [u8] =
            std::ptr::slice_from_raw_parts_mut(ptr.as_ptr().cast(), buffer_space);
        let msg_ptr = slice_ptr as *mut Self;
        
        // Initialize the buffer_size field if this is a new mapping
        let msg = NonNull::new(msg_ptr)?;
        let msg_ref = unsafe { &mut *msg.as_ptr() };
        
        // Only set buffer_size if it hasn't been set yet (creation scenario)
        // In the open scenario, this should already be set
        if msg_ref.buffer_size == 0 {
            msg_ref.buffer_size = buffer_size;
        }
        
        Some(msg)
    }
}

pub type ZeroCopySharedMessageMapper = SharedMemoryMapper<ZeroCopySharedMessage>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    const DEFAULT_SIZE: usize = 1024 * 1024;

    fn init(name: &str, target_read_count: u16) -> ZeroCopySharedMessageMapper {
        let c_name = CString::new(name).unwrap();
        let mapper = ZeroCopySharedMessageMapper::create(
            c_name,
            ZeroCopySharedMessage::size_of_fields() + DEFAULT_SIZE,
        )
        .unwrap();
        mapper.set_target_read_count(target_read_count);
        mapper
    }

    #[test]
    fn basic_write_read() {
        let data = b"Hello, zero-copy world!";
        let memory = Arc::new(init("basic_write_read", 0));

        // Write data
        let seq = memory.write(data).unwrap();
        assert_eq!(seq, 1);

        // Read data
        let guard = memory.try_read(0).unwrap();
        assert_eq!(guard.sequence(), 1);
        assert_eq!(guard.data(), data);
        
        // Reading again without new data should return None
        assert!(memory.try_read(1).is_none());
    }

    #[test]
    fn blocking_read() {
        let data = b"Blocking read test";
        let memory = Arc::new(init("blocking_read", 0));
        let memory_clone = memory.clone();

        // Start a thread that will write after a delay
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            memory_clone.write(data).unwrap();
        });

        // Blocking read should wait for the write
        let guard = memory.read(0).unwrap();
        assert_eq!(guard.data(), data);
    }

    #[test]
    fn multiple_readers() {
        let data = b"Multiple readers test";
        let memory = Arc::new(init("multiple_readers", 0));
        
        // Add readers
        memory.add_reader();
        memory.add_reader();

        // Write data
        memory.write(data).unwrap();

        // Multiple readers can access the same buffer
        let guard1 = memory.try_read(0).unwrap();
        let guard2 = memory.try_read(0).unwrap();
        
        assert_eq!(guard1.data(), data);
        assert_eq!(guard2.data(), data);
    }

    #[test]
    fn writer_waits_for_readers() {
        let memory = Arc::new(init("writer_waits", 0));
        memory.add_reader();
        
        // First write
        memory.write(b"First").unwrap();
        
        // Start reading (hold the guard)
        let guard1 = memory.try_read(0).unwrap();
        
        // Second write should succeed since target_read_count is 0
        memory.write(b"Second").unwrap();
        
        // Hold guard for second buffer
        let guard2 = memory.try_read(1).unwrap();
        
        // Now both buffers are held by readers
        // Try to write a third time - writer should wait for a buffer to be free
        let memory_clone = memory.clone();
        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            memory_clone.write(b"Third")
        });
        
        // Give the writer thread time to try writing
        thread::sleep(Duration::from_millis(50));
        
        // Drop the first guard to allow the writer to proceed
        drop(guard1);
        
        // The write should now complete
        let seq = handle.join().unwrap().unwrap();
        assert_eq!(seq, 3);
        
        // Second guard should still be valid
        assert_eq!(guard2.data(), b"Second");
    }

    #[test]
    fn stop_flag() {
        let memory = Arc::new(init("stop_flag", 0));
        
        // Write some data
        memory.write(b"Before stop").unwrap();
        
        // Stop the memory
        memory.stop();
        assert!(memory.is_stopped());
        
        // Writing after stop should return None
        assert!(memory.write(b"After stop").is_none());
        
        // Reading old data should still work
        let guard = memory.try_read(0).unwrap();
        assert_eq!(guard.data(), b"Before stop");
        
        // Blocking read with no new data should return None
        assert!(memory.read(1).is_none());
    }

    #[test]
    fn double_buffering() {
        let memory = Arc::new(init("double_buffering", 0));
        
        // Write first message
        let seq1 = memory.write(b"Message 1").unwrap();
        let guard1 = memory.try_read(0).unwrap();
        assert_eq!(guard1.sequence(), seq1);
        assert_eq!(guard1.data(), b"Message 1");
        
        // Write second message while holding first guard
        let seq2 = memory.write(b"Message 2").unwrap();
        
        // First guard should still be valid
        assert_eq!(guard1.data(), b"Message 1");
        
        // Read second message
        let guard2 = memory.try_read(seq1).unwrap();
        assert_eq!(guard2.sequence(), seq2);
        assert_eq!(guard2.data(), b"Message 2");
        
        // Both guards should be valid
        assert_eq!(guard1.data(), b"Message 1");
        assert_eq!(guard2.data(), b"Message 2");
    }
}

