use crate::memory_mapper::{SharedMemoryMapper, SlicePtrCast};
use crate::sync::LockFreeCondvar;
use crate::zero_copy::{ReadGuard, WriteGuard};
use std::ffi::c_void;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU16, AtomicU32, AtomicU64, AtomicUsize, Ordering};

// Bit masks for packing stopped flag and buffer index into sequence
const STOPPED_BIT_MASK: u64 = 1u64 << 63;
const BUFFER_IDX_BIT_MASK: u64 = 1u64 << 62;
const SEQUENCE_MASK: u64 = !(STOPPED_BIT_MASK | BUFFER_IDX_BIT_MASK);

/// Helper struct to unpack the combined sequence/stopped/buffer_idx value
#[derive(Default, Clone, Copy)]
struct SequenceState {
    sequence: u64,
    stopped: bool,
    buffer_idx: u8,
}

/// Zero-copy shared message with double buffering.
///
/// # Concurrency Model
/// - **Single Writer**: This implementation is designed for a single writer.
///   Multiple concurrent writers will have undefined behavior due to races in
///   buffer selection and reader count management.
/// - **Multiple Readers**: Supports multiple concurrent readers through
///   per-buffer reference counting.
///
/// # Memory Layout
/// The structure uses bit packing to minimize header size:
/// - Sequence (62 bits): Message version counter
/// - Buffer Index (1 bit): Which buffer contains the latest data (0 or 1)
/// - Stopped (1 bit): Termination flag
#[repr(C)]
pub struct ZeroCopySharedMessage<T: ?Sized = [u8]> {
    // Packed: sequence (62 bits) + buffer_idx (1 bit) + stopped (1 bit)
    sequence_and_flags: AtomicU64,
    
    data_sizes: [AtomicUsize; 2],
    
    // Reader tracking (per buffer)
    reader_counts: [AtomicU32; 2],     // Active readers (reference count)
    consumed_counts: [AtomicU32; 2],   // Total readers that have consumed (finished reading)
    
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
    
    /// Unpack the combined sequence/stopped/buffer_idx value
    /// Uses Acquire ordering to ensure visibility of prior writes
    #[inline]
    fn get_state(&self) -> SequenceState {
        let packed = self.sequence_and_flags.load(Ordering::Acquire);
        SequenceState {
            sequence: packed & SEQUENCE_MASK,
            stopped: (packed & STOPPED_BIT_MASK) != 0,
            buffer_idx: ((packed & BUFFER_IDX_BIT_MASK) >> 62) as u8,
        }
    }
    
    /// Internal: Acquire a write buffer, waiting for readers to finish.
    /// Returns the buffer index if successful, None if stopped.
    fn acquire_write_buffer(&self) -> Option<u8> {
        if self.is_stopped() {
            return None;
        }
        
        // 1. Determine which buffer to write to (the non-latest one)
        let current_state = self.get_state();
        let write_idx = 1 - current_state.buffer_idx;
        
        // 2. Always wait for readers to finish with this buffer
        while self.reader_counts[write_idx as usize].load(Ordering::Acquire) > 0 {
            if self.is_stopped() {
                return None;
            }
            self.reader_done_futex.wait();
        }
        
        // 3. Optionally wait for readers to consume from the other buffer (based on policy)
        // Only wait if there's a previous message (sequence > 0)
        let current_seq = self.get_state().sequence;
        let target_count = self.target_read_count.load(Ordering::Relaxed);
        if target_count > 0 && current_seq > 0 {
            let other_idx = write_idx ^ 1;
            let consumer_count = self.consumer_count.load(Ordering::Relaxed);
            let effective_target = target_count.min(consumer_count);
            
            if effective_target > 0 {
                let mut consumed = self.consumed_counts[other_idx as usize].load(Ordering::Acquire);
                while consumed < effective_target as u32 {
                    if self.is_stopped() {
                        return None;
                    }
                    self.reader_done_futex.wait();
                    consumed = self.consumed_counts[other_idx as usize].load(Ordering::Acquire);
                }
            }
        }
        
        Some(write_idx)
    }
    
    /// Write data to shared memory with zero-copy pattern.
    /// Returns the new sequence number if successful, None if stopped.
    pub fn write(&self, data: &[u8]) -> Option<u64> {
        // Acquire the write buffer
        let write_idx = self.acquire_write_buffer()?;
        
        // Write to buffer (this is the ONLY copy)
        let buffer = self.buffer_mut(write_idx);
        if data.len() > buffer.len() {
            panic!(
                "Data size ({} bytes) exceeds buffer capacity ({} bytes)",
                data.len(),
                buffer.len()
            );
        }
        buffer[..data.len()].copy_from_slice(data);
        
        // Publish the buffer
        self.publish_buffer(write_idx, data.len())
    }
    
    /// Acquire a write guard for zero-copy writing.
    /// Returns a WriteGuard that provides mutable access to a buffer.
    /// The buffer will be published when the guard's `publish()` method is called.
    pub fn acquire_write_guard(&self) -> Option<WriteGuard<'_>> {
        let write_idx = self.acquire_write_buffer()?;
        Some(WriteGuard::new(self, write_idx))
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
            let state = self.get_state();
            
            if state.stopped && state.sequence == last_seen_seq {
                return None;
            }
            
            // Check if new data available
            if state.sequence == last_seen_seq {
                if !block {
                    return None;
                }
                // Sleep until new data or stop
                self.writer_futex.wait();
                continue;
            }
            
            // Get latest buffer index from the state
            let buffer_idx = state.buffer_idx;
            
            // Register as reader BEFORE accessing buffer
            self.reader_counts[buffer_idx as usize].fetch_add(1, Ordering::AcqRel);
            
            // Verify buffer didn't change while registering
            let verify_state = self.get_state();
            if verify_state.buffer_idx != buffer_idx {
                // Buffer changed - unregister and retry
                self.release_reader(buffer_idx);
                continue;
            }
            
            // Return guard with direct pointer to buffer (ZERO COPY)
            return Some(ReadGuard::new(self, buffer_idx, state.sequence));
        }
    }
    
    /// Release a reader reference for the given buffer index.
    pub(crate) fn release_reader(&self, buffer_idx: u8) {
        self.reader_counts[buffer_idx as usize].fetch_sub(1, Ordering::AcqRel);
        
        // Increment consumed count to signal that this reader has finished
        self.consumed_counts[buffer_idx as usize].fetch_add(1, Ordering::AcqRel);
        
        // Wake any waiting writer (either waiting for buffer to be free or for consumption target)
        self.reader_done_futex.notify_all();
    }
    
    /// Get the current sequence number.
    pub fn current_sequence(&self) -> u64 {
        self.get_state().sequence
    }
    
    /// Check if there's new data compared to the given sequence.
    pub fn has_new_data(&self, last_seen_seq: u64) -> bool {
        self.get_state().sequence != last_seen_seq
    }
    
    /// Check if the shared memory has been stopped.
    #[inline]
    pub fn is_stopped(&self) -> bool {
        self.get_state().stopped
    }
    
    /// Signal that no more writes will occur.
    pub fn stop(&self) {
        // Set the stopped bit using fetch_or
        self.sequence_and_flags.fetch_or(STOPPED_BIT_MASK, Ordering::Relaxed);
        
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
    pub(crate) fn buffer_mut(&self, buffer_idx: u8) -> &mut [u8] {
        let offset = buffer_idx as usize * self.buffer_size;
        unsafe {
            let ptr = self.buffers.as_ptr().add(offset) as *mut u8;
            std::slice::from_raw_parts_mut(ptr, self.buffer_size)
        }
    }
    
    /// Publish a buffer that was written via WriteGuard.
    /// This is called by WriteGuard::publish().
    pub(crate) fn publish_buffer(&self, buffer_idx: u8, size: usize) -> Option<u64> {
        // Store the actual size
        self.data_sizes[buffer_idx as usize].store(size, Ordering::Release);
        
        // Reset reader count and consumed count for the buffer we're about to publish
        self.reader_counts[buffer_idx as usize].store(0, Ordering::Release);
        self.consumed_counts[buffer_idx as usize].store(0, Ordering::Release);
        
        // Atomically update: increment sequence, switch buffer, keep stopped flag
        loop {
            let old_packed = self.sequence_and_flags.load(Ordering::Acquire);
            let old_seq = old_packed & SEQUENCE_MASK;
            let stopped_flag = old_packed & STOPPED_BIT_MASK;
            
            // Check if stopped while we were writing
            if stopped_flag != 0 {
                return None;
            }
            
            let mut new_seq = (old_seq + 1) & SEQUENCE_MASK;
            if new_seq == 0 {
                new_seq = 1; // Skip 0 as it has special meaning
            }
            
            let new_buffer_flag = if buffer_idx == 1 { BUFFER_IDX_BIT_MASK } else { 0 };
            let new_packed = new_seq | new_buffer_flag | stopped_flag;
            
            // Try to atomically update
            if self.sequence_and_flags
                .compare_exchange(old_packed, new_packed, Ordering::Release, Ordering::Acquire)
                .is_ok()
            {
                // Wake readers
                self.writer_futex.notify_all();
                return Some(new_seq);
            }
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

    #[test]
    fn target_read_count_wait_for_one() {
        let memory = Arc::new(init("target_read_count_one", 1));
        memory.add_reader(); // Register 1 reader
        
        // First write
        memory.write(b"Message 1").unwrap();
        
        // Reader 1 reads the message
        let guard1 = memory.try_read(0).unwrap();
        assert_eq!(guard1.data(), b"Message 1");
        
        // Start a second write in a separate thread
        let memory_clone = memory.clone();
        let handle = thread::spawn(move || {
            memory_clone.write(b"Message 2")
        });
        
        // Give the writer time to start waiting
        thread::sleep(Duration::from_millis(50));
        
        // The writer should be blocked waiting for reader to finish
        // Now drop the guard to signal reader has consumed
        drop(guard1);
        
        // Writer should complete
        let seq2 = handle.join().unwrap().unwrap();
        assert_eq!(seq2, 2);
        
        // Verify second message
        let guard2 = memory.try_read(1).unwrap();
        assert_eq!(guard2.data(), b"Message 2");
    }

    #[test]
    fn target_read_count_wait_for_all() {
        let memory = Arc::new(init("target_read_count_all", u16::MAX)); // Wait for all
        memory.add_reader();
        memory.add_reader(); // Register 2 readers
        
        // First write
        memory.write(b"Message 1").unwrap();
        
        // Both readers read the message
        let guard1_r1 = memory.try_read(0).unwrap();
        let guard1_r2 = memory.try_read(0).unwrap();
        assert_eq!(guard1_r1.data(), b"Message 1");
        assert_eq!(guard1_r2.data(), b"Message 1");
        
        // Start a second write in a separate thread
        let memory_clone = memory.clone();
        let handle = thread::spawn(move || {
            memory_clone.write(b"Message 2")
        });
        
        // Give the writer time to start waiting
        thread::sleep(Duration::from_millis(50));
        
        // Drop first reader's guard
        drop(guard1_r1);
        
        // Writer should still be blocked (needs all readers)
        thread::sleep(Duration::from_millis(50));
        
        // Drop second reader's guard
        drop(guard1_r2);
        
        // Now writer should complete
        let seq2 = handle.join().unwrap().unwrap();
        assert_eq!(seq2, 2);
    }

    #[test]
    fn target_read_count_wait_for_specific() {
        let memory = Arc::new(init("target_read_count_specific", 2)); // Wait for 2
        memory.add_reader();
        memory.add_reader();
        memory.add_reader(); // Register 3 readers
        
        // First write
        memory.write(b"Message 1").unwrap();
        
        // Three readers read the message
        let guard1_r1 = memory.try_read(0).unwrap();
        let guard1_r2 = memory.try_read(0).unwrap();
        let guard1_r3 = memory.try_read(0).unwrap();
        
        // Start a second write in a separate thread
        let memory_clone = memory.clone();
        let handle = thread::spawn(move || {
            memory_clone.write(b"Message 2")
        });
        
        // Give the writer time to start waiting
        thread::sleep(Duration::from_millis(50));
        
        // Drop first reader's guard
        drop(guard1_r1);
        
        // Writer should still be blocked (needs 2 readers)
        thread::sleep(Duration::from_millis(50));
        
        // Drop second reader's guard - now we have 2 consumed
        drop(guard1_r2);
        
        // Writer should complete (doesn't need to wait for third reader)
        let seq2 = handle.join().unwrap().unwrap();
        assert_eq!(seq2, 2);
        
        // Third reader can still access the old message
        assert_eq!(guard1_r3.data(), b"Message 1");
    }

    #[test]
    fn target_read_count_fire_and_forget() {
        let memory = Arc::new(init("target_read_count_zero", 0)); // Fire and forget
        memory.add_reader();
        memory.add_reader(); // Register 2 readers
        
        // First write
        memory.write(b"Message 1").unwrap();
        
        // Readers read the message
        let guard1_r1 = memory.try_read(0).unwrap();
        let guard1_r2 = memory.try_read(0).unwrap();
        
        // Second write should NOT wait for readers to consume
        let seq2 = memory.write(b"Message 2").unwrap();
        assert_eq!(seq2, 2);
        
        // Old guards should still be valid
        assert_eq!(guard1_r1.data(), b"Message 1");
        assert_eq!(guard1_r2.data(), b"Message 1");
    }
}

