use crate::memory_mapper::{SharedMemoryMapper, SlicePtrCast};
use crate::shared_message_guard::{MessageReadGuard, MessageWriteGuard};
use crate::sync::condvar::SharedCondvar;
use crate::sync::{SharedMutex, SharedMutexGuard};
use std::cell::UnsafeCell;
use std::ffi::c_void;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU16, AtomicU64, AtomicUsize, Ordering};

pub(crate) type PayloadWriteGuard<'a> = SharedMutexGuard<'a, ()>;

// Bit masks for packing stopped flag and buffer index into sequence
const STOPPED_BIT_MASK: u64 = 1u64 << 63;
const WRITING_IN_PROGRESS: u64 = 1u64 << 62;
const SEQUENCE_MASK: u64 = !(STOPPED_BIT_MASK | WRITING_IN_PROGRESS);

/// Helper struct to unpack the combined sequence/stopped/buffer_idx value
#[derive(Default, Clone, Copy)]
struct SequenceState {
    sequence: u64,
    stopped: bool,
    writing_in_progress: bool,
}

impl SequenceState {
    #[inline]
    fn from_packed(packed: u64) -> Self {
        Self {
            sequence: packed & SEQUENCE_MASK,
            stopped: (packed & STOPPED_BIT_MASK) != 0,
            writing_in_progress: (packed & WRITING_IN_PROGRESS) != 0,
        }
    }

    #[inline]
    fn to_packed(&self) -> u64 {
        ((self.stopped as u64) << 63)
            | ((self.writing_in_progress as u64) << 62)
            | self.sequence & SEQUENCE_MASK
    }
}

#[repr(C)]
pub struct SharedMessage<T: ?Sized = [u8]> {
    sequence_and_flags: AtomicU64,

    // Reader tracking
    active_readers_count: AtomicU16,
    consumed_counts: AtomicU16,

    // Reader wait policy
    target_read_count: AtomicU16,
    consumer_count: AtomicU16,

    // Synchronization (lock-free condition variables)
    writer_futex: SharedCondvar,      // Readers wait here for new data
    reader_done_futex: SharedCondvar, // Writer waits here for readers to finish

    // Writer mutex (serializes multiple writers)
    writer_mutex: SharedMutex<()>,

    data_size: AtomicUsize,
    data: UnsafeCell<T>,
}

impl SharedMessage {
    pub(crate) const fn size_of_fields() -> usize {
        size_of::<SharedMessage<[u8; 0]>>()
    }

    pub(crate) const fn align_of_fields() -> usize {
        align_of::<SharedMessage<[u8; 0]>>()
    }

    /// Unpack the combined sequence/stopped/buffer_idx value
    #[inline]
    fn get_state(&self) -> SequenceState {
        SequenceState::from_packed(self.sequence_and_flags.load(Ordering::Acquire))
    }

    /// Acquire a write buffer, waiting for any active readers to finish.
    /// Returns the buffer index if successful, None if stopped.
    /// This allows the writer to prepare the next message while readers consume the current one.
    fn acquire_write_buffer(&self) -> Option<PayloadWriteGuard<'_>> {
        if self.is_stopped() {
            return None;
        }

        let writer_guard = self.writer_mutex.lock();

        // Wait for readers to consume the already existing data (based on policy)
        let current_seq = self.get_state().sequence;

        let mut target_count = self
            .target_read_count
            .load(Ordering::Acquire)
            .min(self.consumer_count.load(Ordering::Acquire));

        // Only wait if there's a previous message to be consumed (sequence > 0)
        if target_count > 0 && current_seq > 0 {
            let mut consumed = self.consumed_counts.load(Ordering::Acquire);
            while consumed < target_count {
                if self.is_stopped() {
                    return None;
                }
                self.reader_done_futex.wait();
                consumed = self.consumed_counts.load(Ordering::Acquire);
                target_count = target_count.min(self.consumer_count.load(Ordering::Acquire));
            }
        }

        // Mark in progress so no one new will try to write
        self.sequence_and_flags
            .fetch_or(WRITING_IN_PROGRESS, Ordering::Release);

        // Wait for active readers to finish
        while self.active_readers_count.load(Ordering::Acquire) > 0 {
            if self.is_stopped() {
                self.sequence_and_flags
                    .fetch_and(!WRITING_IN_PROGRESS, Ordering::Release);
                return None;
            }
            self.reader_done_futex.wait();
        }

        Some(writer_guard)
    }

    /// Release a reader reference
    pub(crate) fn release_active_reader(&self) {
        self.active_readers_count.fetch_sub(1, Ordering::AcqRel);

        // Increment consumed count to signal that this reader has finished
        self.consumed_counts.fetch_add(1, Ordering::AcqRel);

        self.reader_done_futex.notify_all();
    }

    pub(crate) fn capacity(&self) -> usize {
        let data = unsafe { &*(self.data.get()) };
        data.len()
    }

    pub(crate) fn payload_ref(&self) -> &[u8] {
        let data = unsafe { &*(self.data.get()) };
        let size = self.data_size.load(Ordering::Acquire);
        &data[..size]
    }

    /// Get a mutable reference to a specific buffer (for writing).
    /// SAFETY: This is safe because:
    /// 1. The buffers are behind UnsafeCell, which allows interior mutability
    /// 2. The writer mutex ensures only one writer accesses this buffer at a time
    /// 3. The reference counting ensures no readers access this buffer while being written
    pub(crate) fn payload_mut(&self, _write_guard: &PayloadWriteGuard) -> &mut [u8] {
        unsafe { &mut *self.data.get() }
    }

    pub(crate) fn publish_write(
        &self,
        _write_guard: PayloadWriteGuard<'_>,
        size: usize,
    ) -> Option<u64> {
        // Store the actual size
        self.data_size.store(size, Ordering::Release);
        // Reset consumed count for the buffer we're about to publish
        self.consumed_counts.store(0, Ordering::Release);

        // Atomically update: increment sequence, remove in_writing flag, keep the stopped flag
        loop {
            let old_packed = self.sequence_and_flags.load(Ordering::Acquire);
            let old_state = SequenceState::from_packed(old_packed);

            let mut new_seq = (old_state.sequence + 1) & SEQUENCE_MASK;
            if new_seq == 0 {
                new_seq = 1; // Skip 0 as it has special meaning
            }

            let new_packed = SequenceState {
                sequence: new_seq,
                stopped: old_state.stopped,
                writing_in_progress: false,
            }
            .to_packed();

            // Try to atomically update
            if self
                .sequence_and_flags
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

// Public functions
impl SharedMessage {
    /// Returns the new sequence number if successful, None if stopped.
    pub fn write(&self, data: &[u8]) -> Option<u64> {
        let writer_guard = self.acquire_write_buffer()?;

        let buffer = self.payload_mut(&writer_guard);
        if data.len() > buffer.len() {
            panic!(
                "Data size ({} bytes) exceeds buffer capacity ({} bytes)",
                data.len(),
                buffer.len()
            );
        }
        buffer[..data.len()].copy_from_slice(data);

        self.publish_write(writer_guard, data.len())
    }

    /// Returns a WriteGuard that provides mutable access to a buffer.
    /// The buffer will be published when the guard's `publish()` method is called.
    /// The writer mutex is acquired here and will be held until the guard is published or dropped.
    pub fn acquire_write_guard(&self) -> Option<MessageWriteGuard<'_>> {
        let write_guard = self.acquire_write_buffer()?;
        Some(MessageWriteGuard::new(self, write_guard))
    }

    /// Returns a ReadGuard if new data is available, None otherwise.
    pub fn read(&self, last_seen_seq: u64, block: bool) -> Option<MessageReadGuard<'_>> {
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

            if state.writing_in_progress {
                // Wait until it's finished writing
                self.writer_futex.wait();
                continue;
            }

            // Register as a reader BEFORE accessing buffer
            self.active_readers_count.fetch_add(1, Ordering::AcqRel);

            // Verify that the state didn't change while registering
            let verify_state = self.get_state();
            if verify_state.writing_in_progress || verify_state.sequence != state.sequence {
                // Buffer changed - unregister and retry
                self.active_readers_count.fetch_sub(1, Ordering::AcqRel);
                continue;
            }

            return Some(MessageReadGuard::new(self, state.sequence));
        }
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
    pub fn is_stopped(&self) -> bool {
        self.get_state().stopped
    }

    pub fn stop(&self) {
        self.sequence_and_flags
            .fetch_or(STOPPED_BIT_MASK, Ordering::AcqRel);

        // Wake all waiting readers and writers
        self.writer_futex.notify_all();
        self.reader_done_futex.notify_all();
    }

    /// Set the target read count for writer wait policy.
    pub fn set_target_read_count(&self, count: u16) {
        self.target_read_count.store(count, Ordering::Relaxed);
    }

    /// Register a new reader
    pub fn add_reader(&self) {
        self.consumer_count.fetch_add(1, Ordering::Release);
    }

    /// Unregister a reader
    pub fn remove_reader(&self) {
        self.consumer_count.fetch_sub(1, Ordering::Release);
    }
}

// SAFETY: SharedMessage is safe to share across threads because:
// 1. All mutable access to data is protected by the writer mutex (serializes writers)
// 2. Reader access is protected by reference counting (ensures no concurrent read/write)
// 3. The UnsafeCell is only used to enable interior mutability for the buffer data,
//    and all access is properly synchronized through atomics and the writer mutex
unsafe impl<T: ?Sized> Sync for SharedMessage<T> {}

unsafe impl SlicePtrCast for SharedMessage {
    unsafe fn cast_from_void_ptr(
        ptr: NonNull<c_void>,
        memory_size: usize,
    ) -> Option<NonNull<Self>> {
        if ptr.align_offset(Self::align_of_fields()) != 0 {
            return None;
        }

        let header_size = Self::size_of_fields();
        let payload_size = memory_size.saturating_sub(header_size);
        if payload_size == 0 {
            return None;
        }

        let slice_ptr: *mut [u8] =
            std::ptr::slice_from_raw_parts_mut(ptr.as_ptr().cast(), payload_size);
        NonNull::new(slice_ptr as *mut Self)
    }
}

pub type SharedMessageMapper = SharedMemoryMapper<SharedMessage>;
