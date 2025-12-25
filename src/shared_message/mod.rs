use crate::memory_mapper::{SharedMemoryMapper, SlicePtrCast};
use crate::sync::condvar::SharedCondvar;
use crate::sync::{SharedMutex, SharedMutexGuard};
use packed::*;
use std::cell::UnsafeCell;
use std::ffi::c_void;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

mod guard;
mod packed;

pub use guard::*;

pub(crate) type PayloadWriteGuard<'a> = SharedMutexGuard<'a, ()>;

#[repr(C)]
pub struct SharedMessage<T: ?Sized = [u8]> {
    sequence_and_flags: AtomicU64, // SequenceState
    readers_state: AtomicU64, // ReadersState

    // Synchronization (lock-free condition variables)
    writer_condvar: SharedCondvar, // Readers wait here for new data
    reader_done_condvar: SharedCondvar, // Writer waits here for readers to finish

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

    #[inline]
    fn get_sequence_state(&self) -> SequenceState {
        SequenceState::from(self.sequence_and_flags.load(Ordering::Acquire))
    }

    #[inline]
    fn get_reader_state(&self) -> ReadersStateCount {
        ReadersStateCount::from(self.readers_state.load(Ordering::Acquire))
    }

    fn start_write(&self) -> Option<PayloadWriteGuard<'_>> {
        if self.is_stopped() {
            return None;
        }

        let writer_guard = self.writer_mutex.lock();

        // Wait for readers to consume the already existing data (based on policy)
        let current_seq = self.get_sequence_state().sequence;

        let mut reader_state = self.get_reader_state();

        // Only wait if there's a previous message to be consumed (sequence > 0)
        if reader_state.get_target_consumed() > 0 && current_seq > 0 {
            let mut ticket = self.reader_done_condvar.value();

            while reader_state.data_consumed < reader_state.get_target_consumed() {
                if self.is_stopped() {
                    return None;
                }

                // Kick readers that might be sleeping and haven't consumed the data yet.
                // If we don't do this, we might deadlock waiting for a reader that is waiting for us.
                self.writer_condvar.notify_all();
                self.reader_done_condvar.wait(ticket);

                ticket = self.reader_done_condvar.value();
                reader_state = self.get_reader_state();
            }
        }

        // Mark in progress so no one new will try to write
        self.sequence_and_flags
            .fetch_or(SequenceState::WRITING_IN_PROGRESS_MASK, Ordering::Release);

        // Wait for active readers to finish
        let mut ticket = self.reader_done_condvar.value();
        while self.get_reader_state().active_readers > 0 {
            if self.is_stopped() {
                self.sequence_and_flags
                    .fetch_and(!SequenceState::WRITING_IN_PROGRESS_MASK, Ordering::Release);
                return None;
            }

            self.reader_done_condvar.wait(ticket);
            ticket = self.reader_done_condvar.value();
        }

        Some(writer_guard)
    }

    /// Release a reader reference
    pub(crate) fn release_active_reader(&self) {
        self.update_reader_state(|state| {
            state.active_readers -= 1;
            // Increment consumed count to signal that this reader has finished
            state.data_consumed += 1;
        });

        self.reader_done_condvar.notify_all();
    }

    pub(crate) fn capacity(&self) -> usize {
        let data = unsafe { &*(self.data.get()) };
        data.len()
    }

    pub(crate) fn data_ref(&self) -> &[u8] {
        let data = unsafe { &*(self.data.get()) };
        let size = self.data_size.load(Ordering::Acquire);
        &data[..size]
    }

    pub(crate) fn data_mut(&self, _write_guard: &PayloadWriteGuard) -> &mut [u8] {
        // SAFETY: This is safe because:
        // 1. The writer mutex ensures only one writer accesses this at a time
        // 2. The reference counting ensures no readers access this while being written
        unsafe { &mut *self.data.get() }
    }

    pub(crate) fn publish_write(
        &self,
        _write_guard: PayloadWriteGuard<'_>,
        size: usize,
    ) -> Option<u64> {
        // Store the actual size
        self.data_size.store(size, Ordering::Release);
        // Reset consumed count
        self.update_reader_state(|state| state.data_consumed = 0);

        // Atomically update: increment sequence, remove in_writing flag, keep the stopped flag
        let mut old_packed = self.sequence_and_flags.load(Ordering::Acquire);
        loop {
            let old_state = SequenceState::from(old_packed);

            let mut new_seq = (old_state.sequence + 1) & SequenceState::SEQUENCE_MASK;
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
            match self.sequence_and_flags.compare_exchange_weak(
                old_packed,
                new_packed,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.writer_condvar.notify_all();
                    return Some(new_seq);
                }
                Err(actual_value) => old_packed = actual_value,
            }
        }
    }

    fn update_reader_state(&self, mut mutator: impl FnMut(&mut ReadersStateCount)) {
        let mut packed = self.readers_state.load(Ordering::Acquire);
        loop {
            let mut policy = ReadersStateCount::from(packed);
            mutator(&mut policy);
            let new_packed = policy.to_packed();
            if packed == new_packed {
                break; // nothing to do
            }

            match self.readers_state.compare_exchange_weak(
                packed,
                new_packed,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual_value) => packed = actual_value,
            }
        }
    }
}

// Public functions
impl SharedMessage {
    /// Returns the new sequence number if successful, None if stopped.
    pub fn write(&self, new_data: &[u8]) -> Option<u64> {
        let writer_guard = self.start_write()?;

        let data = self.data_mut(&writer_guard);
        if new_data.len() > data.len() {
            panic!(
                "Data size ({} bytes) exceeds capacity ({} bytes)",
                new_data.len(),
                data.len()
            );
        }
        data[..new_data.len()].copy_from_slice(new_data);

        self.publish_write(writer_guard, new_data.len())
    }

    /// Returns a WriteGuard that provides mutable access
    /// The data will be published when the guard's `publish()` method is called.
    /// The writer mutex is acquired here and will be held until the guard is published or dropped.
    pub fn acquire_write_guard(&self) -> Option<MessageWriteGuard<'_>> {
        let write_guard = self.start_write()?;
        Some(MessageWriteGuard::new(self, write_guard))
    }

    /// Returns a ReadGuard if new data is available, None otherwise.
    pub fn read(&self, last_seen_seq: u64, block: bool) -> Option<MessageReadGuard<'_>> {
        loop {
            let ticket = self.writer_condvar.value();
            let state = self.get_sequence_state();

            if state.stopped && state.sequence == last_seen_seq {
                return None;
            }

            // Check if new data available
            if state.sequence == last_seen_seq {
                if !block {
                    return None;
                }
                // Sleep until new data or stop
                self.writer_condvar.wait(ticket);
                continue;
            }

            if state.writing_in_progress {
                // Wait until it's finished writing
                self.writer_condvar.wait(ticket);
                continue;
            }

            // Register as a reader BEFORE accessing the data
            self.update_reader_state(|state| state.active_readers += 1);

            // Verify that the state didn't change while registering
            let verify_state = self.get_sequence_state();
            if verify_state.writing_in_progress || verify_state.sequence != state.sequence {
                // Buffer changed - unregister and retry
                self.update_reader_state(|state| state.active_readers -= 1);
                self.reader_done_condvar.notify_all();
                continue;
            }

            return Some(MessageReadGuard::new(self, state.sequence));
        }
    }

    /// Get the current sequence number.
    pub fn current_sequence(&self) -> u64 {
        self.get_sequence_state().sequence
    }

    /// Check if there's new data compared to the given sequence.
    pub fn has_new_data(&self, last_seen_seq: u64) -> bool {
        self.get_sequence_state().sequence != last_seen_seq
    }

    /// Check if the shared memory has been stopped.
    pub fn is_stopped(&self) -> bool {
        self.get_sequence_state().stopped
    }

    pub fn stop(&self) {
        self.sequence_and_flags
            .fetch_or(SequenceState::STOPPED_MASK, Ordering::AcqRel);

        // Wake all waiting readers and writers
        self.writer_condvar.notify_all();
        self.reader_done_condvar.notify_all();
    }

    /// Set the target read count for writer wait policy.
    pub fn set_target_read_count(&self, count: u16) {
        let _guard = self.writer_mutex.lock(); // Don't update the target while someone is writing
        self.update_reader_state(|policy| policy.target_read = count);
    }

    /// Register a new reader
    pub fn add_reader(&self) {
        self.update_reader_state(|policy| {
            if policy.consumers == u16::MAX {
                panic!("Too many readers!");
            }
            policy.consumers += 1;
        });
    }

    /// Unregister a reader
    pub fn remove_reader(&self) {
        self.update_reader_state(|policy| {
            if policy.consumers == 0 {
                panic!("No readers to remove!");
            }
            policy.consumers -= 1;
        });
    }
}

// SAFETY: SharedMessage is safe to share across threads because:
// 1. All mutable access to data is protected by the writer mutex (serializes writers)
// 2. Reader access is protected by reference counting (ensures no concurrent read/write)
// 3. The UnsafeCell is only used to enable interior mutability for the data,
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
