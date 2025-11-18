use crate::memory_mapper::{SharedMemoryMapper, SlicePtrCast};
use crate::sync::condvar::SharedCondvar;
use crate::sync::SharedMutex;
use std::ffi::c_void;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};

const STOPPED_BIT_MASK: usize = 1usize << (usize::BITS - 1);
const VERSION_MASK: usize = !STOPPED_BIT_MASK;

#[repr(C)]
pub struct SharedMessage<T: ?Sized = [u8]> {
    stopped_and_version: AtomicUsize,
    write_condvar: SharedCondvar,
    read_condvar: SharedCondvar,
    data: SharedMutex<SharedMessageData<T>>,
}

#[repr(C)]
struct SharedMessageData<T: ?Sized = [u8]> {
    /// Number of readers that have consumed the current payload.
    /// Reset to 0 whenever a new payload is written.
    read_count: u16,
    /// Maximum number of readers the writer will wait for before
    /// being allowed to write a new payload.
    target_read_count: u16,
    /// Number of currently registered reader instances.
    consumer_count: u16,
    size: usize,
    payload: T,
}

impl SharedMessageData {
    fn payload_data(&self) -> &[u8] {
        &self.payload[..self.size]
    }

    fn is_reading_done(&self) -> bool {
        self.read_count >= self.target_read_count.min(self.consumer_count)
    }

    fn increment_read_count(&mut self) -> bool {
        self.read_count += 1;
        self.is_reading_done()
    }

    fn copy(&mut self, data: &[u8]) {
        let data_len = data.len();

        self.read_count = 0;
        self.size = data_len;
        self.payload[..data_len].copy_from_slice(data);
    }
}

#[derive(Default)]
struct StoppedAndVersion {
    stopped: bool,
    version: usize,
}

impl SharedMessage {
    pub(crate) const fn size_of_fields() -> usize {
        size_of::<SharedMessage<SharedMessageData<()>>>()
    }

    pub fn write(&self, data: &[u8]) -> Option<usize> {
        if self.is_stopped() {
            return None;
        }

        let mut data_guard = self.data.lock();
        if self.is_stopped() {
            return None;
        }

        let new_version = unsafe { self.increment_version(&mut data_guard) };
        data_guard.copy(data);
        self.write_condvar.notify_all();

        Some(new_version)
    }

    pub fn write_waiting(&self, data: &[u8]) -> Option<usize> {
        if self.is_stopped() {
            return None;
        }

        let mut data_guard = self.data.lock();

        let mut status = StoppedAndVersion::default();
        data_guard = self.read_condvar.wait_while(data_guard, |guard| {
            status = self.get_version();
            !status.stopped && status.version != 0 && !guard.is_reading_done()
        });

        if status.stopped {
            return None;
        }

        let new_version = unsafe { self.increment_version(&mut data_guard) };
        data_guard.copy(data);
        self.write_condvar.notify_all();

        Some(new_version)
    }

    pub fn try_read(&self, current_version: usize, read: impl FnOnce(usize, &[u8])) {
        // Read the version to check if there is a new one
        if current_version == self.get_version().version {
            return;
        }

        let mut data_guard = self.data.lock();
        // Read the version again after the lock has been acquired, as it could have changed
        let version = self.get_version().version;

        read(version, data_guard.payload_data());

        data_guard.increment_read_count();
        if data_guard.is_reading_done() {
            self.read_condvar.notify_one();
        }
    }

    pub fn blocking_read(&self, current_version: usize, read: impl FnOnce(usize, &[u8])) {
        let mut data_guard = self.data.lock();

        let mut status = StoppedAndVersion::default();
        data_guard = self.write_condvar.wait_while(data_guard, |_| {
            status = self.get_version();
            !status.stopped && status.version == current_version
        });
        if status.version == current_version {
            return;
        }

        read(status.version, data_guard.payload_data());

        data_guard.increment_read_count();
        if data_guard.is_reading_done() {
            self.read_condvar.notify_one();
        }
    }

    #[inline]
    pub fn is_new_version_available(&self, current_version: usize) -> bool {
        self.get_version().version != current_version
    }

    pub fn set_target_read_count(&self, target_read_count: u16) {
        let mut data_guard = self.data.lock();
        data_guard.target_read_count = target_read_count;
    }

    pub fn get_target_read_count(&self) -> u16 {
        let data_guard = self.data.lock();
        data_guard.target_read_count
    }

    pub fn add_reader(&self) {
        let mut data_guard = self.data.lock();
        data_guard.consumer_count = data_guard.consumer_count.saturating_add(1);
        self.read_condvar.notify_all();
    }

    pub fn remove_reader(&self) {
        let mut data_guard = self.data.lock();
        data_guard.consumer_count = data_guard.consumer_count.saturating_sub(1);
        self.read_condvar.notify_all();
    }

    #[inline]
    pub fn is_stopped(&self) -> bool {
        self.get_version().stopped
    }

    pub fn stop(&self) {
        let _data_guard = self.data.lock();
        self.stopped_and_version
            .fetch_or(STOPPED_BIT_MASK, Ordering::Relaxed);

        self.write_condvar.notify_all();
        self.read_condvar.notify_all();
    }

    fn get_version(&self) -> StoppedAndVersion {
        let version = self.stopped_and_version.load(Ordering::Relaxed);
        let stopped = (version & STOPPED_BIT_MASK) != 0;
        let version = version & !STOPPED_BIT_MASK;
        StoppedAndVersion { stopped, version }
    }

    /// This function must only be called when the mutex is locked and if the message is not stopped.
    /// A mutable reference is required to ensure that an exclusive lock is held while calling this function
    unsafe fn increment_version(&self, _data: &mut SharedMessageData) -> usize {
        debug_assert!(
            !self.is_stopped(),
            "increment_version must not be called on a stopped message"
        );

        let old = self.stopped_and_version.load(Ordering::Relaxed);
        let mut new = (old + 1) & VERSION_MASK;
        if new == 0 {
            new += 1; // if it wraps around, increment to 1, as version 0 has special meaning
        }
        self.stopped_and_version.store(new, Ordering::Relaxed);
        new
    }
}

unsafe impl SlicePtrCast for SharedMessage {
    unsafe fn cast_from_void_ptr(
        ptr: NonNull<c_void>,
        memory_size: usize,
    ) -> Option<NonNull<Self>> {
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
