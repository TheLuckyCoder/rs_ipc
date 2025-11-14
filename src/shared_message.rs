use crate::memory_mapper::SlicePtrCast;
use crate::sync::condvar::SharedCondvar;
use crate::sync::SharedMutex;
use std::ffi::c_void;
use std::sync::atomic::{AtomicU16, AtomicUsize, Ordering};

const STOPPED_BIT: usize = 1 << 63;

#[repr(C)]
pub struct SharedMessage<T: ?Sized = SharedMessageData> {
    stopped_and_version: AtomicUsize,
    write_condvar: SharedCondvar,
    read_condvar: SharedCondvar,
    data: SharedMutex<T>,
}

#[repr(C)]
pub struct SharedMessageData {
    read_count: u16,
    target_read_count: u16,
    consumer_count: u16,
    size: usize,
    payload: [u8],
}

impl SharedMessageData {
    #[inline]
    fn copy(&mut self, data: &[u8]) {
        let data_len = data.len();

        self.read_count = 0;
        self.size = data_len;
        self.payload[..data_len].copy_from_slice(data);
    }

    #[inline]
    fn increment_read_count(&mut self) -> bool {
        self.read_count += 1;
        self.read_count >= self.target_read_count.min(self.consumer_count)
    }
}

#[derive(Default)]
pub(crate) struct StoppedAndVersion {
    pub(crate) stopped: bool,
    pub(crate) version: usize,
}

impl SharedMessage {
    pub(crate) const fn size_of_fields() -> usize {
        #[repr(C)]
        struct SharedMemoryDataSized {
            read_count: AtomicU16,
            target_read_count: u16,
            consumer_count: u16,
            size: usize,
        }
        size_of::<SharedMessage<SharedMemoryDataSized>>()
    }

    pub fn write(&self, data: &[u8]) -> Option<usize> {
        if self.get_version().stopped {
            return None;
        }

        let mut content = self.data.lock();

        let new_version = unsafe { self.increment_version() };
        content.copy(data);
        self.write_condvar.notify_all();

        Some(new_version)
    }

    pub fn write_waiting(&self, data: &[u8]) -> Option<usize> {
        let mut content = self.data.lock();

        let mut status = StoppedAndVersion::default();
        content = self.read_condvar.wait_while(content, |lock| {
            status = self.get_version();
            !status.stopped
                && status.version != 0
                && lock.read_count < lock.target_read_count.min(lock.consumer_count)
        });

        if status.stopped {
            return None;
        }

        let new_version = unsafe { self.increment_version() };
        content.copy(data);
        self.write_condvar.notify_all();

        Some(new_version)
    }

    pub fn try_read(&self, current_version: usize, mut read: impl FnMut(usize, &[u8])) {
        // Read the version to check if there is a new one
        if current_version == self.get_version().version {
            return;
        }

        let mut lock = self.data.lock();
        // Read the version again after the lock has been acquired, as it could have changed
        let version = self.get_version().version;

        read(version, &lock.payload[..lock.size]);

        if lock.increment_read_count() {
            self.read_condvar.notify_one();
        }
    }

    pub fn blocking_read(&self, current_version: usize, mut read: impl FnMut(usize, &[u8])) {
        let mut lock = self.data.lock();

        let mut status = StoppedAndVersion::default();
        lock = self.write_condvar.wait_while(lock, |_| {
            status = self.get_version();
            !status.stopped && status.version == current_version
        });
        if status.version == current_version {
            return;
        }

        read(status.version, &lock.payload[..lock.size]);

        if lock.increment_read_count() {
            self.read_condvar.notify_one();
        }
    }

    pub fn is_new_version_available(&self, current_version: usize) -> bool {
        self.get_version().version != current_version
    }

    pub fn set_target_read_count(&self, target_read_count: u16) {
        let mut content = self.data.lock();
        content.target_read_count = target_read_count;
    }

    pub fn get_target_read_count(&self) -> u16 {
        let content = self.data.lock();
        content.target_read_count
    }

    pub fn add_reader(&self) {
        let mut content = self.data.lock();
        content.consumer_count += 1;
        self.read_condvar.notify_all();
    }

    pub fn remove_reader(&self) {
        let mut content = self.data.lock();
        content.consumer_count -= 1;
        self.read_condvar.notify_all();
    }

    pub fn is_stopped(&self) -> bool {
        self.get_version().stopped
    }

    pub fn stop(&self) {
        let _ = self.data.lock();
        self.stopped_and_version
            .fetch_or(STOPPED_BIT, Ordering::Relaxed);

        self.write_condvar.notify_all();
        self.read_condvar.notify_all();
    }

    #[inline]
    fn get_version(&self) -> StoppedAndVersion {
        let version = self.stopped_and_version.load(Ordering::Relaxed);
        let stopped = (version & STOPPED_BIT) != 0;
        let version = version & !STOPPED_BIT;
        StoppedAndVersion { stopped, version }
    }

    /// This function must only be called when the mutex is locked and if the message not is stopped
    unsafe fn increment_version(&self) -> usize {
        let old_version = self.stopped_and_version.fetch_add(1, Ordering::Relaxed);
        let new_version = old_version + 1;
        if (old_version & STOPPED_BIT) != (new_version & STOPPED_BIT) {
            // The value has overflowed, reset it back to 0
            self.stopped_and_version.store(0, Ordering::Relaxed);
            0
        } else {
            new_version
        }
    }
}

impl SlicePtrCast for SharedMessage {
    unsafe fn cast_from_void_ptr(ptr: *mut c_void, memory_size: usize) -> *const Self {
        let payload_size = memory_size - Self::size_of_fields();
        let slice_ptr: *mut [u8] = std::ptr::slice_from_raw_parts_mut(ptr.cast(), payload_size);
        slice_ptr as *const Self
    }
}
