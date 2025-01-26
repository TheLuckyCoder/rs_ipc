use crate::primitives::condvar::SharedCondvar;
use crate::primitives::memory_mapper::SlicePtrCast;
use crate::primitives::mutex::SharedMutex;
use std::ffi::c_void;
use std::sync::atomic::{AtomicUsize, Ordering};

#[repr(C)]
pub struct SharedMessage<T: ?Sized = SharedMessageData> {
    closed_and_version: AtomicUsize,
    write_condvar: SharedCondvar,
    read_condvar: SharedCondvar,
    data: SharedMutex<T>,
}

#[repr(C)]
pub struct SharedMessageData {
    consumer_count: u32,
    read_count: u32,
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
}

pub(crate) struct ClosedAndVersion {
    pub(crate) closed: bool,
    pub(crate) version: usize,
}

impl SharedMessage {
    const CLOSED_BIT: usize = 1 << 63;

    pub(crate) const fn size_of_fields() -> usize {
        #[repr(C)]
        struct SharedMemoryDataSized {
            consumer_count: u32,
            read_count: u32,
            size: usize,
        }
        size_of::<SharedMessage<SharedMemoryDataSized>>()
    }

    pub(crate) fn write(&self, data: &[u8]) -> usize {
        let mut content = self.data.lock();

        let new_version = self.increment_version();
        content.copy(data);
        self.write_condvar.notify_all();

        new_version
    }

    pub(crate) fn write_waiting_for_readers(&self, data: &[u8], wait_for: u32) -> usize {
        let mut content = self.data.lock();

        if self.get_version().version != 0 {
            content = self.read_condvar.wait_while(content, |lock| {
                lock.read_count < wait_for.min(lock.consumer_count)
            });
        }

        let new_version = self.increment_version();
        content.copy(data);
        self.write_condvar.notify_all();

        new_version
    }

    pub(crate) fn try_read(&self, current_version: usize, mut read: impl FnMut(usize, &[u8])) {
        let ClosedAndVersion { closed, version } = self.get_version();
        if closed {
            return;
        }

        // Read the version to check if there is a new one
        if current_version == version {
            return;
        }

        let mut data = self.data.lock();
        // Read the version again after the lock has been acquired
        let new_version = self.get_version().version;

        read(new_version, &data.payload[..data.size]);
        data.read_count += 1;
        self.read_condvar.notify_one();
    }

    pub(crate) fn blocking_read(&self, current_version: usize, mut read: impl FnMut(usize, &[u8])) {
        let mut data = self.data.lock();
        loop {
            let ClosedAndVersion { closed, version } = self.get_version();
            if closed {
                return;
            }

            if version != current_version {
                read(version, &data.payload[..data.size]);
                data.read_count += 1;
                self.read_condvar.notify_one();
                return;
            }

            // Wait for new version
            data = self.write_condvar.wait(data);
        }
    }

    pub(crate) fn is_new_version_available(&self, current_version: usize) -> bool {
        self.get_version().version != current_version
    }

    pub(crate) fn add_reader(&self) {
        let mut content = self.data.lock();
        content.consumer_count += 1;
    }

    pub(crate) fn remove_reader(&self) {
        let mut content = self.data.lock();
        content.consumer_count -= 1;
    }

    #[inline]
    pub(crate) fn get_version(&self) -> ClosedAndVersion {
        let version = self.closed_and_version.load(Ordering::Relaxed);
        let closed = (version & Self::CLOSED_BIT) != 0;
        let version = version & !Self::CLOSED_BIT;
        ClosedAndVersion { closed, version }
    }

    pub(crate) fn close(&self) {
        let _ = self.data.lock();
        self.closed_and_version
            .fetch_or(Self::CLOSED_BIT, Ordering::Relaxed);
        self.write_condvar.notify_all();
    }

    /// This function must only be called when the mutex is locked and if the message not is closed
    fn increment_version(&self) -> usize {
        let old_version = self.closed_and_version.fetch_add(1, Ordering::Relaxed);
        let new_version = old_version + 1;
        if (old_version & Self::CLOSED_BIT) != (new_version & Self::CLOSED_BIT) {
            // The value has overflowed, reset it back to 0
            self.closed_and_version.store(0, Ordering::Relaxed);
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
