use rustix::fs::Mode;
use rustix::mm;
use rustix::mm::{MapFlags, ProtFlags};
use rustix::shm::OFlags;
use std::ffi::{c_void, CString};
use std::ops::Deref;
use std::os::fd::OwnedFd;
use std::ptr::NonNull;

pub unsafe trait SlicePtrCast {
    /// # Safety
    /// - `ptr` and `memory_size` must refer to a mapping that is valid for Self's layout.
    /// - Implementation must ensure a returned pointer is valid for reads/writes.
    unsafe fn cast_from_void_ptr(ptr: NonNull<c_void>, memory_size: usize)
        -> Option<NonNull<Self>>;
}

pub struct SharedMemoryMapper<T: 'static + ?Sized> {
    name: CString,
    _fd: OwnedFd,
    mapped_ptr: *const T,
    mapped_size: usize,
    created: bool,
}

impl<T: ?Sized + SlicePtrCast> SharedMemoryMapper<T> {
    pub fn create(name: CString, size: usize) -> std::io::Result<Self> {
        // Open shared memory
        let shm = rustix::shm::open(
            name.as_c_str(),
            OFlags::CREATE | OFlags::RDWR | OFlags::TRUNC,
            Mode::all(),
        )?;

        // Resize shared memory
        if let Err(e) = rustix::fs::ftruncate(&shm, size as u64) {
            let _ = rustix::shm::unlink(name.as_c_str());
            return Err(e.into());
        }

        match unsafe { Self::map_and_init(&shm, true) } {
            Ok((mapped_struct, mapped_size)) => Ok(Self {
                name,
                _fd: shm,
                mapped_ptr: mapped_struct,
                mapped_size,
                created: true,
            }),
            Err(e) => {
                let _ = rustix::shm::unlink(name);
                Err(e.into())
            }
        }
    }

    pub fn open(name: CString) -> std::io::Result<Self> {
        // Open shared memory
        let shm = rustix::shm::open(&name, OFlags::RDWR, Mode::all())?;
        let (mapped_ptr, mapped_size) = unsafe { Self::map_and_init(&shm, false)? };

        Ok(Self {
            name,
            mapped_ptr,
            mapped_size,
            _fd: shm,
            created: false,
        })
    }

    unsafe fn map_and_init(shm: &OwnedFd, create: bool) -> rustix::io::Result<(*const T, usize)> {
        // Read actual size
        let stats = rustix::fs::fstat(shm)?;
        let size = stats.st_size as usize;

        // Map shared memory
        let void_ptr = unsafe {
            mm::mmap(
                std::ptr::null_mut(),
                size,
                ProtFlags::READ | ProtFlags::WRITE,
                MapFlags::SHARED_VALIDATE,
                shm,
                0,
            )?
        };
        let void_ptr = NonNull::new(void_ptr).ok_or(rustix::io::Errno::INVAL)?;

        if let Err(e) = unsafe { mm::madvise(void_ptr.as_ptr(), size, mm::Advice::LinuxHugepage) } {
            eprintln!("Failed to set huge pages advice: {e}");
        }

        if create {
            unsafe {
                std::ptr::write_bytes(void_ptr.as_ptr(), 0, size);
            }
        }

        let ptr =
            unsafe { T::cast_from_void_ptr(void_ptr, size) }.ok_or(rustix::io::Errno::INVAL)?;

        Ok((ptr.as_ptr(), size))
    }

    pub fn mapped_memory_size(&self) -> usize {
        self.mapped_size
    }
}

impl<T: ?Sized> Deref for SharedMemoryMapper<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.mapped_ptr }
    }
}

impl<T: ?Sized> Drop for SharedMemoryMapper<T> {
    fn drop(&mut self) {
        let ptr = self.mapped_ptr as *mut c_void;
        if let Err(e) = unsafe { mm::munmap(ptr, self.mapped_size) } {
            eprintln!("Failed to unmap shared memory: {}", e);
        }

        if self.created {
            if let Err(e) = rustix::shm::unlink(&self.name) {
                eprintln!("Failed to unlink shared memory: {}", e);
            }
        }
    }
}

unsafe impl<T: ?Sized + Send> Send for SharedMemoryMapper<T> {}
unsafe impl<T: ?Sized + Sync> Sync for SharedMemoryMapper<T> {}
