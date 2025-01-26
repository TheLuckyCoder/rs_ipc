use rustix::fs::Mode;
use rustix::mm::{MapFlags, ProtFlags};
use rustix::shm::ShmOFlags;
use std::ffi::{c_void, CString};
use std::ops::Deref;
use std::os::fd::OwnedFd;
use std::ptr::slice_from_raw_parts_mut;

pub trait SlicePtrCast {
    unsafe fn cast_from_void_ptr(ptr: *mut c_void, memory_size: usize) -> *const Self;
}

pub struct SharedMemoryMapper<T: 'static + ?Sized> {
    name: CString,
    _fd: OwnedFd,
    mapped_struct: &'static T,
    mapped_size: usize,
    created: bool,
}

impl<T: ?Sized + SlicePtrCast> SharedMemoryMapper<T> {
    pub unsafe fn create(name: CString, size: usize) -> std::io::Result<Self> {
        // Open shared memory
        let shm = rustix::shm::shm_open(
            name.as_c_str(),
            ShmOFlags::CREATE | ShmOFlags::RDWR | ShmOFlags::TRUNC,
            Mode::all(),
        )?;

        // Resize shared memory
        if let Err(e) = rustix::fs::ftruncate(&shm, size as u64) {
            let _ = rustix::shm::shm_unlink(name.as_c_str());
            return Err(e.into());
        }

        match unsafe { Self::map_memory(&shm, true) } {
            Ok((mapped_struct, mapped_size)) => Ok(Self {
                name,
                _fd: shm,
                mapped_struct,
                mapped_size,
                created: true,
            }),
            Err(e) => {
                let _ = rustix::shm::shm_unlink(name);
                Err(e.into())
            }
        }
    }

    pub unsafe fn open(name: CString) -> std::io::Result<Self> {
        // Open shared memory
        let shm = rustix::shm::shm_open(&name, ShmOFlags::RDWR, Mode::all())?;
        let (mapped_struct, mapped_size) = unsafe { Self::map_memory(&shm, false)? };

        Ok(Self {
            name,
            mapped_struct,
            mapped_size,
            _fd: shm,
            created: false,
        })
    }

    unsafe fn map_memory(shm: &OwnedFd, create: bool) -> rustix::io::Result<(&'static T, usize)> {
        // Read actual size
        let stats = rustix::fs::fstat(shm)?;
        let size = stats.st_size as usize;

        // Map shared memory
        let void_ptr = unsafe {
            rustix::mm::mmap(
                std::ptr::null_mut(),
                size,
                ProtFlags::READ | ProtFlags::WRITE,
                MapFlags::SHARED_VALIDATE | MapFlags::POPULATE,
                shm,
                0,
            )?
        };

        if create {
            let slice_ptr: *mut [u8] = slice_from_raw_parts_mut(void_ptr.cast(), size);
            unsafe {
                (*slice_ptr).fill(0);
            }
        }
        let ptr = unsafe { T::cast_from_void_ptr(void_ptr, size) };

        Ok((unsafe { &*ptr }, size))
    }

    pub fn mapped_memory_size(&self) -> usize {
        self.mapped_size
    }
}

impl<T: ?Sized> Deref for SharedMemoryMapper<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.mapped_struct
    }
}

impl<T: ?Sized> Drop for SharedMemoryMapper<T> {
    fn drop(&mut self) {
        let ptr = self.mapped_struct as *const T as *mut c_void;
        if let Err(e) = unsafe { rustix::mm::munmap(ptr, self.mapped_size) } {
            eprintln!("Failed to unmap shared memory: {}", e);
        }

        if self.created {
            if let Err(e) = rustix::shm::shm_unlink(&self.name) {
                eprintln!("Failed to unlink shared memory: {}", e);
            }
        }
    }
}
