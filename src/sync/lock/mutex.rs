use crate::sync::lock::futex_lock::FutexLock;
use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};

#[derive(Default)]
pub struct SharedMutex<T: ?Sized> {
    pub(crate) futex: FutexLock,
    data: UnsafeCell<T>,
}

impl<T: ?Sized> SharedMutex<T> {
    pub fn lock(&self) -> SharedMutexGuard<'_, T> {
        self.futex.lock();
        SharedMutexGuard { lock: self }
    }

    #[allow(dead_code)]
    pub fn try_lock(&self) -> Option<SharedMutexGuard<'_, T>> {
        if self.futex.try_lock() {
            Some(SharedMutexGuard { lock: self })
        } else {
            None
        }
    }
}

unsafe impl<T: ?Sized + Send> Send for SharedMutex<T> {}
unsafe impl<T: ?Sized + Send> Sync for SharedMutex<T> {}

#[must_use = "if unused the SharedMutex will immediately unlock"]
pub struct SharedMutexGuard<'a, T: ?Sized + 'a> {
    lock: &'a SharedMutex<T>,
}

impl<T: ?Sized> Deref for SharedMutexGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.data.get() }
    }
}

impl<T: ?Sized> DerefMut for SharedMutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.lock.data.get() }
    }
}

impl<T: ?Sized> Drop for SharedMutexGuard<'_, T> {
    #[inline]
    fn drop(&mut self) {
        unsafe { self.lock.futex.unlock() }
    }
}

// Safe because:
// 1. Our futex-based lock doesn't require same-thread unlock
// 2. T: Send ensures the data can be accessed from another thread
unsafe impl<T: ?Sized + Send> Send for SharedMutexGuard<'_, T> {}
unsafe impl<T: ?Sized + Sync> Sync for SharedMutexGuard<'_, T> {}

