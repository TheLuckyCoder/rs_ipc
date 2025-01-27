use crate::primitives::mutex::{guard_lock, SharedMutexGuard};
use crate::primitives::shared_futex;
use crate::primitives::shared_futex::SharedFutex;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::Relaxed;

#[derive(Default)]
#[repr(transparent)]
pub struct SharedCondvar(AtomicU32);

impl SharedCondvar {
    pub fn wait<'a, T: ?Sized>(&self, guard: SharedMutexGuard<'a, T>) -> SharedMutexGuard<'a, T> {
        let lock = guard_lock(&guard);
        unsafe {
            self.wait_on_futex(lock);
        }
        guard
    }

    pub fn wait_while<'a, T: ?Sized, F>(
        &self,
        mut guard: SharedMutexGuard<'a, T>,
        mut condition: F,
    ) -> SharedMutexGuard<'a, T>
    where
        F: FnMut(&mut T) -> bool,
    {
        while condition(&mut *guard) {
            guard = self.wait(guard);
        }
        guard
    }

    pub fn notify_one(&self) {
        let _ = shared_futex::futex_wake(&self.0, 1);
    }

    pub fn notify_all(&self) {
        let _ = shared_futex::futex_wake(&self.0, u32::MAX);
    }

    unsafe fn wait_on_futex(&self, mutex: &SharedFutex) -> bool {
        // Examine the notification counter _before_ we unlock the mutex.
        let futex_value = self.0.load(Relaxed);

        // Unlock the mutex before going to sleep.
        mutex.unlock();

        // Wait, but only if there hasn't been any
        // notification since we unlocked the mutex.
        let r = shared_futex::futex_wait(&self.0, futex_value).is_ok();

        // Lock the mutex again.
        mutex.lock();

        r
    }
}
