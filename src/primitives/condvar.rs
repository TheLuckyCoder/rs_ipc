use crate::primitives::mutex::{guard_lock, SharedMutexGuard};
use crate::primitives::shared_futex::SharedFutex;
use linux_futex::{Futex, Shared};
use std::sync::atomic::Ordering::Relaxed;

#[derive(Default)]
#[repr(transparent)]
pub struct SharedCondvar {
    futex: Futex<Shared>,
}

impl SharedCondvar {
    pub fn wait<'a, T: ?Sized>(&self, guard: SharedMutexGuard<'a, T>) -> SharedMutexGuard<'a, T> {
        let lock = guard_lock(&guard);
        unsafe {
            self.futex_wait(lock);
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
        self.futex.wake(1);
    }

    pub fn notify_all(&self) {
        self.futex.wake(i32::MAX);
    }

    unsafe fn futex_wait(&self, mutex: &SharedFutex) -> bool {
        // Examine the notification counter _before_ we unlock the mutex.
        let futex_value = self.futex.value.load(Relaxed);

        // Unlock the mutex before going to sleep.
        mutex.unlock();

        // Wait, but only if there hasn't been any
        // notification since we unlocked the mutex.
        let r = self.futex.wait(futex_value).is_ok();

        // Lock the mutex again.
        mutex.lock();

        r
    }
}
