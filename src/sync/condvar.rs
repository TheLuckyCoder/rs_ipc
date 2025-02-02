use crate::sync::futex::Futex;
use crate::sync::lock::futex_lock::FutexLock;
use crate::sync::lock::mutex::guard_lock;
use crate::sync::{futex, SharedMutexGuard};
use std::sync::atomic::Ordering::Relaxed;

#[derive(Default)]
#[repr(transparent)]
pub struct SharedCondvar(Futex);

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

    // All the memory orderings here are `Relaxed`,
    // because synchronization is done by unlocking and locking the mutex.

    #[allow(dead_code)]
    pub fn notify_one(&self) {
        self.0.fetch_add(1, Relaxed);
        assert!(futex::futex_wake_one(&self.0));
    }

    pub fn notify_all(&self) {
        self.0.fetch_add(1, Relaxed);
        assert!(futex::futex_wake_all(&self.0));
    }

    unsafe fn wait_on_futex(&self, mutex: &FutexLock) -> bool {
        // Examine the notification counter _before_ we unlock the mutex.
        let futex_value = self.0.load(Relaxed);

        // Unlock the mutex before going to sleep.
        mutex.unlock();

        // Wait, but only if there hasn't been any
        // notification since we unlocked the mutex.
        let r = futex::futex_wait(&self.0, futex_value);

        // Lock the mutex again.
        mutex.lock();

        r
    }
}
