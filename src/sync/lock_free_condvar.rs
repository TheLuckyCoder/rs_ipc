use crate::sync::futex::{futex_wait, futex_wake_all, Futex};
use std::sync::atomic::{AtomicU32, Ordering};

/// Simple condition variable for lock-free synchronization.
/// Unlike SharedCondvar, this doesn't require a mutex.
#[repr(transparent)]
#[derive(Default)]
pub struct LockFreeCondvar(Futex);

impl LockFreeCondvar {
    /// Wait on the condition variable if the value matches the expected value,
    /// Remember, this can wake spontaneously
    #[inline]
    pub fn wait(&self) {
        futex_wait(&self.0, self.0.load(Ordering::Relaxed));
    }
    
    /// Notify all threads waiting on this condition variable
    #[inline]
    pub fn notify_all(&self) {
        self.0.fetch_add(1, Ordering::Release);
        futex_wake_all(&self.0);
    }
}
