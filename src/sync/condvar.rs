use crate::sync::futex;
use crate::sync::futex::{Futex, futex_wait};
use std::sync::atomic::Ordering;

#[derive(Default)]
#[repr(transparent)]
pub struct SharedCondvar(Futex);

#[allow(dead_code)]
impl SharedCondvar {
    #[inline(always)]
    pub fn value(&self) -> u32 {
        self.0.load(Ordering::Acquire)
    }

    /// Wait on the condition variable if the value matches the expected value,
    /// Remember, this can wake spontaneously
    #[inline]
    pub fn wait(&self, expected: u32) {
        for _ in 0..1000 {
            if self.0.load(Ordering::Relaxed) != expected {
                return;
            }
            std::hint::spin_loop();
        }
        futex_wait(&self.0, expected);
    }

    #[inline]
    pub fn notify_one(&self) {
        self.0.fetch_add(1, Ordering::Release);
        assert!(futex::futex_wake_one(&self.0));
    }

    #[inline]
    pub fn notify_all(&self) {
        self.0.fetch_add(1, Ordering::Release);
        assert!(futex::futex_wake_all(&self.0));
    }
}
