use crate::sync::futex;
use crate::sync::futex::{Futex, futex_wait};
use std::sync::atomic::Ordering;

#[derive(Default)]
#[repr(transparent)]
pub struct SharedCondvar(Futex);

#[allow(dead_code)]
impl SharedCondvar {
    /// Wait on the condition variable if the value matches the expected value,
    /// Remember, this can wake spontaneously
    pub fn wait(&self) {
        futex_wait(&self.0, self.0.load(Ordering::Acquire));
    }

    pub fn wait_while(&self, mut condition: impl FnMut() -> bool) {
        while condition() {}
    }

    pub fn notify_one(&self) {
        self.0.fetch_add(1, Ordering::Release);
        assert!(futex::futex_wake_one(&self.0));
    }

    pub fn notify_all(&self) {
        self.0.fetch_add(1, Ordering::Release);
        assert!(futex::futex_wake_all(&self.0));
    }
}
