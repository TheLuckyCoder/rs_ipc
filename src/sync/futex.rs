use rustix::thread::futex;
use std::sync::atomic::AtomicU32;

pub type Futex = AtomicU32;
pub type Primitive = u32;

#[inline]
pub fn futex_wait(futex: &Futex, state: Primitive) -> bool {
    futex::wait(futex, futex::Flags::empty(), state, None).is_ok()
}

#[inline]
pub fn futex_wake_one(futex: &Futex) -> bool {
    futex::wake(futex, futex::Flags::empty(), 1).is_ok()
}

#[inline]
pub fn futex_wake_all(futex: &Futex) -> bool {
    futex::wake(futex, futex::Flags::empty(), i32::MAX as u32).is_ok()
}
