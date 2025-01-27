use std::hint::spin_loop;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use rustix::thread::futex;

const UNLOCKED: u32 = 0;
const LOCKED: u32 = 1; // locked, no other threads waiting
const CONTENDED: u32 = 2; // locked, and other threads waiting (contended)

#[inline]
pub fn futex_wait(futex: &AtomicU32, state: u32) -> rustix::io::Result<()> {
    futex::wait(futex, futex::Flags::empty(), state, None)
}

#[inline]
pub fn futex_wake(futex: &AtomicU32, count: u32) -> rustix::io::Result<usize> {
    futex::wake(futex, futex::Flags::empty(), count)
}

#[derive(Default)]
#[repr(transparent)]
pub struct SharedFutex(AtomicU32);

/// This code is largely taken from std::sync::Mutex
impl SharedFutex {
    #[inline]
    pub fn try_lock(&self) -> bool {
        self.0
            .compare_exchange(UNLOCKED, LOCKED, Acquire, Relaxed)
            .is_ok()
    }

    #[inline]
    pub fn lock(&self) {
        if self
            .0
            .compare_exchange(UNLOCKED, LOCKED, Acquire, Relaxed)
            .is_err()
        {
            self.lock_contended();
        }
    }

    #[cold]
    fn lock_contended(&self) {
        let mut state = self.spin();

        // If it's unlocked now, attempt to take the lock
        // without marking it as contended.
        if state == UNLOCKED {
            match self
                .0
                .compare_exchange(UNLOCKED, LOCKED, Acquire, Relaxed)
            {
                Ok(_) => return, // Locked!
                Err(s) => state = s,
            }
        }

        loop {
            // Put the lock in contended state.
            // We avoid an unnecessary write if it as already set to CONTENDED,
            // to be friendlier for the caches.
            if state != CONTENDED && self.0.swap(CONTENDED, Acquire) == UNLOCKED {
                // We changed it from UNLOCKED to CONTENDED, so we just successfully locked it.
                return;
            }

            // Wait for the futex to change state, assuming it is still CONTENDED.
            let _ = futex_wait(&self.0, CONTENDED);

            // Get the new state
            state = self.spin();
        }
    }

    #[inline]
    pub unsafe fn unlock(&self) {
        if self.0.swap(UNLOCKED, Release) == CONTENDED {
            // We only wake up one thread. When that thread locks the mutex, it
            // will mark the mutex as CONTENDED (see lock_contended above),
            // which makes sure that any other waiting threads will also be
            // woken up eventually.
            let _ = futex_wake(&self.0, 1);
        }
    }

    fn spin(&self) -> u32 {
        let mut spin = 100;
        loop {
            // We only use `load` (and not `swap` or `compare_exchange`)
            // while spinning, to be easier on the caches.
            let state = self.0.load(Relaxed);

            // We stop spinning when the mutex is UNLOCKED,
            // but also when it's CONTENDED.
            if state != LOCKED || spin == UNLOCKED {
                return state;
            }

            spin_loop();
            spin -= 1;
        }
    }
    
}
