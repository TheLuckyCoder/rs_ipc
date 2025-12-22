pub mod condvar;
pub mod futex;
mod lock;
pub mod lock_free_condvar;

pub use lock::futex_lock::FutexLock;
pub use lock::mutex::{SharedMutex, SharedMutexGuard};
pub use lock_free_condvar::LockFreeCondvar;

type PhantomDataUnSend = std::marker::PhantomData<*const ()>;
