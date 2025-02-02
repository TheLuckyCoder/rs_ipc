pub mod condvar;
mod futex;
mod lock;

pub use lock::mutex::{SharedMutex, SharedMutexGuard};

type PhantomDataUnSend = std::marker::PhantomData<*const ()>;
