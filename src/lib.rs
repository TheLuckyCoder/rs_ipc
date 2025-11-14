#![cfg_attr(feature = "nightly-features", feature(test))]

#[cfg(feature = "python")]
mod python;
pub mod shared_message;
mod sync;
pub mod memory_mapper;
