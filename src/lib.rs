// Mostly for criterion benchmarks

pub mod http;
pub mod replica_selector;

#[allow(dead_code)]
mod util;

pub use replica_selector::{Replica, ReplicaSelector};
