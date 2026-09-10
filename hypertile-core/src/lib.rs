//! Hypertile — shared work-stealing executor for Rust and Python (PRD v2).
//!
//! A shared, multi-threaded executor whose workers are individually either
//! *native-only* or *bilingual* (interpreter-attached).
//!
//! Key Architecture:
//! - Chase-Lev deques ([`crossbeam_deque`]) with global injector and randomized peer stealing.
//! - Single-hop continuation handoff: task completion on worker W immediately enqueues
//!   the continuation into W's local deque.
//! - Dynamic worker registration: foreign threads (e.g. FastAPI workers) can join and leave the pool.
//! - Panic containment: panics in spawned tasks are caught via `catch_unwind` and routed
//!   to [`JoinHandle`] as [`JoinError::Panicked`].
//! - Self-contained timer wheel for asynchronous sleeps without Tokio.

pub mod executor;
pub mod registration;
pub mod runtime;
pub mod slab;
pub mod task;
pub mod timer;
pub mod waker;
pub mod worker;

pub use executor::{ExecutorCore, WorkerEntry, WorkerId, WorkerKind, WorkerRegistry};
pub use registration::{register_worker, RegisteredWorker};
pub use runtime::{block_on, global_runtime, shutdown, spawn, spawn_local, Runtime};
pub use slab::ObjectPool;
pub use task::{JoinError, JoinHandle, RawTask, Runnable, TaskCell, TaskHandle, TaskKind, TaskScheduler};
pub use timer::{sleep, Sleep};
pub use worker::{start_workers, WorkerGuard, WorkerHandle};

/// Create a new shared executor pool with `n_workers` background native threads.
pub fn new_pool(n_workers: usize) -> std::sync::Arc<Runtime> {
    Runtime::new(n_workers)
}
