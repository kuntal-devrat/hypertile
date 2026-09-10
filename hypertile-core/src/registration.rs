//! Dynamic worker registration API.
//!
//! Enables external threads (e.g. FastAPI/uvicorn worker threads, thread pools)
//! to temporarily or permanently join the Hypertile executor pool as either
//! [`WorkerKind::Native`] or [`WorkerKind::Bilingual`].
//!
//! When the returned [`RegisteredWorker`] is dropped, the thread is cleanly
//! deregistered from the pool, its remaining local tasks are flushed to the
//! shared injector, and the thread returns to its caller.

use std::sync::Arc;

use crate::executor::{ExecutorCore, WorkerId, WorkerKind};
use crate::worker::{WorkerGuard, WorkerHandle};

/// A dynamically registered worker instance.
pub struct RegisteredWorker {
    handle: WorkerHandle,
    guard: WorkerGuard,
}

impl RegisteredWorker {
    pub fn worker_id(&self) -> WorkerId {
        self.guard.worker_id()
    }

    pub fn kind(&self) -> WorkerKind {
        self.handle.kind
    }

    /// Execute tasks until the pool is idle.
    pub fn run_until_idle(&self) {
        self.handle.run_until_idle();
    }

    /// Execute a single task. Returns `true` if a task was found and executed.
    pub fn run_one(&self) -> bool {
        self.handle.run_one()
    }

    /// Run the continuous worker loop until the executor core is shut down.
    pub fn run_loop(&self) {
        self.handle.run_loop();
    }
}

/// Dynamically register the current thread as an active worker in the executor pool.
pub fn register_worker(core: &Arc<ExecutorCore>, kind: WorkerKind) -> RegisteredWorker {
    let (handle, guard) = WorkerHandle::new(core.clone(), kind);
    RegisteredWorker { handle, guard }
}
