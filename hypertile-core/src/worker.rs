//! Worker loop, work-stealing logic, and worker lifecycle.
//!
//! Implements the Chase-Lev deque stealing discipline and starvation prevention:
//! 1. Check local deque (single-hop handoff).
//! 2. Check global injector.
//! 3. Steal from peer workers using randomized victim selection.
//! 4. Park with backoff and double-checking when completely idle.

use crossbeam_deque::{Steal, Worker};
use crossbeam_utils::sync::Parker;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::executor::{ExecutorCore, WorkerEntry, WorkerId, WorkerKind};
use crate::task::{TaskHandle, TaskKind};
use crate::waker::{
    local_pop, set_local_worker, steal_injector_into_local, steal_into_local,
    take_any_local_worker, take_local_worker,
};

thread_local! {
    static RNG_STATE: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Ultra-fast non-cryptographic PRNG (SplitMix64).
/// Requires 0 locks and executes in 2-3 single-cycle ALU instructions.
#[inline(always)]
fn fast_rand(worker_id: usize) -> usize {
    RNG_STATE.with(|state| {
        let mut s = state.get();
        if s == 0 {
            s = (worker_id as u64)
                .wrapping_add(0x9e3779b97f4a7c15)
                .wrapping_mul(0xbf58476d1ce4e5b9);
            if s == 0 {
                s = 0x853c49e65d6f8304;
            }
        }
        s = s.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = s;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        state.set(s);
        (z ^ (z >> 31)) as usize
    })
}

/// Active execution context for a worker.
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

pub struct WorkerHandle {
    pub id: WorkerId,
    pub kind: WorkerKind,
    pub core: Arc<ExecutorCore>,
    parker: Parker,
    tick: AtomicUsize,
}

impl WorkerHandle {
    /// Create and register a new worker in the executor pool.
    pub fn new(core: Arc<ExecutorCore>, kind: WorkerKind) -> (Self, WorkerGuard) {
        let id = core.registry().allocate_id();
        let parker = Parker::new();
        let unparker = parker.unparker().clone();
        let worker_deque = Worker::new_fifo();
        let stealer = worker_deque.stealer();

        // A thread may already own a local deque (nested or repeated registration).
        // Drain it into the shared injector *before* installing the new deque, so the
        // queued tasks are neither dropped nor merely shifted onto this thread's new
        // queue. `core.inject` therefore sees no local worker and uses the injector.
        if let Some(previous) = take_any_local_worker() {
            while let Some(task) = previous.pop() {
                core.inject(task);
            }
        }

        // Install deque in thread-local storage for single-hop push/pop
        set_local_worker(id, worker_deque);

        // Register in executor core
        core.registry().register(WorkerEntry {
            id,
            kind,
            stealer,
            unparker,
        });

        let guard = WorkerGuard {
            id,
            core: core.clone(),
        };

        let handle = Self {
            id,
            kind,
            core,
            parker,
            tick: AtomicUsize::new(0),
        };

        (handle, guard)
    }

    /// Try to find a task to run according to the scheduling hierarchy:
    /// 1. Local deque (immediate continuation)
    /// 2. Global injector (batch-steals half to local deque)
    /// 3. Peer stealers (batch-steals half to local deque)
    pub fn find_task(&self) -> Option<TaskHandle> {
        let tick = self.tick.fetch_add(1, AtomicOrdering::Relaxed);

        // Every 64 ticks, check the global injector first to prevent injector starvation
        if tick % 64 == 0 {
            if let Steal::Success(task) = steal_injector_into_local(self.core.injector()) {
                return Some(task);
            }
        }

        // 1. Local deque
        if let Some(task) = local_pop() {
            return Some(task);
        }

        // 2. Global injector (batch-steal into local queue)
        match steal_injector_into_local(self.core.injector()) {
            Steal::Success(task) => return Some(task),
            Steal::Retry => {
                if let Steal::Success(task) = steal_injector_into_local(self.core.injector()) {
                    return Some(task);
                }
            }
            Steal::Empty => {}
        }

        // 3. Peer stealing with fast randomized victim selection (excluding self)
        let stealers = self.core.registry().get_stealers();
        if stealers.len() > 1 {
            let n = stealers.len();
            let start = fast_rand(self.id) % n;
            for i in 0..n {
                let idx = (start + i) % n;
                let (peer_id, ref stealer) = stealers[idx];
                if peer_id == self.id {
                    continue;
                }
                match steal_into_local(stealer) {
                    Steal::Success(task) => return Some(task),
                    Steal::Retry => {
                        if let Steal::Success(task) = steal_into_local(stealer) {
                            return Some(task);
                        }
                    }
                    Steal::Empty => {}
                }
            }
        }

        None
    }

    /// Run the continuous worker loop until the executor is stopped.
    pub fn run_loop(&self) {
        let mut consecutive_rust = 0usize;
        let mut consecutive_python = 0usize;
        const TASK_BUDGET: usize = 32;

        while self.core.is_running() {
            if let Some(task) = self.find_task() {
                // Check task budget for bilingual workers (PRD_v2 §2.3)
                if self.kind == WorkerKind::Bilingual {
                    match task.task_kind() {
                        TaskKind::Rust => {
                            consecutive_rust += 1;
                            consecutive_python = 0;
                        }
                        TaskKind::Python => {
                            consecutive_python += 1;
                            consecutive_rust = 0;
                        }
                    }

                    // If budget exceeded, yield to injector to allow balance
                    if consecutive_rust > TASK_BUDGET || consecutive_python > TASK_BUDGET {
                        consecutive_rust = 0;
                        consecutive_python = 0;
                        thread::yield_now();
                    }
                }

                // Execute the task
                task.run();

                // Hot-cache local batch drain (PRD v2 §2.1)
                let mut local_batch = 0;
                while local_batch < 16 {
                    if let Some(local_task) = local_pop() {
                        local_task.run();
                        local_batch += 1;
                    } else {
                        break;
                    }
                }
                continue;
            }

            // Micro-spin before taking OS locks / thread parking (PRD_v2 §2.1)
            let mut spun_task = None;
            for _ in 0..32 {
                std::hint::spin_loop();
                if let Some(task) = local_pop() {
                    spun_task = Some(task);
                    break;
                }
                if let Steal::Success(task) = steal_injector_into_local(self.core.injector()) {
                    spun_task = Some(task);
                    break;
                }
            }
            if let Some(task) = spun_task {
                task.run();
                continue;
            }

            // No task found: mark as idle and park with double-check
            self.core.registry().mark_idle(self.id);

            // Double check injector before sleeping to prevent missed wake race
            if let Steal::Success(task) = steal_injector_into_local(self.core.injector()) {
                self.core.registry().unmark_idle(self.id);
                task.run();
                continue;
            }

            // Park with responsive 5ms timeout to eliminate idle thread spinning
            self.parker.park_timeout(Duration::from_millis(5));
            self.core.registry().unmark_idle(self.id);
        }
    }

    /// Run tasks until there are none left across the pool.
    pub fn run_until_idle(&self) {
        while let Some(task) = self.find_task() {
            task.run();
        }
    }

    /// Run a single task if available. Returns `true` if a task was executed.
    pub fn run_one(&self) -> bool {
        if let Some(task) = self.find_task() {
            task.run();
            true
        } else {
            false
        }
    }
}

/// RAII Guard that manages worker deregistration and flushes remaining tasks.
pub struct WorkerGuard {
    id: WorkerId,
    core: Arc<ExecutorCore>,
}

impl WorkerGuard {
    pub fn worker_id(&self) -> WorkerId {
        self.id
    }
}

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        // Deregister from pool
        self.core.registry().deregister(self.id);

        // Take back local worker deque and flush any orphan tasks to global injector.
        // This only succeeds if this guard still owns the thread's deque.
        if let Some(worker) = take_local_worker(self.id) {
            while let Some(task) = worker.pop() {
                self.core.inject(task);
            }
        }

        // Unpark an idle worker to ensure flushed tasks get executed
        self.core.registry().unpark_one_idle();
    }
}

/// Start `n_workers` background OS threads running native worker loops.
pub fn start_workers(core: &Arc<ExecutorCore>, n_workers: usize) -> Vec<JoinHandle<()>> {
    let mut handles = Vec::with_capacity(n_workers);

    for i in 0..n_workers {
        let core = core.clone();
        let handle = thread::Builder::new()
            .name(format!("hypertile-worker-{}", i))
            .spawn(move || {
                let (worker, _guard) = WorkerHandle::new(core, WorkerKind::Native);
                worker.run_loop();
            })
            .expect("failed to spawn worker thread");

        handles.push(handle);
    }

    handles
}
