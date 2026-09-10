//! Shared pool core state and work-stealing scheduler.
//!
//! Architecture (PRD_v2 §2.1 & §2.4):
//! - Global injector (`crossbeam-deque::Injector<TaskHandle>`)
//! - Per-worker Chase-Lev deques with peer `Stealer` handles registered in a [`WorkerRegistry`]
//! - Fast random-victim peer stealing
//! - Immediate unparking of idle workers upon injection
//! - Implements [`TaskScheduler`] for cross-thread wakes and global dispatch

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use crossbeam_deque::{Injector, Steal, Stealer};
use crossbeam_utils::sync::Unparker;
use crossbeam_utils::CachePadded;
use parking_lot::RwLock;

use crate::task::{JoinHandle, RawTask, TaskHandle, TaskScheduler};

pub type WorkerId = usize;

/// Classification of worker capabilities (PRD_v2 §2.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerKind {
    /// Worker executes only native Rust futures.
    Native,
    /// Worker is interpreter-attached and can step both native futures and Python coroutines.
    Bilingual,
}

/// Information registered for an active worker in the pool.
#[derive(Clone)]
pub struct WorkerEntry {
    pub id: WorkerId,
    pub kind: WorkerKind,
    pub stealer: Stealer<TaskHandle>,
    pub unparker: Unparker,
}

type StealersList = Arc<Vec<(WorkerId, Stealer<TaskHandle>)>>;

/// Registry of all active workers and idle tracking.
/// Uses CachePadded on hot atomics and locks to eliminate cross-core cache-line bouncing.
pub struct WorkerRegistry {
    workers: RwLock<HashMap<WorkerId, WorkerEntry>>,
    idle_stack: CachePadded<RwLock<Vec<WorkerId>>>,
    idle_count: CachePadded<AtomicUsize>,
    stealers_cache: CachePadded<RwLock<StealersList>>,
    next_id: AtomicUsize,
}

impl Default for WorkerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkerRegistry {
    pub fn new() -> Self {
        Self {
            workers: RwLock::new(HashMap::new()),
            idle_stack: CachePadded::new(RwLock::new(Vec::new())),
            idle_count: CachePadded::new(AtomicUsize::new(0)),
            stealers_cache: CachePadded::new(RwLock::new(Arc::new(Vec::new()))),
            next_id: AtomicUsize::new(0),
        }
    }

    /// Allocate a new unique worker ID.
    pub fn allocate_id(&self) -> WorkerId {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Register an active worker.
    pub fn register(&self, entry: WorkerEntry) {
        let id = entry.id;
        let mut workers = self.workers.write();
        workers.insert(id, entry);
        self.rebuild_stealers_cache(&workers);
    }

    /// Deregister a worker upon exit or shutdown.
    pub fn deregister(&self, id: WorkerId) {
        let mut workers = self.workers.write();
        workers.remove(&id);
        self.rebuild_stealers_cache(&workers);

        // Remove from idle stack
        let mut idle = self.idle_stack.write();
        let len_before = idle.len();
        idle.retain(|&w_id| w_id != id);
        let removed = len_before - idle.len();
        if removed > 0 {
            self.idle_count.fetch_sub(removed, Ordering::Release);
        }
    }

    fn rebuild_stealers_cache(&self, workers: &HashMap<WorkerId, WorkerEntry>) {
        let mut list = Vec::with_capacity(workers.len());
        for entry in workers.values() {
            list.push((entry.id, entry.stealer.clone()));
        }
        *self.stealers_cache.write() = Arc::new(list);
    }

    /// Mark a worker as idle and awaiting work.
    pub fn mark_idle(&self, id: WorkerId) {
        let mut idle = self.idle_stack.write();
        if !idle.contains(&id) {
            idle.push(id);
            self.idle_count.fetch_add(1, Ordering::Release);
        }
    }

    /// Unmark a worker as idle.
    pub fn unmark_idle(&self, id: WorkerId) {
        let mut idle = self.idle_stack.write();
        let len_before = idle.len();
        idle.retain(|&w_id| w_id != id);
        let removed = len_before - idle.len();
        if removed > 0 {
            self.idle_count.fetch_sub(removed, Ordering::Release);
        }
    }

    /// Unpark one idle worker if available.
    pub fn unpark_one_idle(&self) -> bool {
        // Fast-path: if no workers are idle, avoid acquiring idle_stack write lock
        if self.idle_count.load(Ordering::Acquire) == 0 {
            return false;
        }

        while let Some(id) = {
            let mut idle = self.idle_stack.write();
            let popped = idle.pop();
            if popped.is_some() {
                self.idle_count.fetch_sub(1, Ordering::Release);
            }
            popped
        } {
            let workers = self.workers.read();
            if let Some(entry) = workers.get(&id) {
                entry.unparker.unpark();
                return true;
            }
        }
        false
    }

    /// Unpark all active workers (e.g. during shutdown or mass injection).
    pub fn unpark_all(&self) {
        let workers = self.workers.read();
        for entry in workers.values() {
            entry.unparker.unpark();
        }
    }

    /// Returns a cached Arc reference of all active stealers for zero-allocation work-stealing.
    pub fn get_stealers(&self) -> Arc<Vec<(WorkerId, Stealer<TaskHandle>)>> {
        self.stealers_cache.read().clone()
    }

    /// Returns the number of currently active workers.
    pub fn active_count(&self) -> usize {
        self.workers.read().len()
    }
}

/// Shared executor pool state.
pub struct ExecutorCore {
    injector: Injector<TaskHandle>,
    registry: Arc<WorkerRegistry>,
    running: AtomicBool,
}

impl ExecutorCore {
    /// Create a new pool state.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            injector: Injector::new(),
            registry: Arc::new(WorkerRegistry::new()),
            running: AtomicBool::new(true),
        })
    }

    pub fn injector(&self) -> &Injector<TaskHandle> {
        &self.injector
    }

    pub fn registry(&self) -> &Arc<WorkerRegistry> {
        &self.registry
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    pub fn set_running(&self, v: bool) {
        self.running.store(v, Ordering::Release);
        if !v {
            self.registry.unpark_all();
        }
    }

    /// Push a task into the global injector or local worker queue and awaken an idle worker.
    pub fn inject(&self, task: TaskHandle) {
        if crate::waker::try_single_hop_push(task.clone()) {
            return;
        }
        self.injector.push(task);
        self.registry.unpark_one_idle();
    }

    /// Steal from the global injector.
    pub fn steal_injector(&self) -> Steal<TaskHandle> {
        self.injector.steal()
    }

    /// Spawn a native Rust future into this executor pool.
    pub fn spawn<F, T>(self: &Arc<Self>, future: F) -> JoinHandle<T>
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (raw_task, join_handle) = RawTask::new(future, self.clone());
        self.inject(raw_task.into_handle());
        join_handle
    }

    /// Shut down the executor.
    pub fn shutdown(&self) {
        self.set_running(false);
    }
}

impl TaskScheduler for ExecutorCore {
    fn schedule(&self, task: TaskHandle) {
        self.inject(task);
    }
}
