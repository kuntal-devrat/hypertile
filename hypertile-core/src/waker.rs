//! Cross-thread wakers and single-hop continuation routing.
//!
//! When a task completes on worker W and triggers the waker of an awaiting continuation,
//! the waker checks if it is currently running on worker W.
//!
//! If so, the continuation is pushed directly into W's local Chase-Lev deque without
//! touching the global injector or any cross-thread synchronization.
//! This implements the "single-hop handoff" described in PRD_v2 §1.2 and §2.1.
//!
//! If the waker is fired from an external thread (e.g. IO, timer, or another worker),
//! the task is routed into the shared global injector.

use crossbeam_deque::Worker;
use std::cell::RefCell;
use std::sync::Arc;
use std::task::Waker;

use crate::executor::WorkerId;
use crate::task::{TaskCell, TaskHandle};

thread_local! {
    /// Thread-local Chase-Lev deque owned by the active worker on this thread,
    /// tagged with the owning [`WorkerId`].
    ///
    /// The tag lets a stale [`WorkerGuard`](crate::worker::WorkerGuard) detect that the
    /// deque on this thread now belongs to a *newer* worker (nested registration) and
    /// leave it untouched instead of deregistering the wrong worker.
    static LOCAL_WORKER_DEQUE: RefCell<Option<(WorkerId, Worker<TaskHandle>)>> =
        const { RefCell::new(None) };
}

/// Installs a local worker deque for the current thread.
///
/// The caller must first drain any previously installed deque with
/// [`take_any_local_worker`], otherwise its queued tasks are silently dropped.
pub fn set_local_worker(id: WorkerId, worker: Worker<TaskHandle>) {
    LOCAL_WORKER_DEQUE.with(|cell| {
        *cell.borrow_mut() = Some((id, worker));
    });
}

/// Removes and returns the thread's local worker deque regardless of which worker
/// owns it.
///
/// Used when a thread re-registers as a worker: the previous deque still holds
/// queued tasks that must be flushed instead of dropped along with it.
pub fn take_any_local_worker() -> Option<Worker<TaskHandle>> {
    LOCAL_WORKER_DEQUE.with(|cell| cell.borrow_mut().take().map(|(_, worker)| worker))
}

/// Takes back the current thread's local worker deque upon exit or deregistration.
///
/// Only succeeds when `id` still owns the thread-local deque, so a stale guard can
/// never steal a newer worker's queue.
pub fn take_local_worker(id: WorkerId) -> Option<Worker<TaskHandle>> {
    LOCAL_WORKER_DEQUE.with(|cell| {
        let mut slot = cell.borrow_mut();
        match slot.as_ref() {
            Some((owner, _)) if *owner == id => slot.take().map(|(_, worker)| worker),
            _ => None,
        }
    })
}

/// Attempts to push `task` directly into the calling thread's local worker deque.
///
/// Returns `None` when the task was enqueued locally (single-hop handoff), or
/// `Some(task)` when the calling thread is not a Hypertile worker and the caller
/// must fall back to the shared global injector.
///
/// Handing the task back instead of requiring the caller to `clone()` it keeps the
/// spawn fast path free of atomic refcount traffic.
pub fn try_push_local(task: TaskHandle) -> Option<TaskHandle> {
    LOCAL_WORKER_DEQUE.with(|cell| {
        if let Some((_, worker)) = cell.borrow_mut().as_mut() {
            worker.push(task);
            None
        } else {
            Some(task)
        }
    })
}

/// Pops a task from the local worker's deque if available.
pub fn local_pop() -> Option<TaskHandle> {
    LOCAL_WORKER_DEQUE.with(|cell| {
        cell.borrow_mut()
            .as_mut()
            .and_then(|(_, worker)| worker.pop())
    })
}

/// Steals a batch of tasks from a remote stealer and moves them into the local worker deque,
/// popping one task for immediate execution. If on an external thread, steals a single task.
pub fn steal_into_local(
    stealer: &crossbeam_deque::Stealer<TaskHandle>,
) -> crossbeam_deque::Steal<TaskHandle> {
    LOCAL_WORKER_DEQUE.with(|cell| {
        let mut borrow = cell.borrow_mut();
        if let Some((_, worker)) = borrow.as_mut() {
            stealer.steal_batch_and_pop(worker)
        } else {
            stealer.steal()
        }
    })
}

/// Steals a batch of tasks from the global injector and moves them into the local worker deque,
/// popping one task for immediate execution. If on an external thread, steals a single task.
pub fn steal_injector_into_local(
    injector: &crossbeam_deque::Injector<TaskHandle>,
) -> crossbeam_deque::Steal<TaskHandle> {
    LOCAL_WORKER_DEQUE.with(|cell| {
        let mut borrow = cell.borrow_mut();
        if let Some((_, worker)) = borrow.as_mut() {
            injector.steal_batch_and_pop(worker)
        } else {
            injector.steal()
        }
    })
}

/// Creates a standard [`Waker`] for a given [`TaskCell`].
pub fn create_task_waker<T: Send + 'static>(cell: Arc<TaskCell<T>>) -> Waker {
    cell.into()
}
