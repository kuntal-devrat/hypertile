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

use std::cell::RefCell;
use std::sync::Arc;
use std::task::{Wake, Waker};
use crossbeam_deque::Worker;

use crate::task::{TaskCell, TaskHandle};

thread_local! {
    /// Thread-local Chase-Lev deque owned by the active worker on this thread.
    static LOCAL_WORKER_DEQUE: RefCell<Option<Worker<TaskHandle>>> = const { RefCell::new(None) };
}

/// Sets the current thread's local worker deque.
pub fn set_local_worker(worker: Worker<TaskHandle>) {
    LOCAL_WORKER_DEQUE.with(|cell| {
        *cell.borrow_mut() = Some(worker);
    });
}

/// Takes back the current thread's local worker deque upon exit or deregistration.
pub fn take_local_worker() -> Option<Worker<TaskHandle>> {
    LOCAL_WORKER_DEQUE.with(|cell| cell.borrow_mut().take())
}

/// Pushes a task directly into the local worker's deque if on a worker thread.
/// Returns `true` if single-hop local scheduling succeeded, `false` otherwise.
pub fn try_single_hop_push(task: TaskHandle) -> bool {
    LOCAL_WORKER_DEQUE.with(|cell| {
        if let Some(worker) = cell.borrow_mut().as_mut() {
            worker.push(task);
            true
        } else {
            false
        }
    })
}

/// Pops a task from the local worker's deque if available.
pub fn local_pop() -> Option<TaskHandle> {
    LOCAL_WORKER_DEQUE.with(|cell| {
        cell.borrow_mut().as_mut().and_then(|w| w.pop())
    })
}

/// Checks whether the local worker's deque is empty.
pub fn is_local_empty() -> bool {
    LOCAL_WORKER_DEQUE.with(|cell| {
        cell.borrow().as_ref().map(|w| w.is_empty()).unwrap_or(true)
    })
}

/// A Waker for a specific [`TaskCell`].
struct TaskWaker<T: Send + 'static> {
    cell: Arc<TaskCell<T>>,
}

impl<T: Send + 'static> Wake for TaskWaker<T> {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self.cell.mark_scheduled() {
            let handle = TaskHandle::new(self.cell.clone());
            // 1. Try single-hop handoff onto current worker
            if !try_single_hop_push(handle.clone()) {
                self.cell.schedule_fallback(handle);
            }
        }
    }
}

/// Creates a standard [`Waker`] for a given [`TaskCell`].
pub fn create_task_waker<T: Send + 'static>(cell: Arc<TaskCell<T>>) -> Waker {
    Arc::new(TaskWaker { cell }).into()
}
