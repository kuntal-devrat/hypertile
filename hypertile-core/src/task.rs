//! Task model for Hypertile.
//!
//! Every task runnable by Hypertile is wrapped into a [`TaskHandle`], which is an
//! atomic reference-counted [`Runnable`].
//!
//! Tasks can be native Rust futures ([`TaskCell<T>`]) or foreign Python coroutines
//! (implemented in Phase 2 via `hypertile-sys`).
//!
//! Features:
//! - Pinned, `Send + 'static` futures
//! - Single-hop continuation support via [`JoinHandle`]
//! - Panic containment: worker threads wrap polls in `catch_unwind` and return
//!   [`JoinError::Panicked`] through the [`JoinHandle`] rather than crashing the pool.

use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use parking_lot::Mutex;
use thiserror::Error;

use crate::waker::create_task_waker;

/// Kind of task: native Rust future or interpreter-bound Python coroutine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskKind {
    Rust,
    Python,
}

/// Abstract unit of executable work inside the Hypertile executor.
pub trait Runnable: Send + Sync {
    /// Execute one step / poll of this task.
    fn run(self: Arc<Self>);

    /// Returns the kind of task (for budget & worker affinity enforcement).
    fn task_kind(&self) -> TaskKind {
        TaskKind::Rust
    }
}

/// Opaque handle to a runnable task scheduled in the executor.
#[derive(Clone)]
pub struct TaskHandle {
    inner: Arc<dyn Runnable>,
}

impl TaskHandle {
    pub fn new<R: Runnable + 'static>(runnable: Arc<R>) -> Self {
        Self { inner: runnable }
    }

    pub fn run(self) {
        self.inner.run();
    }

    pub fn task_kind(&self) -> TaskKind {
        self.inner.task_kind()
    }
}

/// Error returned when awaiting a [`JoinHandle`].
#[derive(Debug, Error)]
pub enum JoinError {
    #[error("task panicked during execution")]
    Panicked(Box<dyn Any + Send + 'static>),
    #[error("task was cancelled")]
    Cancelled,
}

/// Atomic state flags for a [`TaskCell`].
const STATE_IDLE: u8 = 0;
const STATE_SCHEDULED: u8 = 1;
const STATE_RUNNING: u8 = 2;
const STATE_COMPLETED: u8 = 3;

/// Trait implemented by the scheduler / executor to accept tasks.
pub trait TaskScheduler: Send + Sync {
    fn schedule(&self, task: TaskHandle);
}

/// An executable native Rust future task cell.
pub struct TaskCell<T> {
    future: Mutex<Option<Pin<Box<dyn Future<Output = T> + Send + 'static>>>>,
    result: Mutex<Option<Result<T, JoinError>>>,
    join_waker: Mutex<Option<Waker>>,
    state: AtomicU8,
    scheduler: Arc<dyn TaskScheduler>,
}

impl<T: Send + 'static> TaskCell<T> {
    pub fn new<F>(future: F, scheduler: Arc<dyn TaskScheduler>) -> (Arc<Self>, JoinHandle<T>)
    where
        F: Future<Output = T> + Send + 'static,
    {
        let cell = Arc::new(Self {
            future: Mutex::new(Some(Box::pin(future))),
            result: Mutex::new(None),
            join_waker: Mutex::new(None),
            state: AtomicU8::new(STATE_SCHEDULED),
            scheduler,
        });

        let join_handle = JoinHandle { cell: cell.clone() };
        (cell, join_handle)
    }

    pub fn schedule_fallback(&self, handle: TaskHandle) {
        self.scheduler.schedule(handle);
    }

    /// Mark the task as scheduled if it was idle, and return whether scheduling is needed.
    pub fn mark_scheduled(&self) -> bool {
        loop {
            let curr = self.state.load(Ordering::Acquire);
            if curr == STATE_COMPLETED {
                return false;
            }
            if curr == STATE_SCHEDULED {
                return false; // Already queued
            }
            if curr == STATE_RUNNING {
                // Running task woke itself; we transition back to scheduled so it re-queues
                // after polling completes.
                return false;
            }
            if self
                .state
                .compare_exchange_weak(curr, STATE_SCHEDULED, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
    }
}

impl<T: Send + 'static> Runnable for TaskCell<T> {
    fn run(self: Arc<Self>) {
        // Transition from SCHEDULED to RUNNING
        if self
            .state
            .compare_exchange(
                STATE_SCHEDULED,
                STATE_RUNNING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            // Task was already completed or altered
            return;
        }

        let mut future_slot = self.future.lock();
        if let Some(mut fut) = future_slot.take() {
            let waker = create_task_waker(self.clone());
            let mut cx = Context::from_waker(&waker);

            // Catch panics to prevent poisoning worker threads
            let poll_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                fut.as_mut().poll(&mut cx)
            }));

            match poll_result {
                Ok(Poll::Ready(output)) => {
                    *self.result.lock() = Some(Ok(output));
                    self.state.store(STATE_COMPLETED, Ordering::Release);
                    // Drop future immediately to release its resources
                    drop(fut);

                    // Single-hop handoff: wake any continuation awaiting this join handle!
                    if let Some(join_waker) = self.join_waker.lock().take() {
                        join_waker.wake();
                    }
                }
                Ok(Poll::Pending) => {
                    // Put future back
                    *future_slot = Some(fut);

                    // Check if a wake() arrived while we were polling
                    let prev = self.state.swap(STATE_IDLE, Ordering::AcqRel);
                    if prev == STATE_SCHEDULED {
                        // Re-schedule immediately
                        self.scheduler.schedule(TaskHandle::new(self.clone()));
                    }
                }
                Err(panic_payload) => {
                    *self.result.lock() = Some(Err(JoinError::Panicked(panic_payload)));
                    self.state.store(STATE_COMPLETED, Ordering::Release);
                    drop(fut);

                    if let Some(join_waker) = self.join_waker.lock().take() {
                        join_waker.wake();
                    }
                }
            }
        }
    }

    fn task_kind(&self) -> TaskKind {
        TaskKind::Rust
    }
}

/// Handle to await the outcome of a spawned task.
pub struct JoinHandle<T> {
    cell: Arc<TaskCell<T>>,
}

impl<T> JoinHandle<T> {
    /// Cancel the task if not yet completed.
    pub fn cancel(&self) {
        let prev = self.cell.state.swap(STATE_COMPLETED, Ordering::AcqRel);
        if prev != STATE_COMPLETED {
            // Drop the future
            *self.cell.future.lock() = None;
            *self.cell.result.lock() = Some(Err(JoinError::Cancelled));
            if let Some(waker) = self.cell.join_waker.lock().take() {
                waker.wake();
            }
        }
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut res = self.cell.result.lock();
        if let Some(output) = res.take() {
            Poll::Ready(output)
        } else {
            if self.cell.state.load(Ordering::Acquire) == STATE_COMPLETED {
                return Poll::Ready(Err(JoinError::Cancelled));
            }
            *self.cell.join_waker.lock() = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

/// Convenience struct for constructing a pinned `Send + 'static` task.
pub struct RawTask {
    handle: TaskHandle,
}

impl RawTask {
    pub fn new<F, T>(future: F, scheduler: Arc<dyn TaskScheduler>) -> (Self, JoinHandle<T>)
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (cell, join_handle) = TaskCell::new(future, scheduler);
        let handle = TaskHandle::new(cell);
        (Self { handle }, join_handle)
    }

    pub fn into_handle(self) -> TaskHandle {
        self.handle
    }
}
