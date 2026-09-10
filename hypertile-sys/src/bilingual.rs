//! Bilingual stepping engine for Python coroutines on Hypertile workers.
//!
//! Enables Python coroutines to be polled directly by bilingual workers in the
//! shared work-stealing pool via `coro.send(None)`.
//!
//! Handles:
//! - Interpreter attach/detach batching
//! - Cooperative cancellation checks
//! - Single-hop completion handoff to continuations
//! - Exception propagation (translating StopIteration to success, and errors to failure)

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use parking_lot::Mutex;
use pyo3::exceptions::PyStopIteration;
use pyo3::prelude::*;

use hypertile_core::{ExecutorCore, Runnable, TaskHandle, TaskKind};
use crate::exceptions::TaskCancelled;

/// A runnable Python coroutine task scheduled within the Hypertile executor.
pub struct PyCoroutineTask {
    coro: Mutex<Option<Py<PyAny>>>,
    done_callback: Mutex<Option<Py<PyAny>>>,
    token: Arc<AtomicBool>,
    scheduler: Arc<ExecutorCore>,
}

impl PyCoroutineTask {
    pub fn new(
        coro: Py<PyAny>,
        done_callback: Option<Py<PyAny>>,
        token: Arc<AtomicBool>,
        scheduler: Arc<ExecutorCore>,
    ) -> Arc<Self> {
        Arc::new(Self {
            coro: Mutex::new(Some(coro)),
            done_callback: Mutex::new(done_callback),
            token,
            scheduler,
        })
    }
}

impl Runnable for PyCoroutineTask {
    fn task_kind(&self) -> TaskKind {
        TaskKind::Python
    }

    fn run(self: Arc<Self>) {
        Python::with_gil(|py| {
            let coro_guard = self.coro.lock();
            let coro = match coro_guard.as_ref() {
                Some(c) => c.bind(py),
                None => return,
            };

            // 1. Check cooperative cancellation
            if self.token.load(Ordering::Acquire) {
                let cancel_exc = TaskCancelled::new_err("coroutine cancelled cooperatively");
                let _ = coro.call_method1("throw", (cancel_exc,));
                if let Some(cb) = self.done_callback.lock().take() {
                    let _ = cb.call1(py, (py.None(), TaskCancelled::new_err("cancelled")));
                }
                return;
            }

            // 2. Step the coroutine via send(None)
            let step_result = coro.call_method1("send", (py.None(),));

            match step_result {
                Ok(yielded) => {
                    // Coroutine yielded an awaitable or future.
                    // We register a done callback to reschedule this task when ready.
                    let task_clone = self.clone();
                    let sched = self.scheduler.clone();

                    // If yielded has add_done_callback, hook into it
                    if yielded.hasattr("add_done_callback").unwrap_or(false) {
                        let wake_fn = pyo3::types::PyCFunction::new_closure(
                            py,
                            None,
                            None,
                            move |_args, _kwargs| {
                                sched.inject(TaskHandle::new(task_clone.clone()));
                                Ok::<(), PyErr>(())
                            },
                        );
                        if let Ok(wake_py) = wake_fn {
                            let _ = yielded.call_method1("add_done_callback", (wake_py,));
                        }
                    } else {
                        // Reschedule directly
                        self.scheduler.inject(TaskHandle::new(self.clone()));
                    }
                }
                Err(err) => {
                    // Check if it's StopIteration (successful completion)
                    if err.is_instance_of::<PyStopIteration>(py) {
                        // Extract return value from StopIteration.value
                        let value = err
                            .value(py)
                            .getattr("value")
                            .map(|v| v.unbind())
                            .unwrap_or_else(|_| py.None());

                        if let Some(cb) = self.done_callback.lock().take() {
                            let _ = cb.call1(py, (value, py.None()));
                        }
                    } else {
                        // Unhandled exception in coroutine
                        if let Some(cb) = self.done_callback.lock().take() {
                            let _ = cb.call1(py, (py.None(), err));
                        }
                    }
                }
            }
        });
    }
}
