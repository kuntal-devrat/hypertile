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

use parking_lot::Mutex;
use pyo3::exceptions::PyStopIteration;
use pyo3::prelude::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::exceptions::{PanicInTask, TaskCancelled};
use hypertile_core::{ExecutorCore, Runnable, TaskHandle, TaskKind};

/// A runnable Python coroutine task scheduled within the Hypertile executor.
pub struct PyCoroutineTask {
    coro: Mutex<Option<Py<PyAny>>>,
    done_callback: Mutex<Option<Py<PyAny>>>,
    pending_value: Mutex<Option<Result<Py<PyAny>, Py<PyAny>>>>,
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
            pending_value: Mutex::new(None),
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
        Python::attach(|py| {
            let mut coro_guard = self.coro.lock();
            let coro = match coro_guard.as_ref() {
                Some(c) => c.bind(py),
                None => return,
            };

            // 1. Check cooperative cancellation
            if self.token.load(Ordering::Acquire) {
                let cancel_exc = TaskCancelled::new_err("coroutine cancelled cooperatively");
                let _ = coro.call_method1("throw", (cancel_exc,));
                *coro_guard = None; // Drop coroutine reference to free memory immediately
                if let Some(cb) = self.done_callback.lock().take() {
                    let _ = cb.call1(py, (py.None(), TaskCancelled::new_err("cancelled")));
                }
                return;
            }

            // 2. Step the coroutine via send(val) or throw(exc) under catch_unwind
            let next_input = self.pending_value.lock().take();
            let unwind_res =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match next_input {
                    Some(Ok(val)) => coro.call_method1("send", (val.bind(py),)),
                    Some(Err(err)) => coro.call_method1("throw", (err.bind(py),)),
                    None => coro.call_method1("send", (py.None(),)),
                }));

            let step_result = match unwind_res {
                Ok(res) => res,
                Err(panic_payload) => {
                    *coro_guard = None;
                    let msg = if let Some(s) = panic_payload.downcast_ref::<&str>() {
                        s.to_string()
                    } else if let Some(s) = panic_payload.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "coroutine panicked during execution".to_string()
                    };
                    if let Some(cb) = self.done_callback.lock().take() {
                        let _ = cb.call1(py, (py.None(), PanicInTask::new_err(msg)));
                    }
                    return;
                }
            };

            match step_result {
                Ok(yielded) => {
                    // Coroutine yielded an awaitable or future.
                    let task_clone = self.clone();
                    let sched = self.scheduler.clone();

                    let mut hooked = false;
                    // If yielded has add_done_callback, hook into it
                    if yielded.hasattr("add_done_callback").unwrap_or(false) {
                        let yielded_clone = yielded.clone().unbind();
                        let wake_fn = pyo3::types::PyCFunction::new_closure(
                            py,
                            None,
                            None,
                            move |args, _kwargs| {
                                Python::attach(|py| {
                                    if let Ok(fut) = args.get_item(0) {
                                        if let Ok(exc) = fut.call_method0("exception") {
                                            if !exc.is_none() {
                                                *task_clone.pending_value.lock() =
                                                    Some(Err(exc.into_any().unbind()));
                                            } else if let Ok(res) = fut.call_method0("result") {
                                                *task_clone.pending_value.lock() =
                                                    Some(Ok(res.into_any().unbind()));
                                            }
                                        } else if let Ok(res) = fut.call_method0("result") {
                                            *task_clone.pending_value.lock() =
                                                Some(Ok(res.into_any().unbind()));
                                        }
                                    } else if let Ok(res) =
                                        yielded_clone.bind(py).call_method0("result")
                                    {
                                        *task_clone.pending_value.lock() =
                                            Some(Ok(res.into_any().unbind()));
                                    }
                                    sched.inject(TaskHandle::new(task_clone.clone()));
                                    Ok::<(), PyErr>(())
                                })
                            },
                        );
                        if let Ok(wake_py) = wake_fn {
                            if yielded
                                .call_method1("add_done_callback", (wake_py,))
                                .is_ok()
                            {
                                hooked = true;
                            }
                        }
                    }
                    if !hooked && yielded.is_none() {
                        // Bare `yield None` (e.g. `await asyncio.sleep(0)`): re-step
                        // immediately, exactly as an asyncio event loop would.
                        self.scheduler.inject(TaskHandle::new(self.clone()));
                    } else if !hooked {
                        // The coroutine is waiting on something this executor cannot
                        // drive. Fail loudly instead of sleeping forever and hanging the
                        // caller that is blocked in `run_level1`.
                        let type_name = yielded
                            .get_type()
                            .name()
                            .map(|name| name.to_string())
                            .unwrap_or_else(|_| "unknown".to_string());
                        let msg = format!(
                            "hypertile Level 1 executor cannot drive an awaitable of type \
                             '{type_name}': only objects exposing add_done_callback() 
                             (e.g. asyncio.Future) are supported. Use hypertile.run(coro) or 
                             hypertile.run(coro, level1=False) for full asyncio support."
                        );
                        *coro_guard = None;
                        if let Some(cb) = self.done_callback.lock().take() {
                            let _ = cb.call1(
                                py,
                                (py.None(), pyo3::exceptions::PyTypeError::new_err(msg)),
                            );
                        }
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

                        *coro_guard = None; // Drop coroutine reference to free memory immediately

                        if let Some(cb) = self.done_callback.lock().take() {
                            let _ = cb.call1(py, (value, py.None()));
                        }
                    } else {
                        // Unhandled exception in coroutine
                        *coro_guard = None; // Drop coroutine reference to free memory immediately

                        if let Some(cb) = self.done_callback.lock().take() {
                            let _ = cb.call1(py, (py.None(), err));
                        }
                    }
                }
            }
        });
    }
}
