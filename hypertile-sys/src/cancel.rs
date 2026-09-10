//! Cooperative cancellation token shared across Python and Rust.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use pyo3::prelude::*;

use crate::exceptions::TaskCancelled;

#[pyclass(name = "CancellationToken", weakref)]
#[derive(Clone)]
pub struct PyCancellationToken {
    inner: Arc<AtomicBool>,
}

impl Default for PyCancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

#[pymethods]
impl PyCancellationToken {
    #[new]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Trigger cancellation across all threads watching this token.
    pub fn cancel(&self) {
        self.inner.store(true, Ordering::Release);
    }

    /// Check whether cancellation was requested.
    pub fn is_cancelled(&self) -> bool {
        self.inner.load(Ordering::Acquire)
    }

    /// Raise `TaskCancelled` if this token has been cancelled.
    pub fn throw_if_cancelled(&self) -> PyResult<()> {
        if self.is_cancelled() {
            Err(TaskCancelled::new_err("operation was cancelled"))
        } else {
            Ok(())
        }
    }
}

impl PyCancellationToken {
    pub fn token(&self) -> Arc<AtomicBool> {
        self.inner.clone()
    }
}
