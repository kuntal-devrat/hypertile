use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyListMethods, PyTuple, PyTupleMethods};

pub mod bilingual;
pub mod cancel;
pub mod exceptions;

use bilingual::PyCoroutineTask;
use cancel::PyCancellationToken;
use exceptions::{PanicInTask, RegistrationError, TaskCancelled};
use hypertile_core::{global_runtime, register_worker as core_register_worker, RegisteredWorker as CoreRegisteredWorker, TaskHandle, WorkerKind};

/// Check whether the active Python runtime has free-threading (PEP 779 / No-GIL) enabled.
#[pyfunction]
fn is_free_threaded(py: Python<'_>) -> bool {
    // Check sys._is_gil_enabled() available on Python 3.13t/3.14t+
    let sys = match py.import("sys") {
        Ok(s) => s,
        Err(_) => return false,
    };

    if let Ok(func) = sys.getattr("_is_gil_enabled") {
        if let Ok(res) = func.call0() {
            if let Ok(gil_enabled) = res.extract::<bool>() {
                return !gil_enabled;
            }
        }
    }

    false
}

/// A registered worker handle exposed to Python.
#[pyclass(name = "RegisteredWorker", unsendable)]
pub struct PyRegisteredWorker {
    inner: Option<CoreRegisteredWorker>,
}

#[pymethods]
impl PyRegisteredWorker {
    pub fn worker_id(&self) -> PyResult<usize> {
        self.inner
            .as_ref()
            .map(|w| w.worker_id())
            .ok_or_else(|| RegistrationError::new_err("worker has already been deregistered"))
    }

    pub fn run_one(&self) -> PyResult<bool> {
        self.inner
            .as_ref()
            .map(|w| w.run_one())
            .ok_or_else(|| RegistrationError::new_err("worker has already been deregistered"))
    }

    pub fn run_until_idle(&self) -> PyResult<()> {
        if let Some(w) = self.inner.as_ref() {
            w.run_until_idle();
            Ok(())
        } else {
            Err(RegistrationError::new_err("worker has already been deregistered"))
        }
    }

    pub fn deregister(&mut self) {
        self.inner = None;
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &mut self,
        _exc_type: Option<Py<PyAny>>,
        _exc_val: Option<Py<PyAny>>,
        _exc_tb: Option<Py<PyAny>>,
    ) {
        self.deregister();
    }
}

/// Register the current Python thread into Hypertile's work-stealing pool.
#[pyfunction]
#[pyo3(signature = (kind = "bilingual"))]
fn register_worker(kind: &str) -> PyResult<PyRegisteredWorker> {
    let worker_kind = match kind.to_lowercase().as_str() {
        "bilingual" => WorkerKind::Bilingual,
        "native" => WorkerKind::Native,
        other => {
            return Err(RegistrationError::new_err(format!(
                "invalid worker kind: '{}', expected 'bilingual' or 'native'",
                other
            )));
        }
    };

    let rt = global_runtime();
    let worker = core_register_worker(rt.core(), worker_kind);
    Ok(PyRegisteredWorker {
        inner: Some(worker),
    })
}

/// Spawn a Python coroutine into Hypertile's shared work-stealing pool.
#[pyfunction]
#[pyo3(signature = (coro, done_callback = None, cancellation_token = None))]
fn spawn_coroutine(
    coro: Py<PyAny>,
    done_callback: Option<Py<PyAny>>,
    cancellation_token: Option<PyCancellationToken>,
) -> PyResult<()> {
    let rt = global_runtime();
    let token = cancellation_token
        .map(|t| t.token())
        .unwrap_or_else(|| Arc::new(AtomicBool::new(false)));

    let task = PyCoroutineTask::new(coro, done_callback, token, rt.core().clone());
    rt.core().inject(TaskHandle::new(task));
    Ok(())
}

/// Hypertile Level 1 native executor entry point: runs a root coroutine to completion.
#[pyfunction]
fn run_level1(py: Python<'_>, main_coro: Py<PyAny>) -> PyResult<Py<PyAny>> {
    use std::sync::mpsc::channel;
    let (tx, rx) = channel();

    // Done callback that sends (result, error) across channel
    let done_fn = pyo3::types::PyCFunction::new_closure(
        py,
        None,
        None,
        move |args, _kwargs| {
            let val = args.get_item(0)?;
            let err = args.get_item(1)?;
            let _ = tx.send((val.unbind(), err.unbind()));
            Ok::<(), PyErr>(())
        },
    )?;

    spawn_coroutine(main_coro, Some(done_fn.into()), None)?;

    // Release GIL while awaiting completion on worker pool
    let result_pair = py.allow_threads(move || {
        rx.recv().expect("failed to receive coroutine result")
    });

    let (val, err) = result_pair;
    if !err.is_none(py) {
        Err(PyErr::from_value(err.into_bound(py)))
    } else {
        Ok(val)
    }
}

/// Inline helper performing cryptographic mixing & hashing.
#[inline(always)]
pub fn run_crypto_pipeline(data: &[u8], rounds: usize) -> Vec<u8> {
    let mut state = 0xcbf29ce484222325_u64;
    for b in data {
        state ^= *b as u64;
        state = state.wrapping_mul(0x100000001b3);
    }
    let mut out = [0u8; 32];
    for r in 0..rounds {
        state = state.rotate_left(13) ^ (r as u64).wrapping_mul(0x517cc1b727220a95);
        state = state.wrapping_add(0x9e3779b97f4a7c15);
        if r % 4 == 0 {
            let slot = (r / 4) % 4;
            out[slot * 8..(slot + 1) * 8].copy_from_slice(&state.to_le_bytes());
        }
    }
    out.to_vec()
}

/// CPU-intensive native Rust computation step (cryptographic mixing & hashing).
/// Releases the GIL during execution to simulate a real-world native compute task.
#[pyfunction]
#[pyo3(signature = (payload, rounds = 100))]
fn native_pipeline_transform(py: Python<'_>, payload: &[u8], rounds: usize) -> Vec<u8> {
    let data = payload.to_vec();
    py.allow_threads(move || run_crypto_pipeline(&data, rounds))
}

fn bridge_task_await<'py, T: 'static>(slf: &Bound<'py, T>) -> PyResult<Py<PyAny>> {
    let py = slf.py();
    if let Ok(asyncio) = py.import("asyncio") {
        if let Ok(loop_obj) = asyncio.call_method0("get_running_loop") {
            let fut = loop_obj.call_method0("create_future")?;
            let fut_clone = fut.clone().unbind();
            let loop_clone = loop_obj.clone().unbind();
            let slf_any = slf.clone().into_any().unbind();

            let cb = pyo3::types::PyCFunction::new_closure(
                py,
                None,
                None,
                move |_args, _kwargs| {
                    Python::with_gil(|py| {
                        let loop_bound = loop_clone.bind(py);
                        let fut_bound = fut_clone.bind(py);
                        if let Ok(cancelled) = fut_bound.call_method0("cancelled") {
                            if let Ok(true) = cancelled.extract::<bool>() {
                                return Ok::<(), PyErr>(());
                            }
                        }
                        let slf_bound = slf_any.bind(py);
                        match slf_bound.call_method0("result") {
                            Ok(val) => {
                                let _ = loop_bound.call_method1(
                                    "call_soon_threadsafe",
                                    (fut_bound.getattr("set_result")?, val),
                                );
                            }
                            Err(err) => {
                                let _ = loop_bound.call_method1(
                                    "call_soon_threadsafe",
                                    (fut_bound.getattr("set_exception")?, err),
                                );
                            }
                        }
                        Ok::<(), PyErr>(())
                    })
                },
            )?;

            slf.as_any().call_method1("add_done_callback", (cb,))?;
            let await_iter = fut.call_method0("__await__")?;
            return Ok(await_iter.unbind());
        }
    }
    Ok(slf.clone().into_any().unbind())
}

struct NativeTaskInner {
    result: parking_lot::Mutex<Option<Result<Vec<u8>, String>>>,
    done: AtomicBool,
    callbacks: parking_lot::Mutex<Vec<Py<PyAny>>>,
}

/// An awaitable native task handle driven directly by Hypertile's work-stealing pool.
#[pyclass(name = "NativeTask")]
#[derive(Clone)]
pub struct PyNativeTask {
    inner: Arc<NativeTaskInner>,
}

#[pymethods]
impl PyNativeTask {
    pub fn done(&self) -> bool {
        self.inner.done.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn result(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let res = self.inner.result.lock();
        match res.as_ref() {
            Some(Ok(v)) => Ok(pyo3::types::PyBytes::new(py, v).into_any().unbind()),
            Some(Err(msg)) => Err(PanicInTask::new_err(msg.clone())),
            None => Err(pyo3::exceptions::PyRuntimeError::new_err("task not completed")),
        }
    }

    pub fn add_done_callback(&self, py: Python<'_>, cb: Py<PyAny>) -> PyResult<()> {
        let mut cb_guard = self.inner.callbacks.lock();
        if self.inner.done.load(std::sync::atomic::Ordering::Acquire) {
            drop(cb_guard);
            let _ = cb.call1(py, ());
        } else {
            cb_guard.push(cb);
        }
        Ok(())
    }

    fn __await__(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        bridge_task_await(&slf)
    }

    fn __iter__(slf: Bound<'_, Self>) -> Bound<'_, Self> {
        slf
    }

    fn __next__(slf: Bound<'_, Self>) -> PyResult<Option<Py<PyAny>>> {
        let py = slf.py();
        if slf.borrow().done() {
            let res = slf.borrow().result(py);
            match res {
                Ok(bytes) => Err(pyo3::exceptions::PyStopIteration::new_err(bytes)),
                Err(err) => Err(err),
            }
        } else {
            Ok(Some(py.None()))
        }
    }
}

/// Spawn a native compute pipeline task directly onto Hypertile's work-stealing pool,
/// returning an awaitable NativeTask.
#[pyfunction]
#[pyo3(signature = (payload, rounds = 100))]
fn spawn_native_pipeline(payload: &[u8], rounds: usize) -> PyNativeTask {
    let inner = Arc::new(NativeTaskInner {
        result: parking_lot::Mutex::new(None),
        done: AtomicBool::new(false),
        callbacks: parking_lot::Mutex::new(Vec::new()),
    });

    let inner_clone = inner.clone();
    let data = payload.to_vec();

    let rt = global_runtime();
    rt.spawn(async move {
        let panic_res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_crypto_pipeline(&data, rounds)
        }));

        match panic_res {
            Ok(output) => {
                *inner_clone.result.lock() = Some(Ok(output));
            }
            Err(panic_payload) => {
                let msg = if let Some(s) = panic_payload.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = panic_payload.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "native task panicked during execution".to_string()
                };
                *inner_clone.result.lock() = Some(Err(msg));
            }
        }
        inner_clone.done.store(true, std::sync::atomic::Ordering::Release);

        let callbacks = {
            let mut cb_guard = inner_clone.callbacks.lock();
            std::mem::take(&mut *cb_guard)
        };
        if !callbacks.is_empty() {
            Python::with_gil(|py| {
                for cb in callbacks {
                    let _ = cb.call1(py, ());
                }
            });
        }
    });

    PyNativeTask { inner }
}

struct BatchNativeInner {
    results: parking_lot::Mutex<Vec<Option<Vec<u8>>>>,
    remaining: std::sync::atomic::AtomicUsize,
    callbacks: parking_lot::Mutex<Vec<Py<PyAny>>>,
    panic_error: parking_lot::Mutex<Option<String>>,
}

/// An awaitable batch of native tasks executed in parallel across Hypertile workers.
#[pyclass(name = "BatchNativeTask")]
#[derive(Clone)]
pub struct PyBatchNativeTask {
    inner: Arc<BatchNativeInner>,
}

#[pymethods]
impl PyBatchNativeTask {
    pub fn done(&self) -> bool {
        self.inner.remaining.load(std::sync::atomic::Ordering::Acquire) == 0
    }

    pub fn result(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if !self.done() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err("batch task not completed"));
        }
        if let Some(err_msg) = self.inner.panic_error.lock().as_ref() {
            return Err(PanicInTask::new_err(err_msg.clone()));
        }
        let guard = self.inner.results.lock();
        let py_list = pyo3::types::PyList::empty(py);
        for item in guard.iter() {
            if let Some(bytes) = item {
                py_list.append(pyo3::types::PyBytes::new(py, bytes))?;
            } else {
                py_list.append(py.None())?;
            }
        }
        Ok(py_list.into_any().unbind())
    }

    pub fn add_done_callback(&self, py: Python<'_>, cb: Py<PyAny>) -> PyResult<()> {
        let mut cb_guard = self.inner.callbacks.lock();
        if self.done() {
            drop(cb_guard);
            let _ = cb.call1(py, ());
        } else {
            cb_guard.push(cb);
        }
        Ok(())
    }

    fn __await__(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        bridge_task_await(&slf)
    }

    fn __iter__(slf: Bound<'_, Self>) -> Bound<'_, Self> {
        slf
    }

    fn __next__(slf: Bound<'_, Self>) -> PyResult<Option<Py<PyAny>>> {
        let py = slf.py();
        if slf.borrow().done() {
            let res = slf.borrow().result(py);
            match res {
                Ok(val) => Err(pyo3::exceptions::PyStopIteration::new_err(val)),
                Err(err) => Err(err),
            }
        } else {
            Ok(Some(py.None()))
        }
    }
}

/// Vectorized batch dispatch of multiple native compute tasks across Hypertile workers.
#[pyfunction]
#[pyo3(signature = (payloads, rounds = 100))]
fn batch_spawn_native_pipeline(payloads: Vec<Vec<u8>>, rounds: usize) -> PyBatchNativeTask {
    let total = payloads.len();
    let inner = Arc::new(BatchNativeInner {
        results: parking_lot::Mutex::new(vec![None; total]),
        remaining: std::sync::atomic::AtomicUsize::new(total),
        callbacks: parking_lot::Mutex::new(Vec::new()),
        panic_error: parking_lot::Mutex::new(None),
    });

    if total == 0 {
        return PyBatchNativeTask { inner };
    }

    let rt = global_runtime();
    let num_workers = rt.core().registry().active_count().max(1);
    let num_chunks = (num_workers * 4).min(total).max(1);
    let chunk_size = total.div_ceil(num_chunks);

    let payloads_arc = Arc::new(payloads);

    for chunk_idx in 0..num_chunks {
        let start = chunk_idx * chunk_size;
        let end = (start + chunk_size).min(total);
        if start >= end {
            break;
        }

        let inner_clone = inner.clone();
        let payloads_ref = payloads_arc.clone();

        rt.spawn(async move {
            let panic_res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let mut chunk_res = Vec::with_capacity(end - start);
                for i in start..end {
                    let output = run_crypto_pipeline(&payloads_ref[i], rounds);
                    chunk_res.push(output);
                }
                chunk_res
            }));

            let count = end - start;
            match panic_res {
                Ok(chunk_res) => {
                    let mut guard = inner_clone.results.lock();
                    for (offset, res) in chunk_res.into_iter().enumerate() {
                        guard[start + offset] = Some(res);
                    }
                }
                Err(payload) => {
                    let msg = if let Some(s) = payload.downcast_ref::<&str>() {
                        s.to_string()
                    } else if let Some(s) = payload.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "batch chunk panicked".to_string()
                    };
                    *inner_clone.panic_error.lock() = Some(msg);
                }
            }

            if inner_clone.remaining.fetch_sub(count, std::sync::atomic::Ordering::AcqRel) == count {
                let callbacks = {
                    let mut cb_guard = inner_clone.callbacks.lock();
                    std::mem::take(&mut *cb_guard)
                };
                if !callbacks.is_empty() {
                    Python::with_gil(|py| {
                        for cb in callbacks {
                            let _ = cb.call1(py, ());
                        }
                    });
                }
            }
        });
    }

    PyBatchNativeTask { inner }
}

struct CallableTaskInner {
    result: parking_lot::Mutex<Option<PyResult<Py<PyAny>>>>,
    done: AtomicBool,
    callbacks: parking_lot::Mutex<Vec<Py<PyAny>>>,
}

/// An awaitable generic Python callable task executed on Hypertile's work-stealing pool.
#[pyclass(name = "CallableTask")]
#[derive(Clone)]
pub struct PyCallableTask {
    inner: Arc<CallableTaskInner>,
}

#[pymethods]
impl PyCallableTask {
    pub fn done(&self) -> bool {
        self.inner.done.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn result(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let res = self.inner.result.lock();
        match res.as_ref() {
            Some(Ok(v)) => Ok(v.clone_ref(py)),
            Some(Err(e)) => Err(e.clone_ref(py)),
            None => Err(pyo3::exceptions::PyRuntimeError::new_err("task not completed")),
        }
    }

    pub fn add_done_callback(&self, py: Python<'_>, cb: Py<PyAny>) -> PyResult<()> {
        let mut cb_guard = self.inner.callbacks.lock();
        if self.inner.done.load(std::sync::atomic::Ordering::Acquire) {
            drop(cb_guard);
            let _ = cb.call1(py, ());
        } else {
            cb_guard.push(cb);
        }
        Ok(())
    }

    fn __await__(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        bridge_task_await(&slf)
    }

    fn __iter__(slf: Bound<'_, Self>) -> Bound<'_, Self> {
        slf
    }

    fn __next__(slf: Bound<'_, Self>) -> PyResult<Option<Py<PyAny>>> {
        let py = slf.py();
        if slf.borrow().done() {
            let res = slf.borrow().result(py);
            match res {
                Ok(val) => Err(pyo3::exceptions::PyStopIteration::new_err(val)),
                Err(err) => Err(err),
            }
        } else {
            Ok(Some(py.None()))
        }
    }
}

/// Spawn any arbitrary Python callable directly onto Hypertile's work-stealing pool.
#[pyfunction]
#[pyo3(signature = (func, args = None, kwargs = None))]
fn spawn_callable(
    _py: Python<'_>,
    func: Py<PyAny>,
    args: Option<Py<PyTuple>>,
    kwargs: Option<Py<PyDict>>,
) -> PyCallableTask {
    let inner = Arc::new(CallableTaskInner {
        result: parking_lot::Mutex::new(None),
        done: AtomicBool::new(false),
        callbacks: parking_lot::Mutex::new(Vec::new()),
    });

    let inner_clone = inner.clone();

    let rt = global_runtime();
    rt.spawn(async move {
        Python::with_gil(|py| {
            let panic_res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                match (args.as_ref(), kwargs.as_ref()) {
                    (Some(a), Some(kw)) => func.bind(py).call(a.bind(py), Some(kw.bind(py))),
                    (Some(a), None) => func.bind(py).call1(a.bind(py)),
                    (None, Some(kw)) => func.bind(py).call((), Some(kw.bind(py))),
                    (None, None) => func.bind(py).call0(),
                }
            }));

            let stored_res = match panic_res {
                Ok(Ok(val)) => Ok(val.unbind()),
                Ok(Err(err)) => Err(err),
                Err(panic_payload) => {
                    let msg = if let Some(s) = panic_payload.downcast_ref::<&str>() {
                        s.to_string()
                    } else if let Some(s) = panic_payload.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "callable task panicked".to_string()
                    };
                    Err(PanicInTask::new_err(msg))
                }
            };

            *inner_clone.result.lock() = Some(stored_res);
            inner_clone.done.store(true, std::sync::atomic::Ordering::Release);

            let callbacks = {
                let mut cb_guard = inner_clone.callbacks.lock();
                std::mem::take(&mut *cb_guard)
            };
            for cb in callbacks {
                let _ = cb.call1(py, ());
            }
        });
    });

    PyCallableTask { inner }
}

struct BatchCallableInner {
    results: parking_lot::Mutex<Vec<Option<PyResult<Py<PyAny>>>>>,
    remaining: std::sync::atomic::AtomicUsize,
    callbacks: parking_lot::Mutex<Vec<Py<PyAny>>>,
}

/// An awaitable batch of Python callables executed in parallel across Hypertile workers.
#[pyclass(name = "BatchCallableTask")]
#[derive(Clone)]
pub struct PyBatchCallableTask {
    inner: Arc<BatchCallableInner>,
}

#[pymethods]
impl PyBatchCallableTask {
    pub fn done(&self) -> bool {
        self.inner.remaining.load(std::sync::atomic::Ordering::Acquire) == 0
    }

    pub fn result(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if !self.done() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err("batch callable task not completed"));
        }
        let guard = self.inner.results.lock();
        let py_list = pyo3::types::PyList::empty(py);
        for item in guard.iter() {
            match item {
                Some(Ok(val)) => py_list.append(val.clone_ref(py))?,
                Some(Err(e)) => return Err(e.clone_ref(py)),
                None => py_list.append(py.None())?,
            }
        }
        Ok(py_list.into_any().unbind())
    }

    pub fn add_done_callback(&self, py: Python<'_>, cb: Py<PyAny>) -> PyResult<()> {
        let mut cb_guard = self.inner.callbacks.lock();
        if self.done() {
            drop(cb_guard);
            let _ = cb.call1(py, ());
        } else {
            cb_guard.push(cb);
        }
        Ok(())
    }

    fn __await__(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        bridge_task_await(&slf)
    }

    fn __iter__(slf: Bound<'_, Self>) -> Bound<'_, Self> {
        slf
    }

    fn __next__(slf: Bound<'_, Self>) -> PyResult<Option<Py<PyAny>>> {
        let py = slf.py();
        if slf.borrow().done() {
            let res = slf.borrow().result(py);
            match res {
                Ok(val) => Err(pyo3::exceptions::PyStopIteration::new_err(val)),
                Err(err) => Err(err),
            }
        } else {
            Ok(Some(py.None()))
        }
    }
}

/// Vectorized batch dispatch of multiple Python callables across Hypertile workers.
#[pyfunction]
fn batch_spawn_callable(
    py: Python<'_>,
    func: Py<PyAny>,
    args_list: Vec<Py<PyTuple>>,
) -> PyBatchCallableTask {
    let total = args_list.len();
    let mut initial_results = Vec::with_capacity(total);
    for _ in 0..total {
        initial_results.push(None);
    }
    let inner = Arc::new(BatchCallableInner {
        results: parking_lot::Mutex::new(initial_results),
        remaining: std::sync::atomic::AtomicUsize::new(total),
        callbacks: parking_lot::Mutex::new(Vec::new()),
    });

    if total == 0 {
        return PyBatchCallableTask { inner };
    }

    let rt = global_runtime();
    let num_workers = rt.core().registry().active_count().max(1);
    let num_chunks = (num_workers * 4).min(total).max(1);
    let chunk_size = total.div_ceil(num_chunks);

    let args_arc = Arc::new(args_list);

    for chunk_idx in 0..num_chunks {
        let start = chunk_idx * chunk_size;
        let end = (start + chunk_size).min(total);
        if start >= end {
            break;
        }

        let inner_clone = inner.clone();
        let func_clone = func.clone_ref(py);
        let args_ref = args_arc.clone();

        rt.spawn(async move {
            Python::with_gil(|py| {
                let mut chunk_res = Vec::with_capacity(end - start);
                for i in start..end {
                    let call_res = func_clone.bind(py).call1(args_ref[i].bind(py));
                    let stored_res = match call_res {
                        Ok(val) => Ok(val.unbind()),
                        Err(err) => Err(err),
                    };
                    chunk_res.push(stored_res);
                }
                let count = chunk_res.len();
                {
                    let mut guard = inner_clone.results.lock();
                    for (offset, res) in chunk_res.into_iter().enumerate() {
                        guard[start + offset] = Some(res);
                    }
                }
                if inner_clone.remaining.fetch_sub(count, std::sync::atomic::Ordering::AcqRel) == count {
                    let callbacks = {
                        let mut cb_guard = inner_clone.callbacks.lock();
                        std::mem::take(&mut *cb_guard)
                    };
                    for cb in callbacks {
                        let _ = cb.call1(py, ());
                    }
                }
            });
        });
    }

    PyBatchCallableTask { inner }
}

/// Python extension module definition.
#[pymodule(name = "_hypertile_sys", gil_used = false)]
fn _hypertile_sys(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("PanicInTask", m.py().get_type::<PanicInTask>())?;
    m.add("TaskCancelled", m.py().get_type::<TaskCancelled>())?;
    m.add("RegistrationError", m.py().get_type::<RegistrationError>())?;
    m.add_class::<PyCancellationToken>()?;
    m.add_class::<PyRegisteredWorker>()?;
    m.add_class::<PyNativeTask>()?;
    m.add_class::<PyCallableTask>()?;
    m.add_class::<PyBatchNativeTask>()?;
    m.add_class::<PyBatchCallableTask>()?;
    m.add_function(wrap_pyfunction!(is_free_threaded, m)?)?;
    m.add_function(wrap_pyfunction!(register_worker, m)?)?;
    m.add_function(wrap_pyfunction!(spawn_coroutine, m)?)?;
    m.add_function(wrap_pyfunction!(run_level1, m)?)?;
    m.add_function(wrap_pyfunction!(native_pipeline_transform, m)?)?;
    m.add_function(wrap_pyfunction!(spawn_native_pipeline, m)?)?;
    m.add_function(wrap_pyfunction!(batch_spawn_native_pipeline, m)?)?;
    m.add_function(wrap_pyfunction!(spawn_callable, m)?)?;
    m.add_function(wrap_pyfunction!(batch_spawn_callable, m)?)?;
    Ok(())
}
