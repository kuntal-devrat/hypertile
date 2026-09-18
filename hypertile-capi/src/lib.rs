//! Lightweight extern "C" ABI for the Hypertile work-stealing runtime.
//!
//! Provides a zero-overhead C interface allowing C, C++, Go (cgo), and Zig
//! applications to embed and leverage Hypertile's work-stealing thread pool.

use std::cell::RefCell;
use std::ffi::c_char;
use std::ffi::c_void;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam_utils::sync::Parker;
use hypertile_core::{
    global_runtime, init_global_runtime, register_worker as core_register_worker,
    RegisteredWorker as CoreRegisteredWorker, WorkerKind,
};
use parking_lot::Mutex;

/// Status codes returned by Hypertile C API functions.
#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum HypertileStatus {
    /// Operation completed successfully.
    Ok = 0,
    /// Task is still executing and not yet ready.
    Pending = 1,
    /// Invalid argument provided (e.g. NULL pointer).
    ErrInvalidArg = -1,
    /// Work function panicked during execution.
    ErrPanic = -2,
    /// Hypertile runtime was not initialized or shut down.
    ErrNotInitialized = -3,
    /// No registered worker on current thread.
    ErrWorkerNotRegistered = -4,
}

/// Function pointer type for work executed on the Hypertile thread pool.
pub type HypertileWorkFn = unsafe extern "C-unwind" fn(*mut c_void) -> *mut c_void;

/// Function pointer type for task completion callbacks.
pub type HypertileCallbackFn = unsafe extern "C-unwind" fn(*mut c_void, *mut c_void);

/// Internal state of an in-flight or completed Hypertile task.
pub struct HypertileTaskInner {
    result: Mutex<Option<Result<usize, String>>>,
    done: AtomicBool,
    unparkers: Mutex<Vec<crossbeam_utils::sync::Unparker>>,
}

/// Opaque task handle representing a spawned unit of work.
pub struct HypertileTask {
    inner: Arc<HypertileTaskInner>,
}

thread_local! {
    static THREAD_WORKER: RefCell<Option<CoreRegisteredWorker>> = const { RefCell::new(None) };
}

/// Initialize Hypertile's global work-stealing runtime.
///
/// If `num_workers` is 0, the pool uses the default size (the logical CPU count plus a
/// small blocking headroom, capped; see [`hypertile_core::default_worker_count`]). The
/// `HYPERTILE_WORKERS` environment variable overrides that default. Calling this
/// function multiple times is safe and idempotent; only the first call determines the
/// worker count, and a pool cannot be resized afterwards.
///
/// # Safety
/// Must be called from an environment where thread spawning is permitted.
#[no_mangle]
pub unsafe extern "C" fn hypertile_init(num_workers: usize) -> i32 {
    let _ = init_global_runtime(num_workers);
    HypertileStatus::Ok as i32
}

/// Shut down the Hypertile global runtime and drain worker threads.
///
/// # Safety
/// The caller must ensure that no concurrent calls to Hypertile functions
/// are made after shutdown has commenced.
#[no_mangle]
pub unsafe extern "C" fn hypertile_shutdown() -> i32 {
    hypertile_core::shutdown();
    HypertileStatus::Ok as i32
}

/// Return version string for the Hypertile C ABI.
#[no_mangle]
pub extern "C" fn hypertile_version() -> *const c_char {
    // Kept in lockstep with the crate version so the ABI can never report a stale
    // release number.
    static VERSION: &[u8] = concat!(env!("CARGO_PKG_VERSION"), "\0").as_bytes();
    VERSION.as_ptr() as *const c_char
}

/// Spawn a work function onto Hypertile's work-stealing pool.
///
/// Returns an opaque task handle that must eventually be freed with
/// `hypertile_task_destroy`. Returns NULL if `work` is NULL.
///
/// # Safety
/// `work` must be a valid, thread-safe function pointer. `arg` must remain
/// valid for the duration of `work`'s execution.
#[no_mangle]
pub unsafe extern "C" fn hypertile_spawn(
    work: Option<HypertileWorkFn>,
    arg: *mut c_void,
) -> *mut HypertileTask {
    let work_fn = match work {
        Some(f) => f,
        None => return std::ptr::null_mut(),
    };

    let inner = Arc::new(HypertileTaskInner {
        result: Mutex::new(None),
        done: AtomicBool::new(false),
        unparkers: Mutex::new(Vec::new()),
    });

    let inner_clone = inner.clone();
    // Function pointers are `Send + Sync + Copy` and can be captured directly; only
    // the bare data pointer has to cross the thread boundary as an integer.
    let arg_addr = arg as usize;

    let rt = global_runtime();
    rt.spawn(async move {
        let work_res = catch_unwind(AssertUnwindSafe(|| {
            let ret = unsafe { work_fn(arg_addr as *mut c_void) };
            ret as usize
        }));

        let mapped_res = match work_res {
            Ok(ret_addr) => Ok(ret_addr),
            Err(e) => {
                let msg = if let Some(s) = e.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = e.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "foreign task panic".to_string()
                };
                Err(msg)
            }
        };

        *inner_clone.result.lock() = Some(mapped_res);
        inner_clone.done.store(true, Ordering::Release);

        let unparkers = {
            let mut guard = inner_clone.unparkers.lock();
            std::mem::take(&mut *guard)
        };
        for unparker in unparkers {
            unparker.unpark();
        }
    });

    Box::into_raw(Box::new(HypertileTask { inner }))
}

/// Poll a task without blocking.
///
/// Returns:
/// - `0` (`HYPERTILE_OK` / ready) if task is completed. `*out_result` is set to the return value.
/// - `1` (`HYPERTILE_PENDING`) if task is still in flight.
/// - Negative `HypertileStatus` on error (e.g. invalid arguments or panic).
///
/// # Safety
/// `task` must be a valid task pointer allocated by `hypertile_spawn`.
/// `out_result` must point to valid writable memory if non-NULL.
#[no_mangle]
pub unsafe extern "C" fn hypertile_poll(
    task: *mut HypertileTask,
    out_result: *mut *mut c_void,
) -> i32 {
    if task.is_null() {
        return HypertileStatus::ErrInvalidArg as i32;
    }

    let task_ref = &*task;
    if !task_ref.inner.done.load(Ordering::Acquire) {
        return HypertileStatus::Pending as i32;
    }

    let lock = task_ref.inner.result.lock();
    if let Some(ref res) = *lock {
        match res {
            Ok(val) => {
                if !out_result.is_null() {
                    *out_result = *val as *mut c_void;
                }
                HypertileStatus::Ok as i32
            }
            Err(_) => HypertileStatus::ErrPanic as i32,
        }
    } else {
        HypertileStatus::Pending as i32
    }
}

/// Block the current thread until the task completes.
///
/// Parks the calling thread while awaiting completion.
/// Sets `*out_result` to the return value of the work function.
///
/// # Safety
/// `task` must be a valid task pointer allocated by `hypertile_spawn`.
/// `out_result` must point to valid writable memory if non-NULL.
#[no_mangle]
pub unsafe extern "C" fn hypertile_wait(
    task: *mut HypertileTask,
    out_result: *mut *mut c_void,
) -> i32 {
    if task.is_null() {
        return HypertileStatus::ErrInvalidArg as i32;
    }

    let task_ref = &*task;

    if !task_ref.inner.done.load(Ordering::Acquire) {
        let parker = Parker::new();
        let unparker = parker.unparker().clone();

        {
            let mut guard = task_ref.inner.unparkers.lock();
            if !task_ref.inner.done.load(Ordering::Acquire) {
                guard.push(unparker);
            } else {
                drop(guard);
            }
        }

        while !task_ref.inner.done.load(Ordering::Acquire) {
            parker.park();
        }
    }

    let lock = task_ref.inner.result.lock();
    if let Some(ref res) = *lock {
        match res {
            Ok(val) => {
                if !out_result.is_null() {
                    *out_result = *val as *mut c_void;
                }
                HypertileStatus::Ok as i32
            }
            Err(_) => HypertileStatus::ErrPanic as i32,
        }
    } else {
        HypertileStatus::ErrNotInitialized as i32
    }
}

/// Free a task handle returned by `hypertile_spawn`.
///
/// Safe to call before or after task completion.
///
/// # Safety
/// `task` must either be NULL or a valid pointer returned by `hypertile_spawn`
/// that has not been previously freed.
#[no_mangle]
pub unsafe extern "C" fn hypertile_task_destroy(task: *mut HypertileTask) {
    if !task.is_null() {
        drop(Box::from_raw(task));
    }
}

/// Spawn an asynchronous task with a completion callback.
///
/// Fire-and-forget: when `work(arg)` completes, `callback(result, user_data)`
/// is automatically invoked. Does not require managing task handles.
///
/// # Safety
/// Both `work` and `callback` must be valid, thread-safe function pointers.
/// `arg` and `user_data` must remain valid for the duration of the task.
#[no_mangle]
pub unsafe extern "C" fn hypertile_spawn_with_callback(
    work: Option<HypertileWorkFn>,
    arg: *mut c_void,
    callback: Option<HypertileCallbackFn>,
    user_data: *mut c_void,
) -> i32 {
    let work_fn = match work {
        Some(f) => f,
        None => return HypertileStatus::ErrInvalidArg as i32,
    };
    let cb_fn = match callback {
        Some(f) => f,
        None => return HypertileStatus::ErrInvalidArg as i32,
    };

    let arg_addr = arg as usize;
    let user_data_addr = user_data as usize;

    let rt = global_runtime();
    rt.spawn(async move {
        let work_res = catch_unwind(AssertUnwindSafe(|| {
            let ret = unsafe { work_fn(arg_addr as *mut c_void) };
            ret as usize
        }));

        let final_result = match work_res {
            Ok(ret_addr) => ret_addr as *mut c_void,
            Err(_) => std::ptr::null_mut(),
        };

        let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
            cb_fn(final_result, user_data_addr as *mut c_void);
        }));
    });

    HypertileStatus::Ok as i32
}

/// Execute a parallel batch of tasks across Hypertile's work-stealing pool.
///
/// Blocks until all `count` items complete. Each item `args[i]` is processed by `work`
/// and written to `out_results[i]`.
///
/// # Safety
/// `args` and `out_results` must point to valid arrays of at least `count` elements.
/// `work` must be thread-safe.
#[no_mangle]
pub unsafe extern "C" fn hypertile_batch_spawn(
    work: Option<HypertileWorkFn>,
    args: *const *mut c_void,
    out_results: *mut *mut c_void,
    count: usize,
) -> i32 {
    if count == 0 {
        return HypertileStatus::Ok as i32;
    }
    let work_fn = match work {
        Some(f) => f,
        None => return HypertileStatus::ErrInvalidArg as i32,
    };
    if args.is_null() || out_results.is_null() {
        return HypertileStatus::ErrInvalidArg as i32;
    }

    let rt = global_runtime();
    let num_workers = rt.core().registry().active_count().max(1);
    let num_chunks = (num_workers * 4).min(count).max(1);
    let chunk_size = count.div_ceil(num_chunks);
    let actual_chunks = count.div_ceil(chunk_size);

    let remaining = Arc::new(AtomicUsize::new(actual_chunks));
    let had_panic = Arc::new(AtomicBool::new(false));
    let parker = Parker::new();
    let unparker = parker.unparker().clone();

    let args_slice = std::slice::from_raw_parts(args, count);
    let out_slice = std::slice::from_raw_parts_mut(out_results, count);

    let args_addr = args_slice.as_ptr() as usize;
    let out_addr = out_slice.as_mut_ptr() as usize;

    for chunk_idx in 0..actual_chunks {
        let start = chunk_idx * chunk_size;
        let end = (start + chunk_size).min(count);

        let rem = remaining.clone();
        let unp = unparker.clone();
        let panic_flag = had_panic.clone();

        rt.spawn(async move {
            let in_base = args_addr as *const *mut c_void;
            let out_base = out_addr as *mut *mut c_void;

            for i in start..end {
                let arg = unsafe { *in_base.add(i) };
                let res = match catch_unwind(AssertUnwindSafe(|| unsafe { work_fn(arg) })) {
                    Ok(r) => r,
                    Err(_) => {
                        panic_flag.store(true, Ordering::Release);
                        std::ptr::null_mut()
                    }
                };
                unsafe { *out_base.add(i) = res };
            }

            if rem.fetch_sub(1, Ordering::AcqRel) == 1 {
                unp.unpark();
            }
        });
    }

    while remaining.load(Ordering::Acquire) > 0 {
        parker.park();
    }

    if had_panic.load(Ordering::Acquire) {
        HypertileStatus::ErrPanic as i32
    } else {
        HypertileStatus::Ok as i32
    }
}

/// Register the current calling thread as an auxiliary worker in the pool.
///
/// Returns the allocated worker ID, or 0 on failure.
///
/// # Safety
/// Must be paired with `hypertile_worker_deregister` on the same thread
/// before thread termination.
#[no_mangle]
pub unsafe extern "C" fn hypertile_register_worker() -> u64 {
    let rt = global_runtime();
    let worker = core_register_worker(rt.core(), WorkerKind::Native);
    let id = worker.worker_id() as u64;
    THREAD_WORKER.with(|w| {
        *w.borrow_mut() = Some(worker);
    });
    id
}

/// Execute work on the current thread until the pool is idle.
///
/// The thread must have been registered via `hypertile_register_worker`.
///
/// # Safety
/// The calling thread must be registered as a worker.
#[no_mangle]
pub unsafe extern "C" fn hypertile_worker_run_until_idle() -> i32 {
    // Move the registration out of thread-local storage while user code runs, so a
    // task that re-registers or deregisters this thread cannot trip a `RefCell`
    // borrow panic.
    let Some(worker) = THREAD_WORKER.with(|w| w.borrow_mut().take()) else {
        return HypertileStatus::ErrWorkerNotRegistered as i32;
    };

    worker.run_until_idle();

    THREAD_WORKER.with(|w| {
        let mut slot = w.borrow_mut();
        if slot.is_none() {
            *slot = Some(worker);
        }
    });

    HypertileStatus::Ok as i32
}

/// Deregister the current thread from the worker pool.
///
/// # Safety
/// Must be called on a thread previously registered via `hypertile_register_worker`.
#[no_mangle]
pub unsafe extern "C" fn hypertile_worker_deregister() -> i32 {
    let deregistered = THREAD_WORKER.with(|w| w.borrow_mut().take().is_some());
    if deregistered {
        HypertileStatus::Ok as i32
    } else {
        HypertileStatus::ErrWorkerNotRegistered as i32
    }
}
