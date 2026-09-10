use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread::JoinHandle as ThreadJoinHandle;
use crossbeam_utils::sync::{Parker, Unparker};

use crate::executor::ExecutorCore;
use crate::task::{JoinHandle, RawTask};
use crate::waker::try_single_hop_push;
use crate::worker::start_workers;

/// Standalone Hypertile runtime instance owning an executor and worker pool.
pub struct Runtime {
    core: Arc<ExecutorCore>,
    worker_threads: parking_lot::Mutex<Vec<ThreadJoinHandle<()>>>,
}

impl Runtime {
    /// Create and start a new Hypertile runtime with `n_workers` native threads.
    pub fn new(n_workers: usize) -> Arc<Self> {
        let core = ExecutorCore::new();
        let worker_threads = start_workers(&core, n_workers);

        Arc::new(Self {
            core,
            worker_threads: parking_lot::Mutex::new(worker_threads),
        })
    }

    /// Access the underlying executor core.
    pub fn core(&self) -> &Arc<ExecutorCore> {
        &self.core
    }

    /// Spawn a `Send + 'static` future into the runtime pool.
    pub fn spawn<F, T>(&self, future: F) -> JoinHandle<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.core.spawn(future)
    }

    /// Spawn a future into the local worker's queue if currently on a worker thread,
    /// or into the global injector otherwise.
    pub fn spawn_local<F, T>(&self, future: F) -> JoinHandle<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (raw_task, join_handle) = RawTask::new(future, self.core.clone());
        let handle = raw_task.into_handle();

        if !try_single_hop_push(handle.clone()) {
            self.core.inject(handle);
        }

        join_handle
    }

    /// Execute a future on the calling thread until completion.
    pub fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        block_on(future)
    }

    /// Shut down the runtime and join all worker threads.
    pub fn shutdown(&self) {
        self.core.shutdown();
        let mut threads = self.worker_threads.lock();
        for handle in threads.drain(..) {
            let _ = handle.join();
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        self.shutdown();
    }
}

// Global runtime instance
static GLOBAL_RUNTIME: parking_lot::Once = parking_lot::Once::new();
static mut GLOBAL_RUNTIME_PTR: *const Arc<Runtime> = std::ptr::null();

/// Retrieve or initialize the global shared Hypertile runtime.
pub fn global_runtime() -> &'static Arc<Runtime> {
    GLOBAL_RUNTIME.call_once(|| {
        let num_cpus = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4);
        let rt = Box::new(Runtime::new(num_cpus));
        unsafe {
            GLOBAL_RUNTIME_PTR = Box::into_raw(rt);
        }
    });
    unsafe { &*GLOBAL_RUNTIME_PTR }
}

/// Spawn a top-level future onto the global shared runtime.
pub fn spawn<F, T>(future: F) -> JoinHandle<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    global_runtime().spawn(future)
}

/// Spawn a future with affinity to the current worker if possible.
pub fn spawn_local<F, T>(future: F) -> JoinHandle<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    global_runtime().spawn_local(future)
}

/// Shut down the global shared runtime.
pub fn shutdown() {
    global_runtime().shutdown();
}

struct BlockingWaker(Unparker);

impl Wake for BlockingWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

/// Blocks the current thread until the provided future completes.
pub fn block_on<F: Future>(future: F) -> F::Output {
    let mut pinned = pin!(future);
    let parker = Parker::new();
    let unparker = parker.unparker().clone();
    let waker: Waker = Arc::new(BlockingWaker(unparker)).into();
    let mut cx = Context::from_waker(&waker);

    loop {
        match pinned.as_mut().poll(&mut cx) {
            Poll::Ready(val) => return val,
            Poll::Pending => parker.park(),
        }
    }
}
