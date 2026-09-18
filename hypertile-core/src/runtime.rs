use crossbeam_utils::sync::{Parker, Unparker};
use std::future::Future;
use std::pin::pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::JoinHandle as ThreadJoinHandle;
use std::time::{Duration, Instant};

use crate::executor::ExecutorCore;
use crate::task::{JoinHandle, RawTask};
use crate::waker::try_push_local;
use crate::worker::start_workers;

/// Standalone Hypertile runtime instance owning an executor and worker pool.
pub struct Runtime {
    core: Arc<ExecutorCore>,
    worker_threads: parking_lot::Mutex<Vec<ThreadJoinHandle<()>>>,
    num_workers: usize,
}

impl Runtime {
    /// Create and start a new Hypertile runtime with `n_workers` native threads.
    pub fn new(n_workers: usize) -> Arc<Self> {
        let core = ExecutorCore::new();
        let worker_threads = start_workers(&core, n_workers);
        let num_workers = worker_threads.len();

        Arc::new(Self {
            core,
            worker_threads: parking_lot::Mutex::new(worker_threads),
            num_workers,
        })
    }

    /// Number of worker threads this runtime started with.
    ///
    /// The pool size is fixed once the runtime exists, so this is a constant for the
    /// lifetime of the runtime.
    pub fn num_workers(&self) -> usize {
        self.num_workers
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

        if let Some(handle) = try_push_local(handle) {
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

    /// Ask all worker threads to stop, without waiting for them to exit.
    ///
    /// This never blocks, so it is safe to call from an interpreter shutdown hook
    /// or from within a running task.
    pub fn request_shutdown(&self) {
        self.core.shutdown();
    }

    /// Shut down the runtime and join all worker threads.
    pub fn shutdown(&self) {
        self.core.shutdown();
        let current_id = std::thread::current().id();
        let mut to_join = Vec::new();
        {
            let mut threads = self.worker_threads.lock();
            for handle in threads.drain(..) {
                if handle.thread().id() == current_id {
                    continue;
                }
                to_join.push(handle);
            }
        }
        for handle in to_join {
            let _ = handle.join();
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Global runtime instance.
static GLOBAL_RUNTIME: OnceLock<Arc<Runtime>> = OnceLock::new();

/// Environment variable that sets the shared pool's worker count.
///
/// Read once, when the pool starts. It exists because the pool must be sized *before*
/// its first use, which for an application is often before any Hypertile call of your
/// own (a framework or a third-party library can start the pool at import time).
/// Invalid values are ignored with a warning on stderr; `0` means "choose the default".
pub const WORKERS_ENV: &str = "HYPERTILE_WORKERS";

/// Extra workers beyond the logical CPU count, reserved for blocking calls.
const BLOCKING_HEADROOM: usize = 4;

/// Upper bound on the default pool size, so a many-core host does not get a
/// needlessly large thread pool.
const DEFAULT_WORKERS_CAP: usize = 32;

/// The default worker count for the shared pool.
///
/// Two workloads with opposite requirements share this pool:
///
/// * **CPU-bound** work wants about one thread per logical CPU. Threads beyond the
///   core count contend for the same execution units and lose throughput to context
///   switching, so over-provisioning is not free.
/// * **Blocking** work wants *more* threads than cores. A thread blocked in the OS (a
///   synchronous database driver, `time.sleep`, file I/O) consumes no CPU, so a pool of
///   exactly `cores` threads just queues everything past `cores` concurrent blocking
///   calls. This is the case `to_thread` exists for, and a CPU-sized pool ends up
///   slower there than the standard library's default executor, which sizes itself to
///   `min(32, cores + 4)` for the same reason.
///
/// The compromise is the CPU count plus a small blocking headroom, capped. The cap can
/// only ever *add* threads: the count never drops below one worker per logical CPU, so
/// CPU-bound work is never under-provisioned on a large machine.
///
/// Workloads that know better should override this via [`configure_global_runtime`] or
/// the [`WORKERS_ENV`] environment variable; the size cannot change once the pool runs.
pub fn default_worker_count() -> usize {
    let cores = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(4)
        .max(1);

    worker_count_for_cores(cores)
}

/// The pool-size policy, as a pure function of the logical CPU count.
///
/// Split out from [`default_worker_count`] so the policy can be exercised directly,
/// including for machine sizes other than the one running the tests.
fn worker_count_for_cores(cores: usize) -> usize {
    cores.max(
        cores
            .saturating_add(BLOCKING_HEADROOM)
            .min(DEFAULT_WORKERS_CAP),
    )
}

/// Parse a worker count from an environment variable value.
///
/// Split out from the lookup so the parsing rules are testable without mutating the
/// process environment.
///
/// * `Ok(Some(n))` — an explicit positive count.
/// * `Ok(None)` — an explicit request for the default (`0`).
/// * `Err(())` — unusable input, which the caller reports rather than silently ignoring.
fn parse_worker_count(raw: &str) -> Result<Option<usize>, ()> {
    match raw.trim().parse::<usize>() {
        Ok(0) => Ok(None),
        Ok(count) => Ok(Some(count)),
        Err(_) => Err(()),
    }
}

/// The [`WORKERS_ENV`] override, if it is set to something usable.
fn workers_from_env() -> Option<usize> {
    let raw = std::env::var(WORKERS_ENV).ok()?;
    match parse_worker_count(&raw) {
        Ok(explicit) => explicit,
        Err(()) => {
            // A silently ignored pool-size request is the kind of thing that gets
            // debugged for an afternoon, so say something.
            eprintln!(
                "hypertile: ignoring {WORKERS_ENV}={raw:?}; expected a positive integer \
                 (using the default worker count)"
            );
            None
        }
    }
}

/// Error returned when a worker count is requested after the shared pool has started.
///
/// The pool's size is fixed for its lifetime; silently ignoring the request would hide
/// a misconfiguration, so the configuration calls report it instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeAlreadyStarted {
    /// The worker count the running pool was started with.
    pub workers: usize,
}

impl std::fmt::Display for RuntimeAlreadyStarted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the Hypertile worker pool has already started with {} workers, and its size \
             is fixed once it has been used. Set {WORKERS_ENV} before the first Hypertile \
             call (or before importing) to change it",
            self.workers
        )
    }
}

impl std::error::Error for RuntimeAlreadyStarted {}

/// Choose the worker count for a fresh pool.
///
/// `requested == 0` means "automatic": the [`WORKERS_ENV`] override if it is set, and
/// otherwise [`default_worker_count`].
fn resolve_worker_count(requested: usize) -> usize {
    if requested == 0 {
        workers_from_env().unwrap_or_else(default_worker_count)
    } else {
        requested
    }
}

/// Retrieve or initialize the global shared Hypertile runtime.
pub fn global_runtime() -> &'static Arc<Runtime> {
    GLOBAL_RUNTIME.get_or_init(|| Runtime::new(resolve_worker_count(0)))
}

/// Set the shared pool's worker count, before the pool starts.
///
/// `requested == 0` selects the automatic count (see [`default_worker_count`]).
///
/// Returns [`RuntimeAlreadyStarted`] if work has already been submitted, because a
/// running pool cannot be resized. Prefer [`WORKERS_ENV`] when the pool may be started
/// by code that runs before your own (an application framework, for example).
///
/// ```
/// use hypertile_core::{configure_global_runtime, RuntimeAlreadyStarted};
///
/// match configure_global_runtime(4) {
///     Ok(rt) => assert_eq!(rt.num_workers(), 4),
///     // Someone already used the pool; the running size is reported back.
///     Err(RuntimeAlreadyStarted { workers }) => assert!(workers > 0),
/// }
/// ```
pub fn configure_global_runtime(
    requested: usize,
) -> Result<&'static Arc<Runtime>, RuntimeAlreadyStarted> {
    if let Some(existing) = GLOBAL_RUNTIME.get() {
        return Err(RuntimeAlreadyStarted {
            workers: existing.num_workers(),
        });
    }

    // `Runtime::new` starts its threads immediately, so build a candidate and only
    // publish it if we win the race. A loser is dropped here, stopping its own workers.
    let candidate = Runtime::new(resolve_worker_count(requested));
    match GLOBAL_RUNTIME.set(candidate) {
        Ok(()) => Ok(GLOBAL_RUNTIME.get().expect("just published")),
        Err(_outraced) => Err(RuntimeAlreadyStarted {
            workers: GLOBAL_RUNTIME
                .get()
                .expect("the racing thread published a runtime")
                .num_workers(),
        }),
    }
}

/// Initialize the global shared Hypertile runtime with an explicit worker count.
///
/// `num_workers == 0` selects the automatic count. This is the lenient counterpart to
/// [`configure_global_runtime`]: only the first call has an effect, and later calls
/// return the already-initialized runtime unchanged. Use [`configure_global_runtime`]
/// when you need to know whether your request was the one that took effect.
pub fn init_global_runtime(num_workers: usize) -> &'static Arc<Runtime> {
    match configure_global_runtime(num_workers) {
        Ok(runtime) => runtime,
        Err(_already_started) => GLOBAL_RUNTIME
            .get()
            .expect("started before we were told it was running"),
    }
}

/// The global runtime, if it has already been initialized.
pub fn global_runtime_if_init() -> Option<&'static Arc<Runtime>> {
    GLOBAL_RUNTIME.get()
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

/// Shut down the global shared runtime and join its worker threads.
///
/// Never initializes the runtime if it was not already created, and never joins the
/// calling thread. Do **not** call this while holding the GIL: a worker blocked on
/// the interpreter lock cannot exit, which would deadlock the join. Prefer
/// [`shutdown_with_timeout`] from interpreter shutdown hooks.
pub fn shutdown() {
    if let Some(rt) = GLOBAL_RUNTIME.get() {
        rt.shutdown();
    }
    crate::timer::shutdown_timer();
}

/// Stop the global runtime's workers and wait up to `timeout` for them to exit.
///
/// Unlike [`shutdown`] this never initializes the runtime and never joins worker
/// threads, so it is safe to call while an interpreter lock is held. Returns the
/// number of workers still alive when the timeout elapsed.
pub fn shutdown_with_timeout(timeout: Duration) -> usize {
    let Some(rt) = GLOBAL_RUNTIME.get() else {
        crate::timer::shutdown_timer();
        return 0;
    };

    rt.request_shutdown();
    crate::timer::shutdown_timer();

    let deadline = Instant::now() + timeout;
    while rt.core().registry().active_count() > 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    rt.core().registry().active_count()
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

#[cfg(test)]
mod tests {
    use super::*;

    fn live_cores() -> usize {
        std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4)
            .max(1)
    }

    #[test]
    fn the_default_never_drops_below_one_worker_per_cpu() {
        // The whole point of the CPU-count floor: a compute-bound caller must never be
        // given fewer workers than the machine has logical CPUs.
        for cores in [1usize, 2, 4, 8, 16, 31, 32, 64, 256] {
            let workers = worker_count_for_cores(cores);
            assert!(workers >= cores, "{cores} CPUs only got {workers} workers");
        }
    }

    #[test]
    fn machines_below_the_cap_get_blocking_headroom() {
        // Without headroom, blocking calls past the CPU count just queue - the regression
        // this policy exists to fix.
        for cores in [1usize, 2, 4, 8, 16] {
            assert_eq!(
                worker_count_for_cores(cores),
                cores + BLOCKING_HEADROOM,
                "{cores} CPUs should get blocking headroom"
            );
        }
    }

    #[test]
    fn the_cap_never_removes_a_core_or_adds_headroom() {
        // At the cap the headroom disappears rather than pushing past it...
        assert_eq!(worker_count_for_cores(DEFAULT_WORKERS_CAP), 32);
        // ...and above it the pool is exactly one worker per CPU, never capped down.
        for cores in [DEFAULT_WORKERS_CAP + 1, 64, 128] {
            assert_eq!(worker_count_for_cores(cores), cores);
        }
    }

    #[test]
    fn the_live_default_follows_the_policy() {
        assert_eq!(default_worker_count(), worker_count_for_cores(live_cores()));
        assert!(default_worker_count() >= live_cores());
    }

    #[test]
    fn parse_worker_count_reads_positive_integers_and_explicit_auto() {
        assert_eq!(parse_worker_count("4"), Ok(Some(4)));
        assert_eq!(parse_worker_count("  8  "), Ok(Some(8)));
        assert_eq!(parse_worker_count("1"), Ok(Some(1)));

        // Zero is a deliberate request for the default, not an error, so it must not
        // produce the misconfiguration warning.
        assert_eq!(parse_worker_count("0"), Ok(None));
        assert_eq!(parse_worker_count(" 0 "), Ok(None));

        for rejected in ["", "  ", "-2", "four", "4.0", "1e3"] {
            assert_eq!(parse_worker_count(rejected), Err(()), "{rejected:?}");
        }
    }
}
