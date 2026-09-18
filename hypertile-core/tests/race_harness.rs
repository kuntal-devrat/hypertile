//! Deterministic and randomized concurrency harness for the task state machine and the
//! worker registry.
//!
//! # What this does and does not guarantee
//!
//! Every scenario here is driven by a seed derived from the base seed and the iteration
//! index, so a failing iteration can be replayed exactly:
//!
//! ```text
//! HYPERTILE_RACE_SEED=<seed> cargo test -p hypertile-core --test race_harness
//! ```
//!
//! The *operation schedule* (which thread does what, and in which order) is therefore
//! reproducible, but the *OS thread interleaving* is deliberately not: these scenarios
//! let real threads race, because that is what surfaces real races. Treat this as a
//! seeded stress harness, not a model checker. The `deterministic_*` tests below cover
//! the parts of the state machine that can be pinned exactly.
//!
//! Two properties make the randomized part usable:
//!
//! 1. **It fails instead of hanging.** Each iteration runs under a watchdog, because a
//!    harness that deadlocks when it finds a deadlock is worse than useless.
//! 2. **Failures name their seed**, so a flake becomes a permanent regression test.
//!
//! # Tuning
//!
//! * `HYPERTILE_RACE_SEED` — pin the base seed (default: derived from the clock).
//! * `HYPERTILE_RACE_ITERATIONS` — iterations of the task-state scenario.
//! * `HYPERTILE_RACE_CHURN_ITERATIONS` — iterations of the registry-churn scenario.
//! * `HYPERTILE_RACE_TIMEOUT_SECS` — per-iteration watchdog timeout (default 60).
//!
//! `scripts/race_stress.sh` runs both scenarios at high iteration counts across many
//! seeds, which is how this harness is meant to be used when hunting rather than
//! relaying a known seed.

use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, Barrier, Mutex as StdMutex, MutexGuard};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hypertile_core::{
    register_worker, waker::create_task_waker, JoinError, JoinHandle, Runtime, TaskCell,
    TaskHandle, TaskScheduler, WorkerKind,
};

// ---------------------------------------------------------------------------
// Harness plumbing
// ---------------------------------------------------------------------------

/// Deterministic, seedable PRNG (SplitMix64) so a schedule is reproducible.
struct Rng {
    state: u64,
}

impl Rng {
    fn new(seed: u64) -> Self {
        Self {
            state: seed ^ 0x9e37_79b9_7f4a_7c15,
        }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Sample in `0..upper`.
    fn below(&mut self, upper: usize) -> usize {
        if upper == 0 {
            return 0;
        }
        (self.next_u64() % upper as u64) as usize
    }

    fn percent(&mut self, percent: u64) -> bool {
        self.next_u64() % 100 < percent
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn iteration_timeout() -> Duration {
    Duration::from_secs(env_usize("HYPERTILE_RACE_TIMEOUT_SECS", 60) as u64)
}

/// Base seed: pinned by the environment, otherwise derived from the clock.
fn base_seed(story: &str) -> u64 {
    if let Ok(raw) = std::env::var("HYPERTILE_RACE_SEED") {
        if let Ok(seed) = raw.parse::<u64>() {
            return seed;
        }
    }
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    // Mix so two scenarios started in the same nanosecond do not share a schedule.
    let story_mix = story
        .bytes()
        .fold(0u64, |acc, b| {
            (acc ^ b as u64).wrapping_mul(1_099_511_628_211)
        })
        .rotate_left(17);
    nanos ^ story_mix
}

/// Derives a distinct per-thread seed from the base seed.
///
/// The mixing multiplier is wider than the `u64` range once scaled by a stream index, so
/// this must wrap rather than overflow (which panics in debug builds and would make the
/// harness itself the bug).
fn stream_seed(base: u64, stream: u64) -> u64 {
    base ^ (stream.wrapping_mul(0x9e37_79b9_7f4a_7c15))
}

fn panic_text(payload: &(dyn Any + Send)) -> String {
    if let Some(text) = payload.downcast_ref::<&str>() {
        (*text).to_string()
    } else if let Some(text) = payload.downcast_ref::<String>() {
        text.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

/// Locks a mutex without poisoning the harness: one panic should not cascade into others.
fn lock<T>(mutex: &StdMutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Runs `scenario` for `iterations` seeded iterations under a per-iteration watchdog.
///
/// The watchdog reports the seed *before* starting each iteration, so a timeout
/// identifies the exact schedule that got stuck.
fn run_seeded(story: &'static str, iterations: usize, scenario: fn(u64)) {
    let base = base_seed(story);
    println!(
        "[race] {story}: base seed {base}, iterations {iterations} \
         (replay with HYPERTILE_RACE_SEED={base})"
    );

    let (tx, rx) = std::sync::mpsc::channel::<u64>();
    let runner = thread::spawn(move || {
        for index in 0..iterations as u64 {
            let seed = base.wrapping_add(index.wrapping_mul(0x9e37_79b9_7f4a_7c15));
            tx.send(seed).expect("watchdog receiver dropped");

            // Attribute any failure to its seed so the schedule can be replayed.
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                scenario(seed);
            }));
            if let Err(payload) = outcome {
                panic!(
                    "[{story}] iteration failed (replay with HYPERTILE_RACE_SEED={seed}): {}",
                    panic_text(payload.as_ref())
                );
            }
        }
        drop(tx);
    });

    let mut current = base;
    let mut completed = 0usize;
    loop {
        match rx.recv_timeout(iteration_timeout()) {
            Ok(seed) => current = seed,
            Err(RecvTimeoutError::Timeout) => panic!(
                "[{story}] iteration {completed} (seed {current}) did not finish within {:?}: \
                 either a deadlock or a lost wake-up. Replay with HYPERTILE_RACE_SEED={current}",
                iteration_timeout()
            ),
            Err(RecvTimeoutError::Disconnected) => break,
        }
        completed += 1;
    }

    if let Err(payload) = runner.join() {
        std::panic::resume_unwind(payload);
    }
    println!("[race] {story}: {completed} iterations completed cleanly");
}

/// A scheduler that records exactly which tasks it was asked to run.
#[derive(Default)]
struct RecordingScheduler {
    scheduled: StdMutex<Vec<TaskHandle>>,
    count: AtomicUsize,
}

impl RecordingScheduler {
    fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    /// Runs everything scheduled so far, in the order it was scheduled.
    fn run_all(&self) {
        for handle in std::mem::take(&mut *lock(&self.scheduled)) {
            handle.run();
        }
    }
}

impl TaskScheduler for RecordingScheduler {
    fn schedule(&self, task: TaskHandle) {
        self.count.fetch_add(1, Ordering::SeqCst);
        lock(&self.scheduled).push(task);
    }
}

/// A future that yields `remaining` times before completing with `value`.
///
/// `self_wake` selects between the two ways of returning `Pending`: with a wake-up (the
/// common case) or silently, which leaves the task idle and runnable again only once
/// something explicitly wakes it.
struct YieldingTask {
    remaining: usize,
    value: u64,
    self_wake: bool,
    completions: Arc<AtomicUsize>,
}

impl Future for YieldingTask {
    type Output = u64;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<u64> {
        if self.remaining == 0 {
            // A correct state machine polls to `Ready` at most once per task.
            self.completions.fetch_add(1, Ordering::SeqCst);
            return Poll::Ready(self.value);
        }
        self.remaining -= 1;
        if self.self_wake {
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }
}

/// A future that cancels its own `JoinHandle` from inside `poll`.
///
/// This is the re-entrancy case the task state machine must survive: `JoinHandle::cancel`
/// takes the cell's future lock, so an implementation of `TaskCell::run` that holds that
/// lock across `poll` self-deadlocks right here. The watchdog then reports the hang as a
/// seed-named failure rather than wedging the test run.
///
/// The handle is deliberately left in its slot (only `cancel` is called) so the harness can
/// still await the outcome and assert it is `Cancelled`.
struct SelfCancellingTask {
    slot: Arc<StdMutex<Option<JoinHandle<u64>>>>,
    yields_before_cancel: usize,
    polls: usize,
}

impl Future for SelfCancellingTask {
    type Output = u64;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<u64> {
        if self.polls >= self.yields_before_cancel {
            // Hold the slot guard across `cancel` on purpose: this mirrors a caller that
            // owns the handle. The lock it actually needs is the cell's future lock.
            let guard = lock(&self.slot);
            if let Some(handle) = guard.as_ref() {
                handle.cancel();
            }
            // Cancelled from the inside, so this must never be polled to `Ready`.
            return Poll::Pending;
        }
        self.polls += 1;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// Awaits `result`, failing with a readable message instead of unwrapping a `JoinError`.
fn expect_value<T>(result: Result<T, JoinError>, what: &str) -> T {
    match result {
        Ok(value) => value,
        Err(error) => panic!("{what}: expected a completed task, got {error}"),
    }
}

fn expect_cancelled<T>(result: Result<T, JoinError>, what: &str) {
    match result {
        Err(JoinError::Cancelled) => {}
        other => panic!("{what}: expected Cancelled, got {}", describe(&other)),
    }
}

fn describe<T>(result: &Result<T, JoinError>) -> String {
    match result {
        Ok(_) => "Ok(..)".to_string(),
        Err(JoinError::Cancelled) => "Cancelled".to_string(),
        Err(JoinError::Panicked(_)) => "Panicked".to_string(),
    }
}

// ---------------------------------------------------------------------------
// Deterministic state-machine invariants
// ---------------------------------------------------------------------------

/// A wake-up must be enqueued exactly once, and never after completion.
#[test]
fn deterministic_wake_is_deduplicated_and_stops_at_completion() {
    let scheduler = Arc::new(RecordingScheduler::default());
    let completions = Arc::new(AtomicUsize::new(0));
    let (cell, join) = TaskCell::new(
        YieldingTask {
            remaining: 1,
            value: 7,
            self_wake: true,
            completions: completions.clone(),
        },
        scheduler.clone(),
    );
    let handle = TaskHandle::new(cell.clone());
    let waker = create_task_waker(cell.clone());

    // The task starts scheduled, so a wake-up must not queue a second run.
    waker.wake_by_ref();
    assert_eq!(
        scheduler.count(),
        0,
        "waking an already-scheduled task must be a no-op"
    );

    // The first run yields and self-wakes, which must queue exactly one re-run.
    handle.clone().run();
    assert_eq!(
        scheduler.count(),
        1,
        "a self-wake must enqueue exactly once"
    );
    assert_eq!(completions.load(Ordering::SeqCst), 0);

    // While it is queued, further wakes are still no-ops.
    waker.wake_by_ref();
    waker.wake_by_ref();
    assert_eq!(
        scheduler.count(),
        1,
        "duplicate wakes must not enqueue duplicates"
    );

    // Draining the queue completes the task.
    scheduler.run_all();
    assert_eq!(completions.load(Ordering::SeqCst), 1);

    // Waking a finished task must never enqueue work.
    waker.wake_by_ref();
    assert_eq!(
        scheduler.count(),
        1,
        "a completed task must never be re-queued"
    );

    assert_eq!(expect_value(cell_join(join), "dedup task"), 7);
}

/// A task returning `Pending` *without* waking must stay parked until woken, and then a
/// single wake must be enough to run it again. This is the lost-wake-up invariant.
#[test]
fn deterministic_pending_without_wake_needs_exactly_one_wake() {
    let scheduler = Arc::new(RecordingScheduler::default());
    let completions = Arc::new(AtomicUsize::new(0));
    let (cell, join) = TaskCell::new(
        YieldingTask {
            remaining: 1,
            value: 11,
            self_wake: false,
            completions: completions.clone(),
        },
        scheduler.clone(),
    );
    let handle = TaskHandle::new(cell.clone());
    let waker = create_task_waker(cell.clone());

    handle.clone().run();
    assert_eq!(
        scheduler.count(),
        0,
        "a Pending-without-wake task must not be rescheduled by the executor"
    );

    waker.wake_by_ref();
    assert_eq!(
        scheduler.count(),
        1,
        "a single wake must enqueue exactly once"
    );
    waker.wake_by_ref();
    assert_eq!(scheduler.count(), 1, "the second wake must be deduplicated");

    scheduler.run_all();
    assert_eq!(
        completions.load(Ordering::SeqCst),
        1,
        "the task must complete"
    );
    assert_eq!(expect_value(cell_join(join), "parked task"), 11);
}

/// Cancellation and completion are mutually exclusive: whichever lands first wins, and
/// the loser must not overwrite the result or re-run the task.
#[test]
fn deterministic_cancel_and_completion_are_mutually_exclusive() {
    // Case 1: cancellation lands while the task is parked.
    let scheduler = Arc::new(RecordingScheduler::default());
    let completions = Arc::new(AtomicUsize::new(0));
    let (cell, join) = TaskCell::new(
        YieldingTask {
            remaining: 5,
            value: 1,
            self_wake: false,
            completions: completions.clone(),
        },
        scheduler.clone(),
    );
    let handle = TaskHandle::new(cell.clone());
    let waker = create_task_waker(cell.clone());

    handle.clone().run(); // -> Pending, parked
    join.cancel();
    assert_eq!(
        scheduler.count(),
        0,
        "a cancelled task must not be rescheduled"
    );

    waker.wake_by_ref();
    assert_eq!(
        scheduler.count(),
        0,
        "waking a cancelled task must be a no-op"
    );

    handle.clone().run();
    assert_eq!(
        completions.load(Ordering::SeqCst),
        0,
        "a cancelled task must never be polled to completion"
    );
    expect_cancelled(cell_join(join), "cancelled task");

    // Case 2: completion lands first, so a later cancel must not clobber the value.
    let (cell, join) = TaskCell::new(
        YieldingTask {
            remaining: 0,
            value: 99,
            self_wake: true,
            completions: Arc::new(AtomicUsize::new(0)),
        },
        Arc::new(RecordingScheduler::default()),
    );
    TaskHandle::new(cell).run();
    join.cancel();
    assert_eq!(
        expect_value(cell_join(join), "completed task"),
        99,
        "cancel must not overwrite an already-published result"
    );
}

// ---------------------------------------------------------------------------
// Randomized: task state machine under contention
// ---------------------------------------------------------------------------

const STATE_TASKS: usize = 8;
const STATE_WORKERS: usize = 3;
const STATE_STEPS: usize = 256;

/// Hammers `mark_scheduled` / `run` / `complete` / `cancel` from several threads at once:
/// wake storms, direct runs, spontaneous cancellations, and park/yield churn.
///
/// Invariants: every task either completes with its own value or reports `Cancelled`; no
/// task is ever polled to `Ready` twice; nothing is lost (a lost wake-up leaves a task
/// unrunnable, which the watchdog reports as a hang rather than a silent pass).
fn scenario_task_state_under_contention(seed: u64) {
    let runtime = Runtime::new(0); // no background workers: the harness supplies them
    let core = runtime.core().clone();

    let mut setup = Rng::new(seed);
    let mut handles: Vec<TaskHandle> = Vec::with_capacity(STATE_TASKS);
    let mut wakers: Vec<Waker> = Vec::with_capacity(STATE_TASKS);
    let mut joins: Vec<Arc<StdMutex<Option<JoinHandle<u64>>>>> = Vec::with_capacity(STATE_TASKS);
    let mut completions: Vec<Arc<AtomicUsize>> = Vec::with_capacity(STATE_TASKS);
    let mut cancellable: Vec<bool> = Vec::with_capacity(STATE_TASKS);
    let mut self_cancelling: Vec<bool> = Vec::with_capacity(STATE_TASKS);

    for index in 0..STATE_TASKS {
        let counter = Arc::new(AtomicUsize::new(0));
        // The slot is filled with the join handle immediately after the cell exists, and is
        // the same slot a self-cancelling task reaches into from inside its own poll.
        let slot: Arc<StdMutex<Option<JoinHandle<u64>>>> = Arc::new(StdMutex::new(None));
        let self_cancels = index % 4 == 3;

        let (cell, join) = if self_cancels {
            TaskCell::new(
                SelfCancellingTask {
                    slot: slot.clone(),
                    yields_before_cancel: setup.below(2),
                    polls: 0,
                },
                core.clone(),
            )
        } else {
            TaskCell::new(
                YieldingTask {
                    remaining: setup.below(4),
                    value: index as u64,
                    self_wake: true,
                    completions: counter.clone(),
                },
                core.clone(),
            )
        };
        *lock(&slot) = Some(join);

        let handle = TaskHandle::new(cell.clone());
        core.inject(handle.clone());

        // The waker holds an `Arc` to the cell, keeping it alive for the whole scenario.
        wakers.push(create_task_waker(cell));
        handles.push(handle);
        joins.push(slot);
        completions.push(counter);
        // A self-cancelling task already cancels itself; external cancellation would mask it.
        cancellable.push(!self_cancels && setup.percent(50));
        self_cancelling.push(self_cancels);
    }

    let wakers = Arc::new(wakers);
    let handles = Arc::new(handles);
    let joins = Arc::new(joins);
    let cancellable = Arc::new(cancellable);
    let self_cancelling = Arc::new(self_cancelling);

    let barrier = Arc::new(Barrier::new(STATE_WORKERS));
    let mut threads = Vec::with_capacity(STATE_WORKERS);

    for worker_index in 0..STATE_WORKERS {
        let core = core.clone();
        let wakers = wakers.clone();
        let handles = handles.clone();
        let joins = joins.clone();
        let cancellable = cancellable.clone();
        let barrier = barrier.clone();
        let mut rng = Rng::new(stream_seed(seed, worker_index as u64 + 1));

        threads.push(thread::spawn(move || {
            let worker = register_worker(&core, WorkerKind::Native);
            barrier.wait();

            for _ in 0..STATE_STEPS {
                match rng.below(100) {
                    // Find work through the normal path: local deque, injector, peers.
                    0..=44 => {
                        worker.run_one();
                    }
                    // Wake storms: exercise mark_scheduled and the single-hop push.
                    45..=66 => {
                        let index = rng.below(STATE_TASKS);
                        wakers[index].wake_by_ref();
                    }
                    // Run a task handle directly, bypassing the worker loop.
                    67..=79 => {
                        let index = rng.below(STATE_TASKS);
                        handles[index].clone().run();
                    }
                    // Push competing work at this thread's own local deque.
                    80..=87 => {
                        let index = rng.below(STATE_TASKS);
                        core.inject(handles[index].clone());
                    }
                    // Provoke park/unpark races in the idle bookkeeping.
                    88..=94 => thread::yield_now(),
                    _ => thread::sleep(Duration::from_micros(rng.below(80) as u64)),
                }

                // Spontaneous cooperative cancellation.
                if cancellable[rng.below(STATE_TASKS)] && rng.percent(4) {
                    let index = rng.below(STATE_TASKS);
                    if let Some(join) = lock(&joins[index]).as_ref() {
                        join.cancel();
                    }
                }
            }

            // Dropping the worker flushes its local deque into the shared injector. A bug
            // there strands tasks, and the assertions below catch it instead of lying.
            drop(worker);
        }));
    }

    for worker in threads {
        worker.join().expect("worker thread panicked");
    }

    // Everything left is either complete, cancelled, or sitting in the shared injector.
    let drain = register_worker(&core, WorkerKind::Native);
    drain.run_until_idle();
    drain.run_until_idle();

    for index in 0..STATE_TASKS {
        let join = lock(&joins[index])
            .take()
            .expect("join handle consumed once");
        let seen = completions[index].load(Ordering::SeqCst);
        match await_with_timeout(join) {
            Ok(value) => {
                assert!(
                    !self_cancelling[index],
                    "task {index} cancelled its own join handle yet still completed"
                );
                assert_eq!(value, index as u64, "task {index} returned the wrong value");
                assert_eq!(
                    seen, 1,
                    "task {index} was polled to Ready {seen} times; expected exactly once"
                );
            }
            Err(JoinError::Cancelled) => {
                assert!(
                    seen <= 1,
                    "cancelled task {index} was polled to Ready {seen} times"
                );
            }
            Err(JoinError::Panicked(_)) => panic!("task {index} panicked unexpectedly"),
        }
    }

    drop(drain);
    assert_eq!(
        core.registry().active_count(),
        0,
        "the worker registry leaked entries (seed {seed})"
    );
}

#[test]
fn randomized_task_state_under_contention() {
    run_seeded(
        "task_state_under_contention",
        env_usize("HYPERTILE_RACE_ITERATIONS", 24),
        scenario_task_state_under_contention,
    );
}

// ---------------------------------------------------------------------------
// Randomized: worker registry churn
// ---------------------------------------------------------------------------

const CHURN_THREADS: usize = 4;
const CHURN_ROUNDS: usize = 10;
const CHURN_TASKS: usize = 64;

/// Registers, runs work on, and deregisters workers from several threads while another
/// thread keeps injecting tasks.
///
/// Invariants: no task is lost (the completion counter must reach the total), the registry
/// never leaks a worker, and no thread panics while racing on the registry's locks. Tasks
/// spawned from inside a churn worker land on its local deque, so this is also the
/// regression test for the flush-on-deregistration path.
fn scenario_registry_churn(seed: u64) {
    let runtime = Runtime::new(0); // the churn threads are the only workers
    let core = runtime.core().clone();

    let completed = Arc::new(AtomicUsize::new(0));
    let handles: Arc<StdMutex<Vec<JoinHandle<u64>>>> = Arc::new(StdMutex::new(Vec::new()));
    let barrier = Arc::new(Barrier::new(CHURN_THREADS + 1));
    let mut threads = Vec::with_capacity(CHURN_THREADS);

    for churn in 0..CHURN_THREADS {
        let core = core.clone();
        let barrier = barrier.clone();
        let mut rng = Rng::new(stream_seed(seed, churn as u64 + 1));

        threads.push(thread::spawn(move || {
            barrier.wait();
            for _ in 0..CHURN_ROUNDS {
                let worker = register_worker(&core, WorkerKind::Native);

                // Spawned while registered, so these land on this thread's local deque and
                // must survive the deregistration below.
                for _ in 0..rng.below(4) {
                    let yields = rng.below(3);
                    drop(core.spawn(async move {
                        for _ in 0..yields {
                            thread::yield_now();
                        }
                    }));
                }

                for _ in 0..rng.below(3) {
                    worker.run_one();
                }
                if rng.percent(60) {
                    worker.run_until_idle();
                }

                // Randomise whether this worker leaves before or after its peers, which is
                // what makes the concurrent flush path interesting.
                if rng.percent(50) {
                    thread::yield_now();
                }
                if rng.percent(25) {
                    thread::sleep(Duration::from_micros(rng.below(50) as u64));
                }

                drop(worker);
            }
        }));
    }

    // The producer keeps the injector warm and races `unpark_one_idle`.
    let producer = {
        let core = core.clone();
        let completed = completed.clone();
        let handles = handles.clone();
        let barrier = barrier.clone();
        let mut rng = Rng::new(stream_seed(seed, 0xbf58_476d_1ce4_e5b9));
        thread::spawn(move || {
            barrier.wait();
            let mut spawned = Vec::with_capacity(CHURN_TASKS);
            for index in 0..CHURN_TASKS {
                let yields = rng.below(3);
                let counter = completed.clone();
                // Spawned from an unregistered thread, so this goes through the injector.
                spawned.push(core.spawn(async move {
                    for _ in 0..yields {
                        thread::yield_now();
                    }
                    counter.fetch_add(1, Ordering::SeqCst);
                    index as u64
                }));
                if rng.percent(20) {
                    thread::yield_now();
                }
            }
            lock(&handles).extend(spawned);
        })
    };

    for thread in threads {
        thread.join().expect("churn thread panicked");
    }
    producer.join().expect("producer thread panicked");

    // Drain. With every churn worker gone, anything still queued is in the shared
    // injector; a stranded task can never reach `completed == CHURN_TASKS`, and the
    // bounded loop reports what is missing instead of hanging.
    let drain = register_worker(&core, WorkerKind::Native);
    let mut idle_rounds = 0;
    while completed.load(Ordering::SeqCst) < CHURN_TASKS {
        drain.run_until_idle();
        if core.injector().is_empty() {
            idle_rounds += 1;
            if idle_rounds > 3 {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        } else {
            idle_rounds = 0;
        }
    }
    drop(drain);

    let seen = completed.load(Ordering::SeqCst);
    assert_eq!(
        seen,
        CHURN_TASKS,
        "{} of {CHURN_TASKS} tasks were lost during registry churn (seed {seed})",
        CHURN_TASKS - seen
    );
    assert_eq!(
        core.registry().active_count(),
        0,
        "the worker registry leaked entries (seed {seed})"
    );

    // Every handle must still resolve to its own index.
    for (index, join) in lock(&handles).drain(..).enumerate() {
        assert_eq!(
            expect_value(await_with_timeout(join), "churn task"),
            index as u64
        );
    }
}

#[test]
fn randomized_worker_registry_churn() {
    run_seeded(
        "worker_registry_churn",
        env_usize("HYPERTILE_RACE_CHURN_ITERATIONS", 8),
        scenario_registry_churn,
    );
}

// ---------------------------------------------------------------------------
// Harness self-checks
// ---------------------------------------------------------------------------

/// Seeds are only useful if they replay, so the RNG must be reproducible.
#[test]
fn rng_schedules_are_reproducible() {
    let sample = |seed: u64| {
        let mut rng = Rng::new(seed);
        (0..16).map(|_| rng.next_u64()).collect::<Vec<_>>()
    };
    assert_eq!(
        sample(42),
        sample(42),
        "the same seed must replay identically"
    );
    assert_ne!(sample(42), sample(43), "different seeds must diverge");
}

// ---------------------------------------------------------------------------
// Awaiting helpers
// ---------------------------------------------------------------------------

/// Awaits a `JoinHandle` with a bounded wait, so a stuck task fails the test instead of
/// hanging the harness.
fn await_with_timeout<T: Send + 'static>(join: JoinHandle<T>) -> Result<T, JoinError> {
    let (tx, rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(hypertile_core::block_on(join));
    });

    rx.recv_timeout(Duration::from_secs(30))
        .unwrap_or_else(|_| {
            panic!("a task never became runnable: lost wake-up, stranded queue entry, or deadlock")
        })
}

/// Awaits a join handle created by `TaskCell::new`, which is not spawned anywhere and
/// therefore needs its result read directly rather than through the pool.
fn cell_join<T: Send + 'static>(join: JoinHandle<T>) -> Result<T, JoinError> {
    await_with_timeout(join)
}
