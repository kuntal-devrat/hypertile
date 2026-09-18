//! Timer wheel and sleep futures.
//!
//! Provides a dedicated, minimal timer subsystem so that native futures and
//! Python tasks can perform asynchronous sleeps and timeouts without depending
//! on Tokio's runtime.

use parking_lot::{Condvar, Mutex};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

struct TimerEntry {
    deadline: Instant,
    id: u64,
    waker: Arc<Mutex<Option<Waker>>>,
}

impl PartialEq for TimerEntry {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline && self.id == other.id
    }
}

impl Eq for TimerEntry {}

impl PartialOrd for TimerEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimerEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap
        other
            .deadline
            .cmp(&self.deadline)
            .then_with(|| other.id.cmp(&self.id))
    }
}

struct TimerState {
    heap: BinaryHeap<TimerEntry>,
    shutdown: bool,
}

/// Global timer manager driving scheduled wakers.
pub struct TimerDriver {
    state: Mutex<TimerState>,
    condvar: Condvar,
    next_id: AtomicU64,
}

impl TimerDriver {
    pub fn new() -> Arc<Self> {
        let driver = Arc::new(Self {
            state: Mutex::new(TimerState {
                heap: BinaryHeap::new(),
                shutdown: false,
            }),
            condvar: Condvar::new(),
            next_id: AtomicU64::new(1),
        });

        let driver_clone = driver.clone();
        std::thread::Builder::new()
            .name("hypertile-timer".to_string())
            .spawn(move || {
                driver_clone.run_timer_loop();
            })
            .expect("failed to spawn timer thread");

        driver
    }

    fn run_timer_loop(&self) {
        let mut state = self.state.lock();

        while !state.shutdown {
            let now = Instant::now();

            // Collect all expired timers without holding the lock during wake()
            let mut expired = Vec::new();
            while let Some(entry) = state.heap.peek() {
                if entry.deadline <= now {
                    let entry = state.heap.pop().unwrap();
                    let maybe_waker = entry.waker.lock().take();
                    if let Some(w) = maybe_waker {
                        expired.push(w);
                    }
                } else {
                    break;
                }
            }

            if !expired.is_empty() {
                drop(state);
                for waker in expired {
                    waker.wake();
                }
                state = self.state.lock();
            }

            if state.shutdown {
                break;
            }

            // Sleep until next deadline or new registration
            if let Some(next) = state.heap.peek() {
                let timeout = next.deadline.saturating_duration_since(Instant::now());
                if timeout > Duration::ZERO {
                    self.condvar.wait_for(&mut state, timeout);
                }
            } else {
                self.condvar.wait(&mut state);
            }
        }
    }

    pub fn register(&self, deadline: Instant, waker: Arc<Mutex<Option<Waker>>>) -> u64 {
        let id = self.next_id.fetch_add(1, AtomicOrdering::Relaxed);
        let mut state = self.state.lock();
        state.heap.push(TimerEntry {
            deadline,
            id,
            waker,
        });
        self.condvar.notify_one();
        id
    }

    pub fn shutdown(&self) {
        let mut state = self.state.lock();
        state.shutdown = true;
        self.condvar.notify_all();
    }
}

// Lazy global timer driver instance
static GLOBAL_TIMER: OnceLock<Arc<TimerDriver>> = OnceLock::new();

fn get_timer_driver() -> &'static Arc<TimerDriver> {
    GLOBAL_TIMER.get_or_init(TimerDriver::new)
}

/// Shut down the global timer subsystem, if it was ever started.
pub fn shutdown_timer() {
    if let Some(driver) = GLOBAL_TIMER.get() {
        driver.shutdown();
    }
}

/// Asynchronous sleep future.
pub struct Sleep {
    deadline: Instant,
    waker_slot: Arc<Mutex<Option<Waker>>>,
    registered: bool,
}

impl Sleep {
    pub fn new(duration: Duration) -> Self {
        Self {
            deadline: Instant::now() + duration,
            waker_slot: Arc::new(Mutex::new(None)),
            registered: false,
        }
    }

    pub fn deadline(&self) -> Instant {
        self.deadline
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let now = Instant::now();
        // Allow a 50-microsecond threshold for cross-core clock skew / timer granularity
        if now >= self.deadline
            || self.deadline.saturating_duration_since(now) <= Duration::from_micros(50)
        {
            *self.waker_slot.lock() = None;
            Poll::Ready(())
        } else {
            *self.waker_slot.lock() = Some(cx.waker().clone());
            if !self.registered {
                get_timer_driver().register(self.deadline, self.waker_slot.clone());
                self.registered = true;
            }
            Poll::Pending
        }
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        *self.waker_slot.lock() = None;
    }
}

/// Asynchronously sleep for the specified duration.
pub fn sleep(duration: Duration) -> Sleep {
    Sleep::new(duration)
}
