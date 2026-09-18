//! Tests for the process-global runtime.
//!
//! These live in their own integration-test binary because they mutate state that is
//! shared by the whole process (the global pool and the global timer). Isolating them
//! keeps them deterministic and stops a graceful shutdown from breaking the
//! sleep-based tests in `integration_tests.rs`.

use std::time::{Duration, Instant};

use hypertile_core::{
    global_runtime, global_runtime_if_init, init_global_runtime, shutdown_with_timeout,
};

/// Wait for `n` workers to register, so the assertion does not race thread startup.
fn wait_for_workers(count: usize, timeout: Duration) -> usize {
    let rt = global_runtime();
    let deadline = Instant::now() + timeout;
    while rt.core().registry().active_count() < count && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    rt.core().registry().active_count()
}

#[test]
fn test_init_global_runtime_honors_worker_count() {
    let rt = init_global_runtime(3);
    assert_eq!(wait_for_workers(3, Duration::from_secs(10)), 3);

    // Only the first call configures the pool; later calls are no-ops.
    assert!(std::sync::Arc::ptr_eq(rt, init_global_runtime(9)));
    assert_eq!(global_runtime().core().registry().active_count(), 3);

    // A graceful stop must drain every worker without joining it (joining would
    // deadlock any caller holding a lock a worker needs, e.g. the GIL).
    assert_eq!(
        shutdown_with_timeout(Duration::from_secs(10)),
        0,
        "all workers should exit after the stop flag is set"
    );
    assert!(
        global_runtime_if_init().is_some(),
        "the runtime handle must stay valid after shutdown"
    );
}
