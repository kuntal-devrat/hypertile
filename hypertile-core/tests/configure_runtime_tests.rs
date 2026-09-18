//! Tests for configuring the shared pool *before* it starts.
//!
//! These live in their own integration-test binary for the same reason
//! `global_runtime_tests.rs` does: they mutate process-global state. They additionally
//! require that nothing has started the pool yet, so they must not share a process with
//! any other test that uses the global runtime. `cargo test` gives each test binary its
//! own process, which is what makes the "not started yet" precondition reliable.

use std::time::{Duration, Instant};

use hypertile_core::{
    configure_global_runtime, default_worker_count, global_runtime, global_runtime_if_init,
    shutdown_with_timeout,
};

/// Wait for `count` workers to register, so assertions do not race thread startup.
fn wait_for_workers(count: usize, timeout: Duration) -> usize {
    let rt = global_runtime();
    let deadline = Instant::now() + timeout;
    while rt.core().registry().active_count() < count && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    rt.core().registry().active_count()
}

#[test]
fn test_configure_sizes_the_pool_and_then_refuses_to_resize() {
    // Precondition for every assertion below: this process has not used the pool.
    assert!(
        global_runtime_if_init().is_none(),
        "the global pool must be unstarted when this binary's only test begins"
    );

    let runtime = configure_global_runtime(2).expect("the first configure call wins");
    assert_eq!(
        runtime.num_workers(),
        2,
        "the requested size must be honoured"
    );
    assert_eq!(
        wait_for_workers(2, Duration::from_secs(10)),
        2,
        "exactly two workers should be running"
    );
    assert!(
        global_runtime_if_init().is_some(),
        "configuring a pool must publish it as the global runtime"
    );

    // A running pool cannot be resized, and the error must report the size that is
    // actually running so the caller can tell the difference between "too late" and
    // "the pool really is that size".
    let error = match configure_global_runtime(7) {
        Ok(_) => panic!("a started pool must refuse resizing"),
        Err(error) => error,
    };
    assert_eq!(error.workers, 2);
    let message = error.to_string();
    assert!(
        message.contains("already started") && message.contains("HYPERTILE_WORKERS"),
        "the error must explain the situation and name the escape hatch: {message}"
    );

    // The rejected request must not have spawned a competing pool.
    assert_eq!(
        global_runtime().core().registry().active_count(),
        2,
        "a refused reconfigure must not add workers"
    );

    // The automatic count is the documented policy and is always usable as a size.
    let automatic = default_worker_count();
    assert!(automatic >= 1);

    assert_eq!(
        shutdown_with_timeout(Duration::from_secs(10)),
        0,
        "all workers should exit after the stop flag is set"
    );
}
