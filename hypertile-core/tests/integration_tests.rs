use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hypertile_core::{block_on, register_worker, sleep, JoinError, Runtime, WorkerKind};

#[test]
fn test_basic_spawn_and_await() {
    let rt = Runtime::new(2);
    let handle = rt.spawn(async {
        10 + 32
    });

    let result = block_on(handle).expect("task failed");
    assert_eq!(result, 42);
}

#[test]
fn test_concurrent_tasks_sum() {
    let rt = Runtime::new(4);
    const COUNT: usize = 2000;
    let mut handles = Vec::with_capacity(COUNT);

    for i in 0..COUNT {
        handles.push(rt.spawn(async move {
            i * 2
        }));
    }

    let sum = block_on(async {
        let mut total = 0usize;
        for h in handles {
            total += h.await.unwrap();
        }
        total
    });

    let expected: usize = (0..COUNT).map(|i| i * 2).sum();
    assert_eq!(sum, expected);
}

#[test]
fn test_panic_containment() {
    let rt = Runtime::new(2);

    // Spawn a panicking task
    let panic_handle = rt.spawn(async {
        panic!("intentional test panic in task");
    });

    // Spawn a normal task right after
    let normal_handle = rt.spawn(async {
        "success"
    });

    let panic_result = block_on(panic_handle);
    assert!(panic_result.is_err());
    match panic_result.unwrap_err() {
        JoinError::Panicked(_) => {} // expected
        JoinError::Cancelled => panic!("expected Panicked, got Cancelled"),
    }

    // Verify worker pool is still healthy and normal task succeeds
    let normal_result = block_on(normal_handle).expect("normal task should succeed");
    assert_eq!(normal_result, "success");
}

#[test]
fn test_task_cancellation() {
    let rt = Runtime::new(2);
    let handle = rt.spawn(async {
        sleep(Duration::from_millis(500)).await;
        100
    });

    handle.cancel();

    let result = block_on(handle);
    assert!(result.is_err());
    match result.unwrap_err() {
        JoinError::Cancelled => {}
        JoinError::Panicked(_) => panic!("expected Cancelled, got Panicked"),
    }
}

#[test]
fn test_dynamic_worker_registration() {
    let rt = Runtime::new(1);
    let core = rt.core();

    // Register current thread as a Bilingual worker
    let worker = register_worker(core, WorkerKind::Bilingual);
    assert_eq!(worker.kind(), WorkerKind::Bilingual);

    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();

    // Inject task directly into pool
    let handle = rt.spawn(async move {
        counter_clone.fetch_add(1, Ordering::SeqCst);
        99
    });

    // Worker steps one task
    let executed = worker.run_one();
    assert!(executed);

    let res = block_on(handle).unwrap();
    assert_eq!(res, 99);
    assert_eq!(counter.load(Ordering::SeqCst), 1);

    // Drop worker: deregisters and returns thread to caller
    drop(worker);
}

#[test]
fn test_timer_sleep() {
    let rt = Runtime::new(2);
    let start = Instant::now();

    let handle = rt.spawn(async {
        sleep(Duration::from_millis(60)).await;
        "slept"
    });

    let res = block_on(handle).unwrap();
    let elapsed = start.elapsed();

    assert_eq!(res, "slept");
    assert!(elapsed >= Duration::from_millis(50), "elapsed was {:?}", elapsed);
}

#[test]
fn test_single_hop_continuation() {
    let rt = Runtime::new(2);
    let chain_len = 100;

    let res = block_on(async {
        let mut curr = 0usize;
        for _ in 0..chain_len {
            let h = rt.spawn_local(async move { curr + 1 });
            curr = h.await.unwrap();
        }
        curr
    });

    assert_eq!(res, chain_len);
}
