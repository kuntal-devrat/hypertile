use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use hypertile_core::{
    block_on, register_worker, sleep, JoinError, JoinHandle, Runtime, WorkerKind,
};

#[test]
fn test_basic_spawn_and_await() {
    let rt = Runtime::new(2);
    let handle = rt.spawn(async { 10 + 32 });

    let result = block_on(handle).expect("task failed");
    assert_eq!(result, 42);
}

#[test]
fn test_concurrent_tasks_sum() {
    let rt = Runtime::new(4);
    const COUNT: usize = 2000;
    let mut handles = Vec::with_capacity(COUNT);

    for i in 0..COUNT {
        handles.push(rt.spawn(async move { i * 2 }));
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
    let normal_handle = rt.spawn(async { "success" });

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
    assert!(
        elapsed >= Duration::from_millis(50),
        "elapsed was {:?}",
        elapsed
    );
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

#[test]
fn test_self_waking_yield_now() {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    struct YieldNow(bool);
    impl Future for YieldNow {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.0 {
                Poll::Ready(())
            } else {
                self.0 = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    let rt = Runtime::new(2);
    let handle = rt.spawn(async {
        YieldNow(false).await;
        YieldNow(false).await;
        999
    });

    let res = block_on(handle).expect("task should not hang when self-waking");
    assert_eq!(res, 999);
}

#[test]
fn test_reregistration_flushes_previous_local_queue() {
    // Deliberately zero background workers: nothing can steal, so the flush is
    // observable instead of being masked by peer stealing.
    let rt = Runtime::new(0);
    let core = rt.core();

    let worker1 = register_worker(core, WorkerKind::Native);
    assert!(worker1.worker_id() > 0, "worker ids must be non-zero");

    let mut handles = Vec::new();
    for i in 0..32usize {
        // `spawn_local` targets the calling thread's own deque.
        handles.push(rt.spawn_local(async move { i }));
    }
    assert_eq!(
        core.injector().len(),
        0,
        "tasks should sit in the local deque"
    );

    // Re-registering replaces this thread's deque. Its queued tasks must be flushed
    // rather than silently dropped along with the old deque.
    let worker2 = register_worker(core, WorkerKind::Native);
    assert_ne!(worker1.worker_id(), worker2.worker_id());
    assert_eq!(
        core.injector().len(),
        32,
        "previous local deque must be flushed into the injector"
    );

    worker2.run_until_idle();

    let total = block_on(async move {
        let mut acc = 0usize;
        for handle in handles {
            acc += handle.await.expect("flushed task must complete");
        }
        acc
    });
    assert_eq!(total, (0..32).sum::<usize>());

    drop(worker2);
    drop(worker1);
}

#[test]
fn test_self_cancel_from_within_poll_does_not_deadlock() {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    /// Yields until its own `JoinHandle` is published, then cancels itself from
    /// inside `poll`. Cancelling takes the cell's future mutex, so an executor that
    /// holds that mutex across `poll` deadlocks here.
    struct SelfCancel {
        handle: Arc<StdMutex<Option<JoinHandle<()>>>>,
        cancelled_tx: Option<std::sync::mpsc::Sender<()>>,
        yielded: bool,
    }

    impl Future for SelfCancel {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            if !self.yielded {
                // The join handle is published by the test thread after `spawn`
                // returns, so keep yielding until it is visible.
                let cancelled = {
                    let guard = self.handle.lock().unwrap();
                    match guard.as_ref() {
                        Some(handle) => {
                            handle.cancel();
                            true
                        }
                        None => false,
                    }
                };
                if cancelled {
                    self.yielded = true;
                    if let Some(tx) = self.cancelled_tx.take() {
                        let _ = tx.send(());
                    }
                }
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(())
        }
    }

    // Leak the runtime: if the executor deadlocks, `Runtime::drop` would block forever
    // joining the stuck worker, so the harness would hang instead of reporting the
    // failure this test exists to catch.
    let rt = std::mem::ManuallyDrop::new(Runtime::new(2));
    let slot: Arc<StdMutex<Option<JoinHandle<()>>>> = Arc::new(StdMutex::new(None));
    let (tx, rx) = std::sync::mpsc::channel();

    *slot.lock().unwrap() = Some(rt.spawn(SelfCancel {
        handle: slot.clone(),
        cancelled_tx: Some(tx),
        yielded: false,
    }));

    // The signal is sent after `cancel()` returns, so a bounded wait turns a
    // regression into a test failure instead of a hung harness.
    if rx.recv_timeout(Duration::from_secs(10)).is_err() {
        panic!("cancel() from within poll deadlocked");
    }

    let handle = slot.lock().unwrap().take();
    if let Some(handle) = handle {
        assert!(
            matches!(block_on(handle), Err(JoinError::Cancelled)),
            "expected the self-cancelled task to report Cancelled"
        );
    }
}

#[test]
fn test_reentrant_timer_wake() {
    let rt = Runtime::new(2);
    let handle = rt.spawn(async {
        sleep(Duration::from_millis(20)).await;
        sleep(Duration::from_millis(20)).await;
        "reentrant-ok"
    });

    let res = block_on(handle).expect("chained sleeps should succeed without deadlock");
    assert_eq!(res, "reentrant-ok");
}
