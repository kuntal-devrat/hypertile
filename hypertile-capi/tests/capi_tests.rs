use std::ffi::{c_void, CStr};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use hypertile_capi::*;

unsafe extern "C-unwind" fn double_number(arg: *mut c_void) -> *mut c_void {
    let val = arg as usize;
    (val * 2) as *mut c_void
}

unsafe extern "C-unwind" fn slow_work(arg: *mut c_void) -> *mut c_void {
    std::thread::sleep(Duration::from_millis(30));
    arg
}

unsafe extern "C-unwind" fn panicking_work(_arg: *mut c_void) -> *mut c_void {
    panic!("intentional C ABI panic test");
}

#[test]
fn test_capi_init_and_version() {
    unsafe {
        let status = hypertile_init(4);
        assert_eq!(status, HypertileStatus::Ok as i32);

        let ver = hypertile_version();
        let c_str = CStr::from_ptr(ver);
        assert_eq!(c_str.to_str().unwrap(), "0.1.1");
    }
}

#[test]
fn test_capi_spawn_and_wait() {
    unsafe {
        let _ = hypertile_init(2);

        let task = hypertile_spawn(Some(double_number), 21 as *mut c_void);
        assert!(!task.is_null());

        let mut result: *mut c_void = std::ptr::null_mut();
        let status = hypertile_wait(task, &mut result);
        assert_eq!(status, HypertileStatus::Ok as i32);
        assert_eq!(result as usize, 42);

        hypertile_task_destroy(task);
    }
}

#[test]
fn test_capi_poll_lifecycle() {
    unsafe {
        let _ = hypertile_init(2);

        let task = hypertile_spawn(Some(slow_work), 99 as *mut c_void);
        assert!(!task.is_null());

        let mut result: *mut c_void = std::ptr::null_mut();
        let mut ready = false;

        for _ in 0..100 {
            let status = hypertile_poll(task, &mut result);
            if status == 0 {
                ready = true;
                assert_eq!(result as usize, 99);
                break;
            }
            assert_eq!(status, HypertileStatus::Pending as i32);
            std::thread::sleep(Duration::from_millis(2));
        }

        assert!(ready, "Task failed to complete within poll window");
        hypertile_task_destroy(task);
    }
}

#[test]
fn test_capi_panic_containment() {
    unsafe {
        let _ = hypertile_init(2);

        let task = hypertile_spawn(Some(panicking_work), std::ptr::null_mut());
        assert!(!task.is_null());

        let mut result: *mut c_void = std::ptr::null_mut();
        let status = hypertile_wait(task, &mut result);
        assert_eq!(status, HypertileStatus::ErrPanic as i32);

        hypertile_task_destroy(task);
    }
}

#[test]
fn test_capi_callback_spawning() {
    unsafe {
        let _ = hypertile_init(2);

        struct CbContext {
            called: AtomicBool,
            received_val: AtomicUsize,
        }

        let ctx = Arc::new(CbContext {
            called: AtomicBool::new(false),
            received_val: AtomicUsize::new(0),
        });

        unsafe extern "C-unwind" fn on_done(res: *mut c_void, user_data: *mut c_void) {
            let ctx = &*(user_data as *const CbContext);
            ctx.received_val.store(res as usize, Ordering::Release);
            ctx.called.store(true, Ordering::Release);
        }

        let ctx_ptr = Arc::as_ptr(&ctx) as *mut c_void;
        let status = hypertile_spawn_with_callback(
            Some(double_number),
            50 as *mut c_void,
            Some(on_done),
            ctx_ptr,
        );
        assert_eq!(status, HypertileStatus::Ok as i32);

        for _ in 0..50 {
            if ctx.called.load(Ordering::Acquire) {
                break;
            }
            std::thread::sleep(Duration::from_millis(2));
        }

        assert!(ctx.called.load(Ordering::Acquire));
        assert_eq!(ctx.received_val.load(Ordering::Acquire), 100);
    }
}

#[test]
fn test_capi_batch_spawn() {
    unsafe {
        let _ = hypertile_init(4);

        const COUNT: usize = 128;
        let args: Vec<*mut c_void> = (0..COUNT).map(|i| i as *mut c_void).collect();
        let mut results: Vec<*mut c_void> = vec![std::ptr::null_mut(); COUNT];

        let status = hypertile_batch_spawn(
            Some(double_number),
            args.as_ptr(),
            results.as_mut_ptr(),
            COUNT,
        );
        assert_eq!(status, HypertileStatus::Ok as i32);

        for (i, res) in results.iter().enumerate() {
            assert_eq!(*res as usize, i * 2);
        }
    }
}

#[test]
fn test_capi_worker_registration() {
    unsafe {
        let _ = hypertile_init(2);

        let worker_id = hypertile_register_worker();
        assert!(worker_id > 0);

        let run_res = hypertile_worker_run_until_idle();
        assert_eq!(run_res, HypertileStatus::Ok as i32);

        let dereg_res = hypertile_worker_deregister();
        assert_eq!(dereg_res, HypertileStatus::Ok as i32);
    }
}
