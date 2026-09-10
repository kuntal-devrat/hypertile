/**
 * @file hypertile.h
 * @brief Zero-overhead C/C++ ABI for the Hypertile work-stealing runtime.
 *
 * Enables C, C++, Go (cgo), and Zig applications to embed Hypertile's
 * high-performance bilingual work-stealing thread pool without runtime overhead.
 *
 * @copyright Copyright (c) 2026 Hypertile Contributors
 * @license MIT OR Apache-2.0
 */

#ifndef HYPERTILE_H
#define HYPERTILE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Status codes returned by Hypertile C API functions.
 */
typedef enum hypertile_status {
    /** Operation completed successfully. */
    HYPERTILE_OK = 0,
    /** Task is still executing and not yet ready. */
    HYPERTILE_PENDING = 1,
    /** Invalid argument provided (e.g., NULL pointer). */
    HYPERTILE_ERR_INVALID_ARG = -1,
    /** Work function panicked during execution. */
    HYPERTILE_ERR_PANIC = -2,
    /** Hypertile runtime was not initialized or shut down. */
    HYPERTILE_ERR_NOT_INITIALIZED = -3,
    /** No registered worker on current thread. */
    HYPERTILE_ERR_WORKER_NOT_REGISTERED = -4
} hypertile_status_t;

/**
 * @brief Opaque handle representing an in-flight or completed Hypertile task.
 */
typedef struct hypertile_task hypertile_task_t;

/**
 * @brief Function pointer signature for work dispatched to Hypertile workers.
 *
 * @param arg Opaque user-provided argument pointer passed into hypertile_spawn.
 * @return Opaque user-provided result pointer retrieved via hypertile_wait/hypertile_poll.
 */
typedef void* (*hypertile_work_fn)(void* arg);

/**
 * @brief Function pointer signature for task completion callbacks.
 *
 * @param result Return value from the completed work function.
 * @param user_data User data pointer passed into hypertile_spawn_with_callback.
 */
typedef void (*hypertile_callback_fn)(void* result, void* user_data);

/* -------------------------------------------------------------------------
 * Runtime Lifecycle
 * ------------------------------------------------------------------------- */

/**
 * @brief Initialize Hypertile's global work-stealing runtime.
 *
 * @param num_workers Number of native worker threads to spawn. Pass 0 to default
 *                    to the number of logical CPU cores.
 * @return HYPERTILE_OK (0) on success, or an error status code.
 */
int32_t hypertile_init(size_t num_workers);

/**
 * @brief Shut down the Hypertile global runtime and drain worker threads.
 *
 * @return HYPERTILE_OK (0) on success.
 */
int32_t hypertile_shutdown(void);

/**
 * @brief Return a null-terminated version string for the Hypertile C ABI.
 */
const char* hypertile_version(void);

/* -------------------------------------------------------------------------
 * Task Spawning & Synchronization
 * ------------------------------------------------------------------------- */

/**
 * @brief Spawn a work function onto Hypertile's work-stealing pool.
 *
 * @param work Function to execute on a worker thread.
 * @param arg User data passed directly to the work function.
 * @return Opaque task handle, or NULL on allocation/argument failure.
 *         Must eventually be freed with hypertile_task_destroy().
 */
hypertile_task_t* hypertile_spawn(hypertile_work_fn work, void* arg);

/**
 * @brief Poll a task for completion without blocking.
 *
 * @param task Opaque task handle returned from hypertile_spawn().
 * @param[out] out_result Pointer to store the task return value upon completion.
 *                        Can be NULL if the caller does not need the result.
 * @return HYPERTILE_OK (0) if ready and out_result written,
 *         HYPERTILE_PENDING (1) if still running,
 *         or a negative error status code on failure/panic.
 */
int32_t hypertile_poll(hypertile_task_t* task, void** out_result);

/**
 * @brief Block the calling thread until the task completes.
 *
 * Efficiently parks the calling thread on an OS event/unparker until the
 * worker thread completes execution.
 *
 * @param task Opaque task handle returned from hypertile_spawn().
 * @param[out] out_result Pointer to store the task return value upon completion.
 *                        Can be NULL if the caller does not need the result.
 * @return HYPERTILE_OK (0) on success, or a negative error code on panic/failure.
 */
int32_t hypertile_wait(hypertile_task_t* task, void** out_result);

/**
 * @brief Destroy and free a task handle.
 *
 * Decrements the task handle's reference count. Safe to call before or after
 * task completion.
 *
 * @param task Task handle to destroy. Passing NULL is a safe no-op.
 */
void hypertile_task_destroy(hypertile_task_t* task);

/**
 * @brief Spawn an asynchronous task with a completion callback (fire-and-forget).
 *
 * When `work(arg)` completes, `callback(result, user_data)` is automatically
 * invoked on a worker thread. Does not allocate a task handle.
 *
 * @param work Function to execute.
 * @param arg Argument passed to work function.
 * @param callback Function invoked when work completes.
 * @param user_data Argument passed to callback function.
 * @return HYPERTILE_OK (0) on success, or negative error code.
 */
int32_t hypertile_spawn_with_callback(
    hypertile_work_fn work,
    void* arg,
    hypertile_callback_fn callback,
    void* user_data
);

/**
 * @brief Execute a vectorized parallel batch of tasks across worker threads.
 *
 * Distributes `count` items evenly across worker threads with a single barrier
 * wait. Blocks until all items complete.
 *
 * @param work Function to execute for each item.
 * @param args Array of `count` argument pointers.
 * @param[out] out_results Array of `count` pointers where results will be written.
 * @param count Number of items in the batch.
 * @return HYPERTILE_OK (0) on success, or negative error code.
 */
int32_t hypertile_batch_spawn(
    hypertile_work_fn work,
    void** args,
    void** out_results,
    size_t count
);

/* -------------------------------------------------------------------------
 * Dynamic Worker Registration (PRD §2.3)
 * ------------------------------------------------------------------------- */

/**
 * @brief Register the calling external thread as a worker in Hypertile's pool.
 *
 * Enables external threads (e.g. C++ thread pool threads, game engine fibers)
 * to participate in work-stealing.
 *
 * @return Unique worker ID allocated to the calling thread.
 */
uint64_t hypertile_register_worker(void);

/**
 * @brief Execute tasks on the registered calling thread until the pool is idle.
 *
 * @return HYPERTILE_OK (0) on success, or negative error code if thread is not registered.
 */
int32_t hypertile_worker_run_until_idle(void);

/**
 * @brief Deregister the calling thread from the worker pool.
 *
 * @return HYPERTILE_OK (0) on success, or negative error code if thread was not registered.
 */
int32_t hypertile_worker_deregister(void);

#ifdef __cplusplus
}
#endif

#endif /* HYPERTILE_H */
