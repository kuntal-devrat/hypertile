/**
 * @file main.c
 * @brief Hypertile C ABI Demonstration & Verification Program.
 *
 * Demonstrates:
 * 1. Runtime initialization (hypertile_init)
 * 2. Asynchronous task spawning & waiting (hypertile_spawn, hypertile_wait)
 * 3. Non-blocking polling (hypertile_poll)
 * 4. Completion callbacks (hypertile_spawn_with_callback)
 * 5. High-throughput vectorized batch processing (hypertile_batch_spawn)
 * 6. Dynamic worker registration from external threads (hypertile_register_worker)
 * 7. Clean runtime shutdown (hypertile_shutdown)
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <time.h>

#ifdef _WIN32
#include <windows.h>
#define sleep_ms(ms) Sleep(ms)
#else
#include <unistd.h>
#define sleep_ms(ms) usleep((ms) * 1000)
#endif

#include "../../include/hypertile.h"

/* Simple compute workload */
static void* compute_work(void* arg) {
    uintptr_t val = (uintptr_t)arg;
    uint64_t acc = val;
    for (int i = 0; i < 500; ++i) {
        acc = (acc ^ (uint64_t)i) * 0x517cc1b727220a95ULL;
    }
    return (void*)(uintptr_t)acc;
}

/* Callback structure for completion notification */
typedef struct {
    volatile bool completed;
    void* result;
} callback_context_t;

static void on_task_complete(void* result, void* user_data) {
    callback_context_t* ctx = (callback_context_t*)user_data;
    ctx->result = result;
    ctx->completed = true;
}

int main(void) {
    printf("========================================================================\n");
    printf("         HYPERTILE C/C++ ABI EMBEDDED DEMONSTRATION\n");
    printf("         Version: %s\n", hypertile_version());
    printf("========================================================================\n\n");

    /* 1. Initialize Hypertile runtime */
    printf("[Step 1] Initializing Hypertile global runtime (auto core count)...\n");
    if (hypertile_init(0) != HYPERTILE_OK) {
        fprintf(stderr, "Failed to initialize Hypertile runtime\n");
        return 1;
    }
    printf("  -> Hypertile runtime initialized successfully.\n\n");

    /* 2. Spawn and block on a single task */
    printf("[Step 2] Spawning task to compute transform...\n");
    hypertile_task_t* task = hypertile_spawn(compute_work, (void*)(uintptr_t)12345);
    if (!task) {
        fprintf(stderr, "Failed to spawn task\n");
        return 1;
    }

    void* result = NULL;
    int32_t status = hypertile_wait(task, &result);
    if (status != HYPERTILE_OK) {
        fprintf(stderr, "Task execution failed with status %d\n", status);
        return 1;
    }
    printf("  -> Task completed with result: 0x%llx\n", (unsigned long long)(uintptr_t)result);
    hypertile_task_destroy(task);
    printf("  -> Task handle destroyed.\n\n");

    /* 3. Non-blocking polling */
    printf("[Step 3] Spawning task and polling with hypertile_poll()...\n");
    hypertile_task_t* poll_task = hypertile_spawn(compute_work, (void*)(uintptr_t)9999);
    void* poll_res = NULL;
    int poll_count = 0;
    while (hypertile_poll(poll_task, &poll_res) == HYPERTILE_PENDING) {
        poll_count++;
        sleep_ms(1);
    }
    printf("  -> Task completed after %d poll iterations with result: 0x%llx\n",
           poll_count, (unsigned long long)(uintptr_t)poll_res);
    hypertile_task_destroy(poll_task);
    printf("\n");

    /* 4. Completion callback (fire-and-forget) */
    printf("[Step 4] Asynchronous execution with completion callback...\n");
    callback_context_t cb_ctx = { .completed = false, .result = NULL };
    hypertile_spawn_with_callback(compute_work, (void*)(uintptr_t)777, on_task_complete, &cb_ctx);

    while (!cb_ctx.completed) {
        sleep_ms(1);
    }
    printf("  -> Callback triggered! Result: 0x%llx\n\n", (unsigned long long)(uintptr_t)cb_ctx.result);

    /* 5. Vectorized parallel batch execution */
    const size_t BATCH_SIZE = 10000;
    printf("[Step 5] Executing vectorized parallel batch (%zu items)...\n", BATCH_SIZE);
    void** args = (void**)malloc(BATCH_SIZE * sizeof(void*));
    void** results = (void**)malloc(BATCH_SIZE * sizeof(void*));

    for (size_t i = 0; i < BATCH_SIZE; ++i) {
        args[i] = (void*)(uintptr_t)(i + 1);
    }

    clock_t start = clock();
    status = hypertile_batch_spawn(compute_work, args, results, BATCH_SIZE);
    clock_t end = clock();

    if (status != HYPERTILE_OK) {
        fprintf(stderr, "Batch spawn failed\n");
        return 1;
    }

    double elapsed_ms = ((double)(end - start) / CLOCKS_PER_SEC) * 1000.0;
    double throughput = (double)BATCH_SIZE / (elapsed_ms / 1000.0);
    printf("  -> Processed %zu items in %.2f ms\n", BATCH_SIZE, elapsed_ms);
    printf("  -> Throughput: %.0f items/sec\n", throughput);

    free(args);
    free(results);
    printf("\n");

    /* 6. Dynamic worker registration from external thread */
    printf("[Step 6] Dynamically registering calling thread into work-stealing pool...\n");
    uint64_t worker_id = hypertile_register_worker();
    printf("  -> Joined pool as Worker ID: %llu\n", (unsigned long long)worker_id);

    hypertile_worker_run_until_idle();
    printf("  -> Ran tasks until pool idle.\n");

    hypertile_worker_deregister();
    printf("  -> Calling thread gracefully deregistered.\n\n");

    /* 7. Shutdown */
    printf("[Step 7] Shutting down Hypertile runtime...\n");
    hypertile_shutdown();
    printf("  -> Hypertile runtime drained and stopped.\n\n");

    printf("========================================================================\n");
    printf("All C ABI verification steps completed successfully!\n");
    printf("========================================================================\n");
    return 0;
}
