<div align="center">
  <img src="assets/logo.png" width="160" height="160" alt="Hypertile Logo" />
  <h1>Hypertile</h1>
  <p><strong>A unified, work-stealing executor collocating free-threaded Python coroutines and <code>Send</code> Rust futures in one right-sized thread pool.</strong></p>

  <p>
    <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/Rust-2021%20edition-orange.svg" alt="Rust" /></a>
    <a href="include/hypertile.h"><img src="https://img.shields.io/badge/C%2FC%2B%2B-C99%20%7C%20C%2B%2B11-blue.svg" alt="C/C++" /></a>
    <a href="https://www.python.org/"><img src="https://img.shields.io/badge/Python-3.11%E2%80%933.14%20%7C%203.14t-blue.svg" alt="Python" /></a>
    <a href="python/hypertile/py.typed"><img src="https://img.shields.io/badge/typing-PEP%20561-brightgreen.svg" alt="Type Checked" /></a>
    <a href="LICENSE-MIT"><img src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-green.svg" alt="License" /></a>
  </p>
</div>

---

## 1. Installation & Quickstart

```bash
pip install hypertile
```

Free-threaded interpreters (**3.14t**, PEP 779) unlock true parallel bytecode execution;
the package also works in cooperative mode on GIL builds (3.11+). See the
[honest support matrix](#4-gil-vs-free-threaded-support-honest-matrix).

> **Python 3.13t is not supported.** PyO3 dropped free-threaded 3.13 in the same release
> that added 3.14 support, so no single PyO3 version can target both. 3.14t is the CPython
> release that declared free-threading supported, so Hypertile targets that. Users who
> need 3.13t must stay on Hypertile 0.1.1, which pins PyO3 0.23 and therefore cannot
> build for 3.14.

> **Wheels:** pre-built wheels are published for CPython 3.11–3.14 on Linux, macOS and
> Windows. If no wheel matches your interpreter, `pip install` builds from the sdist and
> needs a Rust toolchain — see [Development](#8-development-setup).

```python
import asyncio
import hypertile


def cpu_heavy(payload: bytes, rounds: int = 50) -> bytes:
    # Any blocking, synchronous callable: hashing, compression, JSON, a DB driver...
    return hypertile.native_pipeline_transform(payload, rounds)  # stand-in for real work


async def main() -> None:
    # 1. Offload a blocking callable to the shared pool (no extra OS threads).
    digest = await hypertile.to_thread(cpu_heavy, b"payload", rounds=25)
    print("via to_thread:", digest.hex()[:16])

    # 2. Vectorized batch: one FFI crossing for the whole batch.
    results = await hypertile.gather_to_thread(cpu_heavy, [b"a" * 32, b"b" * 32, b"c" * 32])
    print("batch results:", len(results))

    # 3. Native pipeline: dispatch straight onto the work-stealing queue.
    native = hypertile.spawn_native_pipeline(b"raw-bytes", rounds=100)
    print("native digest:", (await native).hex()[:16])


asyncio.run(main())
```

`asyncio.to_thread` compatibility: `hypertile.to_thread` is a drop-in replacement for
`asyncio.to_thread` that dispatches onto the shared pool instead of a separate
`ThreadPoolExecutor`. Under the hood, `hypertile.run(coro)` installs the pool behind
asyncio for full ecosystem compatibility.

> **Source-comment note:** doc comments refer to sections such as `PRD_v2 §2.1`. That was
> an internal design note; its content is reproduced in
> [Architecture](#3-architecture--core-innovations) below, so no external document is
> needed to work on this codebase.

---

## 2. Executive Summary & Problem Space

Modern high-performance applications combining Python and Rust (such as FastAPI web services, AI/ML inference servers, and distributed ETL pipelines) conventionally run **two completely independent async runtime stacks**:

1. **Python's Async Stack:** Single-threaded `asyncio` event loop driving coroutines, combined with a separate `ThreadPoolExecutor` (allocating 8–32 OS threads) for offloading blocking work.
2. **Rust's Async Stack:** A native multi-threaded runtime (Tokio-style) with its own thread pool sized to available CPU cores.

### The Hidden Bottlenecks of Dual Pools:

* 💥 **Hardware Oversubscription:** Two independent pools sized against hardware cores create $2 \times \text{CPUs}$ active OS threads, resulting in relentless thread parking, context switching, and cache-line invalidation.
* 🐢 **Double-Hop Boundary Latency:** Crossing between Python and Rust requires two scheduling hops:
  $$\text{Native Wake} \longrightarrow \text{Native Tick} \longrightarrow \texttt{call\_soon\_threadsafe} \longrightarrow \texttt{eventfd} \longrightarrow \text{Asyncio Tick} \longrightarrow \text{Python Task}$$
* 🔒 **Zero Capacity Sharing:** Idle interpreter threads cannot assist with stranded native CPU work, and idle native threads cannot step Python coroutines.
* 🛑 **GIL Overhead vs. Free-Threaded Promise:** Standard CPython serializes bytecode execution under the Global Interpreter Lock (GIL). However, with PEP 779 free-threaded Python (`3.14t+`), Python bytecode can execute truly in parallel across multiple OS threads—**if and only if** the executor is designed to step coroutines natively without lock contention.

---

## 3. Architecture & Core Innovations

Hypertile provides **one unified multi-threaded runtime** whose workers are individually either **native-only** (pure Rust) or **bilingual** (interpreter-attached):

```
                     +-----------------------------------------------+
                     |            HYPERTILE GLOBAL POOL              |
                     |   shared injector (MPSC) + per-worker local   |
                     |   Chase-Lev deques (crossbeam-deque)          |
                     +-----------------------------------------------+
                           |                   |                  |
             +-------------+---------+  +------+------+   +------+---------+
             |  Bilingual worker    |  |  Native     |   |  Bilingual     |
             |  (interpreter-attach |  |  worker     |   |  worker        |
             |   on 3.14t+)         |  |  (Rust-only)|   |  (… N workers) |
             +----------------------+  +-------------+   +----------------+
                  |          |                |
        +---------+          |                +------------------+
        | Step Python        |                |  Poll Rust       |
        | coroutines, in     |                |  futures (any    |
        | batches, via       |                |  Send future,    |
        | coro.send()        |                |  never two       |
        | (interpreter       |                |  threads at once)|
        |  mode)             |                |                  |
        +--------------------+                +------------------+
```

### Architectural Highlights:

1. **Single-Hop Continuation Handoff:**
   When a native task finishes on worker $W$, $W$ pushes the awaiting continuation directly onto **its own local Chase-Lev deque**. $W$ resumes it immediately without returning to an event loop or waking another thread (~1.01 µs per hop).
2. **Cache-Line False-Sharing Elimination (`CachePadded`):**
   Hot atomics (`idle_count`) and worker locks (`idle_stack`, `stealers_cache`) are isolated via `crossbeam_utils::CachePadded`, eliminating MESI/MOESI cache-line bouncing across 64-byte (x86/ARM) and 128-byte (Apple Silicon M-series/POWER) lines.
3. **Sub-Nanosecond PRNG (`fast_rand`):**
   Replaced thread-local cryptographic RNG calls with an ultra-fast non-cryptographic SplitMix64 PRNG, executing victim selection in **2–3 single-cycle ALU instructions**.
4. **Inter-Core Batch Work-Stealing (`steal_batch_and_pop`):**
   When stealing from the global injector or peer workers, an idle thread atomically claims **half of the victim's queue in a single transaction**, executing remaining tasks locally out of L1 cache with zero contention.
5. **Hardware-Adaptive Vectorized Batching:**
   Vectorized APIs (`batch_native_pipeline` and `gather_to_thread`) cross the Python $\leftrightarrow$ Rust FFI boundary once per slice and dynamically chunk workloads across physical cores, yielding **>237,000 to >697,000 operations/sec**.
6. **Dynamic Worker Registration & Capacity Sharing:**
   External server threads (e.g. FastAPI / Uvicorn workers) dynamically join Hypertile's work-stealing pool as auxiliary workers while idle and leave cleanly in **2.76 µs / cycle**.
7. **Panic Containment:**
   Native panics are caught via `catch_unwind` and surfaced as `hypertile.PanicInTask` without poisoning mutexes or killing worker threads.

---

## 4. GIL vs. Free-Threaded Support (Honest Matrix)

| Capability | Free-Threaded (`3.14t+`, PEP 779) | Standard GIL Builds (`3.11`–`3.14`) |
|---|---|---|
| **Parallel Python Bytecode Execution** | **Yes** (bilingual workers step simultaneously) | No (bytecode requires GIL) |
| **Native Task Work-Stealing** | **Yes** (all workers) | **Yes** (all workers) |
| **Single-Hop Continuation Handoff** | **Yes** (on any bilingual worker) | **Yes** (on event loop thread) |
| **Vectorized Micro-Batching** | **Yes** (>237,000 req/s) | **Yes** (>226,000 req/s) |
| **Dynamic Worker Registration** | **Yes** (2.76 µs / cycle) | **Yes** (2.76 µs / cycle) |
| **Operating Mode** | **Full Native Work-Stealing** | **Cooperative Mode** |

### Known limitations

* **Level 1 mode is intentionally minimal.** `hypertile.run(coro, level1=True)` drives pure
  coroutines without an event loop and can only await objects exposing
  `add_done_callback`. Anything else raises `TypeError` immediately rather than hanging.
  Use `hypertile.run(coro)` for `asyncio.sleep`, `httpx`, `aiohttp`, and the rest of the
  asyncio ecosystem.
* **The asyncio policy API is deprecated upstream.** Python 3.14 deprecated
  `asyncio.set_event_loop_policy` (removal in 3.16). `hypertile.install()` still works
  there, logs a warning where the API is gone, and the native/offload APIs are unaffected
  because they never depend on the event loop.
* **Bilingual stepping is cooperative, not preemptive.** A Python coroutine that never
  awaits on a free-threaded build will not be descheduled by Hypertile.

---

## 5. API Reference & Developer Ergonomics

### Asynchronous Offloading: `hypertile.to_thread`
Ultra-low-latency drop-in replacement for `asyncio.to_thread()`. Dispatches callables directly to Hypertile's bilingual workers without allocating OS threads or intermediate asyncio futures:

```python
import hypertile

# Dispatch arbitrary Python callable
result = await hypertile.to_thread(crypto_hash, payload, rounds=50)
```

### Vectorized Parallel Dispatch: `hypertile.gather_to_thread`
Vectorized parallel execution across an iterable of inputs. Crosses the FFI boundary once for the entire batch:

```python
# Process a collection of items in parallel across bilingual workers
results = await hypertile.gather_to_thread(process_record, [r1, r2, r3, r4])
```

### Function Decorator: `@hypertile.task`
Converts synchronous functions into awaitable Hypertile tasks:

```python
@hypertile.task
def verify_token(raw_jwt: str) -> dict:
    return jwt.decode(raw_jwt, key, algorithms=["RS256"])


# Inside an async endpoint:
claims = await verify_token(header_auth)
```

### Native Pipelines: `hypertile.spawn_native_pipeline`
Route CPU-intensive numerical and cryptographic transforms directly onto the native Rust work-stealing queue with single-hop continuation:

```python
native_task = hypertile.spawn_native_pipeline(raw_bytes, rounds=100)
digest = await native_task
```

### High-Throughput Batch Pipeline: `hypertile.batch_native_pipeline`
Submits a batch of payloads directly into the native work-stealing engine in a single FFI crossing:

```python
# Dispatches thousands of payloads with sub-5µs amortized latency
digests = await hypertile.batch_native_pipeline(payload_chunks, rounds=100)
```

### Dynamic Worker Registration
Join the work-stealing pool from long-running server threads (e.g., FastAPI lifespan):

```python
with hypertile.register_worker(kind="bilingual") as worker:
    # Assist the executor while waiting for requests
    worker.run_until_idle()
```

### Pool Sizing: `hypertile.configure` & `HYPERTILE_WORKERS`

A single process-wide pool backs every Hypertile call. Its size is fixed once the pool has
been used, so configure it first - or set an environment variable, which also covers pools
started by code that runs before yours.

```python
import hypertile

hypertile.configure(workers=32)  # I/O-bound service: more threads than cores
assert hypertile.worker_count() == 32
```

```bash
HYPERTILE_WORKERS=32 python app.py        # same thing, resolvable before import
```

`hypertile.configure()` with no argument (or `HYPERTILE_WORKERS=0`) restores the default,
and `hypertile.default_worker_count()` reports what that is on the current machine.
Calling `configure()` after the pool has started raises `RuntimeError` naming the size in
use, rather than silently ignoring the request.

**Why the default is not just the CPU count.** CPU-bound and blocking work want opposite
thread counts, and they share one pool:

| Workload | Wants | Why |
|---|---|---|
| CPU-bound | around one thread per logical CPU | extra threads contend for the same execution units |
| Blocking (`to_thread` around a sync driver, `time.sleep`, file I/O) | **more** threads than CPUs | a blocked thread consumes no CPU, so a CPU-sized pool just queues |

The second row is the one a CPU-sized pool gets wrong. Measured on a 4-core/8-thread
machine, 24 blocking tasks of 100 ms each:

| Pool size | Wall time |
|---|---|
| 8 (CPU count) | 0.31 s |
| 12 (the default) | **0.21 s** |

That matches `asyncio.to_thread()`'s own default executor, which sizes itself to
`min(32, cores + 4)` for the same reason. The compute side pays nothing measurable: with
the same 24 CPU-bound tasks the default and a CPU-sized pool finish together (0.95-1.00 s
vs 0.95-0.97 s).

> Measured on a 15 W mobile CPU. Under sustained load that part throttles to a fixed
> power ceiling, at which point both pool sizes converge and the comparison stops being
> meaningful - so compare pool sizes on short workloads, or on hardware without a tight
> power budget.

So the default is the CPU count **plus a small blocking headroom**, capped at 32 and never
below one worker per CPU. Compute-bound applications can ask for exactly the CPU count;
I/O-heavy services should ask for considerably more.

### Cooperative Cancellation: `CancellationToken`
Propagate cooperative cancellation across bilingual and native workers:

```python
token = hypertile.CancellationToken()
token.cancel()
assert token.is_cancelled()
```

---

## 6. C / C++ Embeddable ABI (`hypertile-capi` & `include/hypertile.h`)

For applications written in **C, C++, Go (cgo), or Zig**, Hypertile provides a lightweight, zero-overhead `extern "C"` ABI via the `hypertile-capi` crate and [`include/hypertile.h`](include/hypertile.h).

No heavy C++ framework is imposed; the C ABI maps directly to Hypertile's Chase-Lev deques and work-stealing pool without intermediate runtime overhead.

### Building the C ABI Libraries
```bash
# Build release dynamic (.dll/.so/.dylib) and static (.lib/.a) libraries
cargo build --release -p hypertile-capi
```
Artifacts are generated in `target/release/`:
* Dynamic Library: `hypertile_capi.dll` (Windows) / `libhypertile_capi.so` (Linux) / `libhypertile_capi.dylib` (macOS)
* Static Library: `hypertile_capi.lib` (MSVC) / `libhypertile_capi.a` (GCC/Clang)

### Core C API Functions:
* `hypertile_init(size_t num_workers)`: Initialize global pool (pass 0 for CPU core count; the first call fixes the worker count).
* `hypertile_spawn(work, arg)`: Asynchronously dispatch a task; returns an opaque `hypertile_task_t*`.
* `hypertile_wait(task, &out_result)`: Efficiently park the calling thread until task completes.
* `hypertile_poll(task, &out_result)`: Non-blocking completion poll (`0` ready, `1` pending).
* `hypertile_task_destroy(task)`: Free task handle (safe before or after completion).
* `hypertile_spawn_with_callback(work, arg, callback, user_data)`: Fire-and-forget task with completion callback.
* `hypertile_batch_spawn(work, args, out_results, count)`: Vectorized parallel batch execution (>5,000,000 items/sec).
* `hypertile_register_worker()`: Dynamically join the work-stealing pool from external C/C++ threads. Returns a non-zero worker ID, or `0` on failure.
* `hypertile_shutdown()`: Cleanly drain and stop worker threads.

The ABI reports panics instead of unwinding across the boundary: a panicking work function
is caught and surfaced as `HYPERTILE_ERR_PANIC`. See [SECURITY.md](SECURITY.md) for the
caller's obligations (argument lifetime, thread-safety, and shutdown ordering).

### C Example
```c
#include <stdio.h>
#include <stdint.h>
#include "hypertile.h"

void* compute(void* arg) {
    uintptr_t x = (uintptr_t)arg;
    return (void*)(x * 2);
}

int main(void) {
    hypertile_init(0); // auto CPU cores

    // 1. Single Task Spawning
    hypertile_task_t* task = hypertile_spawn(compute, (void*)21);
    void* result = NULL;
    hypertile_wait(task, &result);
    printf("Result: %zu\n", (uintptr_t)result); // 42
    hypertile_task_destroy(task);

    // 2. Vectorized Parallel Batch (10,000 items)
    const size_t COUNT = 10000;
    void* args[COUNT];
    void* results[COUNT];
    for (size_t i = 0; i < COUNT; ++i) args[i] = (void*)(uintptr_t)i;
    hypertile_batch_spawn(compute, args, results, COUNT);

    hypertile_shutdown();
    return 0;
}
```

### Compiling and Linking:
```bash
# GCC / Clang (Dynamic link)
gcc -O3 -I include main.c -L target/release -lhypertile_capi -o app

# MSVC (cl.exe)
cl /O2 /I include main.c target\release\hypertile_capi.dll.lib
```
A complete executable verification program is located at [`examples/c/main.c`](examples/c/main.c).

---

## 7. Benchmarks & Empirical Performance

All benchmarks were measured on Windows AMD64 (8 physical cores / 16 threads) comparing identical cryptographic/numerical workloads. Re-measure locally before drawing conclusions — see [CONTRIBUTING.md](CONTRIBUTING.md#benchmarking).

### A. Free-Threaded (No-GIL) Benchmark:

These figures were recorded on Python 3.13t. The script still runs unchanged on 3.14t —
the interpreter that Hypertile currently builds for — but the numbers below have not been
re-measured on it, so treat them as indicative rather than as a 3.14t result.

```bash
python showcase/free_threaded_showcase.py
```

| Metric | Standard ThreadPool (Baseline) | Hypertile Direct (Scalar) | Hypertile Vector (Batch) | Speedup vs Baseline |
|---|---|---|---|---|
| **Throughput (req/s)** | 10,747 req/s | **19,340 req/s** | **237,270 req/s** | **1.80x (scalar) / 22.1x (vector)** |
| **Wall Time (10,000 reqs)** | 0.930 s | **0.517 s** | **0.042 s** | **-44.4% (scalar) / -95.5% (vector)** |
| **Median Latency (p50)** | 17.54 ms | **8.21 ms** | **4.21 µs / item** | **-53.2% latency reduction** |
| **Tail Latency (p95)** | 24.08 ms | **11.23 ms** | **4.21 µs / item** | **-53.4% tail latency reduction** |
| **Tail Latency (p99)** | 74.17 ms | **56.39 ms** | **4.21 µs / item** | **-24.0% tail latency reduction** |

### B. Standard GIL (Python 3.11) Honest Benchmark:
```bash
python showcase/showcase_benchmark.py
```

| Metric | Standard asyncio (Baseline) | Hypertile L2 Hook | Hypertile Vector Batch | Speedup vs Baseline |
|---|---|---|---|---|
| **Cross-Thread Offload Latency** | 115.41 µs | **113.22 µs** | N/A | **1.02x faster offload** |
| **Pipeline Throughput** | 9,653 req/s | 8,295 req/s | **226,924 req/s** | **23.5x throughput multiplier** |
| **Median Latency (p50)** | 21.68 ms | 25.62 ms | **4.41 µs / item** | **Sub-5µs per item** |
| **Dynamic Worker Cycle** | N/A | **2.76 µs / cycle** | N/A | **Sub-3µs thread registration** |

---

## 8. Development Setup

Prerequisites: Rust stable (`rust-version = 1.79` floor), Python 3.11+, and
[`maturin`](https://maturin.rs) `>= 1.8`. [`uv`](https://github.com/astral-sh/uv) is a
convenient way to get a free-threaded interpreter.

Any interpreter layout works — the commands below use `python` to mean your venv's
interpreter (`.venv/bin/python` on Linux/macOS, `.venv\Scripts\python.exe` on Windows).

```bash
# 1. A free-threaded environment (recommended for scheduler work)
uv python install 3.14t
uv venv --python 3.14t .venv-314t

# 2. Tooling
python -m pip install --upgrade "maturin>=1.8,<2.0" pytest ruff

# 3. Build the extension in place
maturin develop --release
```

On Windows, `maturin develop` needs the venv to be discoverable:

```powershell
$env:VIRTUAL_ENV = "$PWD\.venv-314t"
maturin develop --release
```

Run the same gates CI runs (see [CONTRIBUTING.md](CONTRIBUTING.md) for details):

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test -p hypertile-core -p hypertile-capi --locked
ruff check .
pytest -v tests/
```

> Rebuild (`maturin develop --release`) after any Rust change, otherwise `pytest` runs
> against a stale extension binary.

Scheduler, task-lifecycle and worker-registry changes are also covered by a seeded
concurrency harness (`hypertile-core/tests/race_harness.rs`) that replays any failure from
its seed. Sweep it across many seeds at high iteration counts when hunting:

```bash
scripts/race_stress.sh            # debug: keeps overflow checks on
scripts/race_stress.sh release    # faster, for long sweeps
```

ThreadSanitizer and AddressSanitizer runs live in the scheduled
[`Sanitizers`](.github/workflows/sanitizers.yml) workflow; see
[CONTRIBUTING.md](CONTRIBUTING.md#the-race-harness) for the manual commands.

---

## 9. Production Examples

Complete, executable production examples are provided in [`examples/`](examples/):

1. **C / C++ Embeddable Driver ([`examples/c/main.c`](examples/c/main.c)):**
   Zero-overhead C API driver testing single tasks, polling, callbacks, vectorized batch execution, and worker registration.
2. **FastAPI Microservice ([`examples/fastapi_service.py`](examples/fastapi_service.py)):**
   Lifespan worker registration, `@hypertile.task` offloading, and native pipeline endpoints.
   ```bash
   python examples/fastapi_service.py
   ```
3. **Batch Data Pipeline ([`examples/data_pipeline.py`](examples/data_pipeline.py)):**
   Multi-stage ETL pipeline, cooperative cancellation tokens, dynamic worker scaling, and vectorized native batches.
   ```bash
   python examples/data_pipeline.py
   ```

---

## 10. Multi-OS & Hardware Compatibility Matrix

| Platform | Architecture | Tier | Verification Status |
|---|---|---|---|
| **Windows** | x86_64, aarch64 | **Tier 1** | Verified with MSVC toolchain, `WaitOnAddress`, keyed events, PyO3 `.pyd`, C ABI `.dll`/`.lib`. |
| **Linux** | x86_64, aarch64 | **Tier 1** | POSIX threads, standard futexes, C ABI `.so`/`.a`, multi-OS GitHub Actions CI workflow enabled. |
| **macOS** | x86_64, aarch64 (Apple Silicon) | **Tier 1** | Pthread primitives, Mach monotonic timing, 128B Apple Silicon cache alignment, C ABI `.dylib`/`.a`. |

CI exercises GIL (3.11–3.14) and free-threaded (3.14t) interpreters on all three
platforms, plus the C ABI test suite.

---

## 11. Contributing, Security & License

- **[CONTRIBUTING.md](CONTRIBUTING.md)** — build, test, lint and PR expectations.
- **[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)** — this project follows the Contributor Covenant.
- **[SECURITY.md](SECURITY.md)** — how to report vulnerabilities privately, and the threat model.
- **[CHANGELOG.md](CHANGELOG.md)** — notable changes per release.
- **Issue templates** are available under [`.github/ISSUE_TEMPLATE`](.github/ISSUE_TEMPLATE).

### License

Dual-licensed under either of:
* Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
* MIT license ([LICENSE-MIT](LICENSE-MIT))

Unless you explicitly state otherwise, any contribution intentionally submitted for
inclusion in this project by you shall be dual-licensed as above, without any additional
terms or conditions.
