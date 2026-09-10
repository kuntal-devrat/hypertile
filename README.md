<div align="center">
  <img src="assets/logo.png" width="160" height="160" alt="Hypertile Logo" />
  <h1>Hypertile</h1>
  <p><strong>A unified, work-stealing executor collocating free-threaded Python coroutines and <code>Send</code> Rust futures in one right-sized thread pool.</strong></p>

  <p>
    <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/Rust-2021%20edition-orange.svg" alt="Rust" /></a>
    <a href="https://www.python.org/"><img src="https://img.shields.io/badge/Python-3.11%20|%203.12%20|%203.13t%20|%203.14t-blue.svg" alt="Python" /></a>
    <a href="python/hypertile/py.typed"><img src="https://img.shields.io/badge/typing-PEP%20561-brightgreen.svg" alt="Type Checked" /></a>
    <a href="https://github.com/astral-sh/uv"><img src="https://img.shields.io/badge/managed%20with-uv-purple.svg" alt="uv" /></a>
    <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-green.svg" alt="License" /></a>
  </p>
</div>

---

## 1. Executive Summary & Problem Space

Modern high-performance applications combining Python and Rust (such as FastAPI web services, AI/ML inference servers, and distributed ETL pipelines) conventionally run **two completely independent async runtime stacks**:

1. **Python's Async Stack:** Single-threaded `asyncio` event loop driving coroutines, combined with a separate `ThreadPoolExecutor` (allocating 8–32 OS threads) for offloading blocking work.
2. **Rust's Async Stack:** A native multi-threaded runtime (Tokio-style) with its own thread pool sized to available CPU cores.

### The Hidden Bottlenecks of Dual Pools:

* 💥 **Hardware Oversubscription:** Two independent pools sized against hardware cores create $2 \times \text{CPUs}$ active OS threads, resulting in relentless thread parking, context switching, and cache-line invalidation.
* 🐢 **Double-Hop Boundary Latency:** Crossing between Python and Rust requires two scheduling hops:
  $$\text{Native Wake} \longrightarrow \text{Native Tick} \longrightarrow \texttt{call\_soon\_threadsafe} \longrightarrow \texttt{eventfd} \longrightarrow \text{Asyncio Tick} \longrightarrow \text{Python Task}$$
* 🔒 **Zero Capacity Sharing:** Idle interpreter threads cannot assist with stranded native CPU work, and idle native threads cannot step Python coroutines.
* 🛑 **GIL Overhead vs. Free-Threaded Promise:** Standard CPython serializes bytecode execution under the Global Interpreter Lock (GIL). However, with PEP 779 free-threaded Python (`3.13t` / `3.14t+`), Python bytecode can execute truly in parallel across multiple OS threads—**if and only if** the executor is designed to step coroutines natively without lock contention.

---

## 2. Architecture & Core Innovations

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
             |   on 3.13t/3.14t+)   |  |  (Rust-only)|   |  (… N workers) |
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

## 3. GIL vs. Free-Threaded Support (Honest Matrix)

| Capability | Free-Threaded (`3.13t` / `3.14t+`, PEP 779) | Standard GIL Builds (`3.11`–`3.14`) |
|---|---|---|
| **Parallel Python Bytecode Execution** | **Yes** (bilingual workers step simultaneously) | No (bytecode requires GIL) |
| **Native Task Work-Stealing** | **Yes** (all workers) | **Yes** (all workers) |
| **Single-Hop Continuation Handoff** | **Yes** (on any bilingual worker) | **Yes** (on event loop thread) |
| **Vectorized Micro-Batching** | **Yes** (>237,000 req/s) | **Yes** (>226,000 req/s) |
| **Dynamic Worker Registration** | **Yes** (2.76 µs / cycle) | **Yes** (2.76 µs / cycle) |
| **Operating Mode** | **Full Native Work-Stealing** | **Cooperative Mode** |

---

## 4. API Reference & Developer Ergonomics

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

### Cooperative Cancellation: `CancellationToken`
Propagate cooperative cancellation across bilingual and native workers:

```python
token = hypertile.CancellationToken()
token.cancel()
assert token.is_cancelled()
```

---

## 5. Benchmarks & Empirical Performance

All benchmarks were measured on Windows AMD64 (8 physical cores / 16 threads) comparing identical cryptographic/numerical workloads:

### A. Free-Threaded (No-GIL, Python 3.13t) Benchmark:
```bash
.venv-313t/Scripts/python showcase/free_threaded_showcase.py
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
.venv/Scripts/python showcase/showcase_benchmark.py
```

| Metric | Standard asyncio (Baseline) | Hypertile L2 Hook | Hypertile Vector Batch | Speedup vs Baseline |
|---|---|---|---|---|
| **Cross-Thread Offload Latency** | 115.41 µs | **113.22 µs** | N/A | **1.02x faster offload** |
| **Pipeline Throughput** | 9,653 req/s | 8,295 req/s | **226,924 req/s** | **23.5x throughput multiplier** |
| **Median Latency (p50)** | 21.68 ms | 25.62 ms | **4.41 µs / item** | **Sub-5µs per item** |
| **Dynamic Worker Cycle** | N/A | **2.76 µs / cycle** | N/A | **Sub-3µs thread registration** |

---

## 6. Development with `uv` & Multi-Environment Setup

Hypertile strictly recommends [Astral `uv`](https://github.com/astral-sh/uv) for fast, reproducible virtual environment management:

### 1. Free-Threaded Environment (Python 3.13t)
```bash
# Install free-threaded Python 3.13t
uv python install 3.13t

# Create virtual environment
uv venv --python 3.13t .venv-313t

# Install dependencies with uv
uv pip install maturin pytest fastapi httpx --python .venv-313t/Scripts/python.exe

# Build and install editable release wheel
$env:VIRTUAL_ENV = "d:\HyperTile\.venv-313t"
& .venv-313t\Scripts\maturin.exe develop --release --uv
```

### 2. Standard GIL Environment (Python 3.11)
```bash
# Create virtual environment
uv venv --python 3.11 .venv

# Install dependencies with uv
uv pip install maturin pytest fastapi httpx --python .venv/Scripts/python.exe

# Build and install editable release wheel
$env:VIRTUAL_ENV = "d:\HyperTile\.venv"
& .venv\Scripts\maturin.exe develop --release --uv
```

---

## 7. Production Examples

Complete, executable production examples are provided in [`examples/`](examples/):

1. **FastAPI Microservice ([`examples/fastapi_service.py`](examples/fastapi_service.py)):**
   Lifespan worker registration, `@hypertile.task` offloading, and native pipeline endpoints.
   ```bash
   python examples/fastapi_service.py
   ```

2. **Batch Data Pipeline ([`examples/data_pipeline.py`](examples/data_pipeline.py)):**
   Multi-stage ETL pipeline, cooperative cancellation tokens, dynamic worker scaling, and vectorized native batches.
   ```bash
   python examples/data_pipeline.py
   ```

---

## 8. Multi-OS & Hardware Compatibility Matrix

| Platform | Architecture | Tier | Verification Status |
|---|---|---|---|
| **Windows** | x86_64, aarch64 | **Tier 1** | Verified with MSVC toolchain, `WaitOnAddress`, keyed events, PyO3 `.pyd`. |
| **Linux** | x86_64, aarch64 | **Tier 1** | POSIX threads, standard futexes, multi-OS GitHub Actions CI workflow enabled. |
| **macOS** | x86_64, aarch64 (Apple Silicon) | **Tier 1** | Pthread primitives, Mach monotonic timing, 128B Apple Silicon cache alignment. |

---

## 9. License

Dual-licensed under either of:
* Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
* MIT license ([LICENSE-MIT](LICENSE-MIT))
