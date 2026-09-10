# Hypertile

**A shared, work-stealing executor that lets free-threaded Python coroutines and `Send` Rust futures colocate in one right-sized thread pool.**

[![Rust](https://img.shields.io/badge/Rust-2021%20edition-orange.svg)](https://www.rust-lang.org/)
[![Python](https://img.shields.io/badge/Python-3.11%20|%203.12%20|%203.13t%20|%203.14t-blue.svg)](https://www.python.org/)
[![Type Checked](https://img.shields.io/badge/typing-PEP%20561-brightgreen.svg)](python/hypertile/py.typed)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-green.svg)](LICENSE)

---

## 1. The Problem

Modern applications collocating CPython and Rust (e.g. PyO3 / FastAPI / data pipelines) run two completely independent async stacks:
1. Python's `asyncio` event loop thread pool, and
2. A native Rust runtime (Tokio-style) with its own thread pool.

When both are active:
- **Oversubscription:** Two pools independently sized against CPU cores, causing redundant thread parking and CPU cache thrashing.
- **Double-Hop Boundary Latency:** Every await crossing the language boundary requires two scheduling hops:
  *Native wake &rarr; Native tick &rarr; `call_soon_threadsafe` &rarr; `eventfd` wakes Python loop &rarr; Python loop schedules Task.*
- **No Capacity Sharing:** Idle interpreter-capable threads cannot pick up stranded native work, and vice-versa.

---

## 2. The Solution

Hypertile provides **one unified multi-threaded executor** whose workers are individually either **native-only** or **bilingual** (interpreter-attached):

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

### Core Innovations:
- **Single-Hop Continuation Handoff:** When a task completes on worker W, W pushes the awaiting continuation directly onto **its own local Chase-Lev queue**. W resumes it immediately without returning to any other scheduler.
- **Dynamic Worker Registration:** Existing server threads (e.g. FastAPI/uvicorn worker threads) dynamically join the pool while idle and leave cleanly.
- **Panic Containment:** Panics inside native tasks are safely caught via `catch_unwind` and propagated as `hypertile.PanicInTask` to the awaiting continuation rather than terminating the worker pool.
- **Self-Contained Engine:** Built directly on Chase-Lev lock-free deques and atomic wakers; does not require or wrap Tokio.

---

## 3. GIL vs. Free-Threaded Support (Honest Matrix)

| Feature | Free-Threaded (`3.13t` / `3.14t+`, PEP 779) | Standard GIL Builds (`3.11`–`3.14`) |
|---|---|---|
| Parallel Python Stepping | **Yes** (bilingual workers step across threads) | No (bytecode execution requires GIL) |
| Native Work Stealing | **Yes** (all workers) | **Yes** (all workers) |
| Single-Hop Handoff | **Yes** (on any bilingual worker) | **Yes** (on loop thread) |
| Dynamic Worker Registration | **Yes** (12–20 µs) | **Yes** (12–20 µs) |
| Fallback Mode | Full Feature Set | **Cooperative Mode** |

---

## 4. Developer Ergonomics & API

### Asynchronous Offloading: `hypertile.to_thread`
Ultra-low-latency replacement for `asyncio.to_thread()`. Dispatches callables directly to Hypertile's work-stealing pool without allocating OS threads or intermediate asyncio futures:

```python
import hypertile

# Dispatch arbitrary Python callable
result = await hypertile.to_thread(heavy_hash, payload, rounds=50)
```

### Function Decorator: `@hypertile.task`
Convert synchronous functions into awaitable Hypertile tasks executed across bilingual workers:

```python
@hypertile.task
def verify_token(raw_jwt: str) -> dict:
    return jwt.decode(raw_jwt, key, algorithms=["RS256"])

# Inside an async endpoint:
claims = await verify_token(header_auth)
```

### Direct Native Pipelines: `hypertile.spawn_native_pipeline`
Route CPU-intensive numerical and cryptographic transforms directly onto the native Rust work-stealing queue with single-hop continuation:

```python
native_task = hypertile.spawn_native_pipeline(raw_bytes, rounds=100)
digest = await native_task
```

### Vectorized Parallel Dispatch: `hypertile.gather_to_thread` & `batch_native_pipeline`
Crosses the Python &harr; Rust FFI boundary once for an entire batch rather than once per item, unlocking extreme throughput:

```python
# Vectorized Python callables
results = await hypertile.gather_to_thread(process_record, [r1, r2, r3, r4])

# Vectorized Native compute (>500,000 items/sec)
digests = await hypertile.batch_native_pipeline([chunk1, chunk2, chunk3], rounds=100)
```

### Dynamic Worker Registration
Join the work-stealing pool from long-running server threads (e.g., FastAPI lifespan):

```python
# Context-manager based lifecycle
with hypertile.register_worker(kind="bilingual") as worker:
    worker.run_until_idle()
```

---

## 5. Development with `uv` & Free-Threaded Python

To set up a local free-threaded environment with `uv`:

```bash
# 1. Install free-threaded CPython 3.13t
uv python install 3.13t

# 2. Create virtual environment
uv venv --python 3.13t .venv-313t

# 3. Install build tools
uv pip install maturin pytest fastapi httpx --python .venv-313t/Scripts/python.exe

# 4. Build and install editable release wheel
$env:VIRTUAL_ENV = "D:\HyperTile\.venv-313t"
& .venv-313t\Scripts\maturin.exe develop --release --uv
```

---

## 6. Runnable Examples & Showcases

The repository includes complete, production-ready examples and showcases:

1. **Free-Threaded Showcase Benchmark ([`showcase/free_threaded_showcase.py`](showcase/free_threaded_showcase.py)):**
   3-mode benchmark comparing standard ThreadPoolExecutor, Hypertile Direct Native, and Hypertile Vectorized Batch on Python 3.13t (No-GIL).
   ```bash
   .venv-313t/Scripts/python showcase/free_threaded_showcase.py
   ```

2. **Honest Baseline Benchmark ([`showcase/showcase_benchmark.py`](showcase/showcase_benchmark.py)):**
   Unvarnished comparison running on standard GIL Python builds (Python 3.11).
   ```bash
   .venv/Scripts/python showcase/showcase_benchmark.py
   ```

3. **FastAPI Microservice ([`examples/fastapi_service.py`](examples/fastapi_service.py)):**
   Lifespan worker registration, `@hypertile.task` offloading, and native pipeline integration.
   ```bash
   python examples/fastapi_service.py
   ```

4. **Batch Data Pipeline ([`examples/data_pipeline.py`](examples/data_pipeline.py)):**
   Multi-stage ETL pipeline, cooperative cancellation tokens, and dynamic worker capacity scaling.
   ```bash
   python examples/data_pipeline.py
   ```

---

## 7. Benchmarks

Measured on AMD64 Windows:

| Benchmark Mode | Standard ThreadPoolExecutor | Hypertile Direct Native (3.13t) | Hypertile Vectorized Batch | Real-World Impact |
|---|---|---|---|---|
| **Throughput (req/s)** | ~10,747 req/s | **19,340 req/s** | **237,270 – 697,817 req/s** | **1.80x (scalar) to 22.1x+ (vector)** |
| **Median Latency (p50)** | 17.54 ms | **8.21 ms** | **4.21 µs / item** | **-53.2% latency (scalar) / sub-5µs (vector)** |
| **Tail Latency (p95)** | 24.08 ms | **11.23 ms** | **4.21 µs / item** | **-53.4% tail latency reduction** |
| **Dynamic Worker Cycle** | N/A (Static Pools) | **2.76 µs / cycle** | N/A | Dynamic external thread adoption |
| **Rust Boundary Handoff** | N/A | **1.01 µs / hop** | **< 100 ns / item** | Single-hop Chase-Lev deque routing |


---

## 8. License

Licensed under either of:
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))
