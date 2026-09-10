# Hypertile Showcase: Real-World Benchmarks & Architecture Comparisons

<div align="center">
  <img src="../assets/logo.png" width="120" height="120" alt="Hypertile Logo" />
  <p><strong>Empirical Performance Validation: Standard Async Architectures vs. Colocated Work-Stealing</strong></p>
</div>

---

## 1. Benchmark Scenarios Overview

This directory contains standalone, reproducible benchmark suites comparing standard asynchronous offloading architectures (**WITHOUT Hypertile**) against Hypertile's colocated work-stealing engine (**WITH Hypertile**).

Workloads simulate production CPU-intensive and cryptographic transforms (e.g. JWT verification, hashing, data transformations) dispatched from asynchronous web request handlers.

---

## 2. Scenario 1: Free-Threaded Python 3.13t (No-GIL, PEP 779)

Run with `uv`:
```bash
.venv-313t/Scripts/python showcase/free_threaded_showcase.py
```

### Modes Evaluated:
* **Mode A: Standard ThreadPoolExecutor (Double-Hop Offload):**
  Coroutines offload compute via `loop.run_in_executor(pool, ...)`:
  $$\text{Native Completion} \longrightarrow \texttt{call\_soon\_threadsafe} \longrightarrow \texttt{eventfd} \longrightarrow \text{Asyncio Loop Tick} \longrightarrow \text{Python Continuation}$$
* **Mode B: Hypertile Direct Native Task (Single-Hop Continuation):**
  Coroutines await native Rust futures directly via `await hypertile.spawn_native_pipeline(...)`. The finishing native thread routes continuation directly to its local Chase-Lev queue.
* **Mode C: Hypertile Vectorized Batch Pipeline (Zero-Overhead Vector FFI):**
  Payloads are dispatched as vectorized batches via `await hypertile.batch_native_pipeline(...)`, crossing the FFI boundary once per slice and dynamically distributing work across physical cores.

### Empirical Results (Windows AMD64, 8 cores / 16 threads, Python 3.13.15t No-GIL):

| Metric | ThreadPoolExecutor (Mode A) | Hypertile Direct (Mode B) | Hypertile Vector (Mode C) | Speedup vs Baseline |
|---|---|---|---|---|
| **Throughput (req/s)** | 10,747 req/s | **19,340 req/s** | **237,270 req/s** | **1.80x (Mode B) / 22.1x (Mode C)** |
| **Wall Time (10,000 reqs)** | 0.930 s | **0.517 s** | **0.042 s** | **-44.4% (Mode B) / -95.5% (Mode C)** |
| **Median Latency (p50)** | 17.54 ms | **8.21 ms** | **4.21 µs / item** | **-53.2% latency reduction** |
| **Tail Latency (p95)** | 24.08 ms | **11.23 ms** | **4.21 µs / item** | **-53.4% tail latency reduction** |
| **Tail Latency (p99)** | 74.17 ms | **56.39 ms** | **4.21 µs / item** | **-24.0% tail latency reduction** |

---

## 3. Scenario 2: Standard Python 3.11 with GIL (Cooperative Mode)

Run with `uv`:
```bash
.venv/Scripts/python showcase/showcase_benchmark.py
```

### Empirical Results (Windows AMD64, Python 3.11.9):

| Metric | Standard asyncio (Baseline) | Hypertile L2 Hook | Hypertile Vector Batch | Speedup vs Baseline |
|---|---|---|---|---|
| **Cross-Thread Offload Latency** | 115.41 µs | **113.22 µs** | N/A | **1.02x faster offload** |
| **Pipeline Throughput** | 9,653 req/s | 8,295 req/s | **226,924 req/s** | **23.5x throughput multiplier** |
| **Median Latency (p50)** | 21.68 ms | 25.62 ms | **4.41 µs / item** | **Sub-5µs per item** |
| **Dynamic Worker Cycle** | N/A | **2.76 µs / cycle** | N/A | **Sub-3µs thread registration** |

### The GIL Reality:
* Under the GIL, Python coroutine stepping is serialized. Pure Python async switching in `asyncio` is tightly implemented in C.
* Hypertile operates in **Cooperative Mode** on GIL builds, providing pure native task offloading, lock-free dynamic worker registration, and high-performance vectorized batch pipelines (>226,000 req/s).

---

## 4. Multi-Environment Setup with `uv`

```bash
# 1. Install free-threaded Python 3.13t
uv python install 3.13t

# 2. Setup 3.13t virtual environment
uv venv --python 3.13t .venv-313t
uv pip install maturin pytest fastapi httpx --python .venv-313t/Scripts/python.exe

# 3. Build release extension with uv
$env:VIRTUAL_ENV = "d:\HyperTile\.venv-313t"
& .venv-313t\Scripts\maturin.exe develop --release --uv

# 4. Run the benchmark
& .venv-313t\Scripts\python.exe showcase/free_threaded_showcase.py
```
