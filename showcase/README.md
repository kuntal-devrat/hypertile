# Hypertile Showcase: Real-World Benchmarks (Free-Threaded & GIL)

This directory contains real-world benchmarks comparing standard asynchronous architectures (**WITHOUT Hypertile**) against Hypertile's colocated work-stealing engine (**WITH Hypertile**).

---

## 1. The Two Benchmark Scenarios

### Scenario 1: Free-Threaded Python 3.13t (No-GIL, PEP 779) — THE WINNING ARCHITECTURE
Run with:
```bash
.venv-313t\Scripts\python.exe showcase/free_threaded_showcase.py
```

Compares:
* **Mode A (Standard Architecture):** Coroutine offloads native compute via `loop.run_in_executor(pool, ...)` (double-hop via `call_soon_threadsafe`).
* **Mode B (Hypertile Direct):** Coroutine directly awaits native Rust task `await hypertile.spawn_native_pipeline(...)` executed on Hypertile's work-stealing pool with single-hop continuation routing.

#### Measured Results (Windows AMD64, Python 3.13.15 No-GIL):
| Metric | ThreadPoolExecutor (Mode A) | Hypertile Direct (Mode B) | Impact |
|---|---|---|---|
| **Throughput (req/s)** | **14,484 req/s** | **22,535 req/s** | **+56% throughput increase (1.56x)** |
| **Wall Time (10,000 reqs)** | **0.690 s** | **0.444 s** | **35.7% faster** |
| **Median Latency (p50)** | **11.56 ms** | **7.28 ms** | **37.0% latency reduction** |
| **95th Percentile (p95)** | **42.80 ms** | **9.41 ms** | **78.0% tail latency reduction** |
| **99th Percentile (p99)** | **67.98 ms** | **44.32 ms** | **34.8% tail latency reduction** |

---

### Scenario 2: Standard Python 3.11 with GIL (Cooperative Mode)
Run with:
```bash
.venv\Scripts\python.exe showcase/showcase_benchmark.py
```

* **Reality under the GIL:** Because Python bytecode is serialized by the GIL, wrapping asyncio with Python subclasses and allocating tokens adds overhead that cannot be recovered through parallel execution.
* On standard GIL builds, raw C-asyncio is faster for pure Python IO, while Hypertile's advantage is restricted to pure Rust execution and dynamic worker registration.

---

## 2. Setting Up Free-Threaded Python with `uv`

Hypertile uses `uv` for fast Python environment management:

```bash
# 1. Download and install free-threaded Python 3.13t
uv python install 3.13t

# 2. Create a free-threaded virtual environment
uv venv .venv-313t --python 3.13t

# 3. Install build tools with uv
uv pip install --python .venv-313t\Scripts\python.exe pytest maturin

# 4. Build Hypertile with free-threading support enabled
$env:VIRTUAL_ENV="d:\HyperTile\.venv-313t"; maturin develop
```
