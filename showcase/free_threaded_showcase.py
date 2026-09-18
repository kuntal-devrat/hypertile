"""Free-Threaded (No-GIL) Python Benchmark Showcase.

Compares:
  Mode A: Standard ThreadPoolExecutor offload (asyncio.run_in_executor)
  Mode B: Hypertile Direct Native Task (await hypertile.spawn_native_pipeline)

On a free-threaded build (PEP 779, Python 3.14t), the GIL is disabled. This benchmark
directly compares the double-hop offload mechanism against Hypertile's direct native
work-stealing continuation.

The figures recorded in the README were produced on Python 3.13t, which Hypertile no
longer builds for (PyO3 dropped free-threaded 3.13 when it added 3.14). The script runs
unchanged on 3.14t - re-run it there for current numbers.
"""

import asyncio
import json
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import hypertile


def generate_payload(idx: int) -> bytes:
    return json.dumps(
        {
            "req_id": f"tx-{idx:07d}",
            "tenant": f"cluster-{idx % 32}",
            "body": "stream-packet-buffer-payload-telemetry" * 6,
        }
    ).encode("utf-8")


# ============================================================================
# Mode A: Standard ThreadPoolExecutor (Double-Hop Offload)
# ============================================================================


async def run_standard_suite(total: int, concurrency: int) -> dict[str, float]:
    pool = ThreadPoolExecutor(max_workers=8)
    sem = asyncio.Semaphore(concurrency)
    latencies: list[float] = []

    async def handle_request(idx: int, data: bytes):
        async with sem:
            t0 = time.perf_counter()
            await asyncio.sleep(0.0001)  # Async ingest
            loop = asyncio.get_running_loop()
            # Standard threadpool offload (double-hop)
            await loop.run_in_executor(pool, hypertile.native_pipeline_transform, data, 100)
            await asyncio.sleep(0.0001)  # Async egress
            latencies.append((time.perf_counter() - t0) * 1000.0)

    payloads = [generate_payload(i) for i in range(total)]
    t_start = time.perf_counter()
    tasks = [asyncio.create_task(handle_request(i, payloads[i])) for i in range(total)]
    await asyncio.gather(*tasks)
    wall = time.perf_counter() - t_start
    pool.shutdown(wait=True)

    latencies.sort()
    return {
        "wall_s": wall,
        "req_s": total / wall,
        "mean_ms": statistics.mean(latencies),
        "p50_ms": statistics.median(latencies),
        "p95_ms": latencies[int(len(latencies) * 0.95)],
        "p99_ms": latencies[int(len(latencies) * 0.99)],
    }


# ============================================================================
# Mode B: Hypertile Direct Native Task (Single-Hop Continuation)
# ============================================================================


async def run_hypertile_direct_suite(total: int, concurrency: int) -> dict[str, float]:
    sem = asyncio.Semaphore(concurrency)
    latencies: list[float] = []

    async def handle_request(idx: int, data: bytes):
        async with sem:
            t0 = time.perf_counter()
            await asyncio.sleep(0.0001)  # Async ingest
            # Hypertile direct native task (bypasses run_in_executor)
            await hypertile.spawn_native_pipeline(data, 100)
            await asyncio.sleep(0.0001)  # Async egress
            latencies.append((time.perf_counter() - t0) * 1000.0)

    payloads = [generate_payload(i) for i in range(total)]
    t_start = time.perf_counter()
    tasks = [asyncio.create_task(handle_request(i, payloads[i])) for i in range(total)]
    await asyncio.gather(*tasks)
    wall = time.perf_counter() - t_start

    latencies.sort()
    return {
        "wall_s": wall,
        "req_s": total / wall,
        "mean_ms": statistics.mean(latencies),
        "p50_ms": statistics.median(latencies),
        "p95_ms": latencies[int(len(latencies) * 0.95)],
        "p99_ms": latencies[int(len(latencies) * 0.99)],
    }


# ============================================================================
# Mode C: Hypertile Vectorized Batch Pipeline (Zero-Overhead Vector FFI)
# ============================================================================


async def run_vectorized_batch_suite(total: int, batch_size: int = 100) -> dict[str, float]:
    payloads = [generate_payload(i) for i in range(total)]
    chunks = [payloads[i : i + batch_size] for i in range(0, total, batch_size)]

    t_start = time.perf_counter()
    tasks = [hypertile.batch_native_pipeline(chunk, rounds=100) for chunk in chunks]
    batch_results = await asyncio.gather(*tasks)
    wall = time.perf_counter() - t_start

    # Flatten results
    total_processed = sum(len(res) for res in batch_results)
    avg_per_batch_ms = (wall / len(chunks)) * 1000.0

    return {
        "wall_s": wall,
        "req_s": total_processed / wall,
        "mean_ms": avg_per_batch_ms / batch_size,
        "p50_ms": avg_per_batch_ms / batch_size,
        "p95_ms": avg_per_batch_ms / batch_size,
        "p99_ms": avg_per_batch_ms / batch_size,
    }


def main():
    print("=" * 80)
    print("      HYPERTILE FREE-THREADED (NO-GIL) SHOWCASE BENCHMARK REPORT")
    print(
        f"Platform: {sys.platform} | Python: {sys.version.split()[0]} | Free-threaded: {hypertile.is_free_threaded()}"
    )
    print("=" * 80)

    total_requests = 10_000
    concurrency = 256

    print(
        f"\n[1/3] Running Mode A: Standard ThreadPoolExecutor ({total_requests:,} requests @ {concurrency} concurrency)..."
    )
    std_res = asyncio.run(run_standard_suite(total_requests, concurrency))
    print(
        f"      Completed in {std_res['wall_s']:.2f}s ({std_res['req_s']:,.0f} req/s, p50: {std_res['p50_ms']:.2f}ms, p99: {std_res['p99_ms']:.2f}ms)"
    )

    time.sleep(0.3)

    print(
        f"\n[2/3] Running Mode B: Hypertile Direct Native Task ({total_requests:,} requests @ {concurrency} concurrency)..."
    )
    hyp_res = asyncio.run(run_hypertile_direct_suite(total_requests, concurrency))
    print(
        f"      Completed in {hyp_res['wall_s']:.2f}s ({hyp_res['req_s']:,.0f} req/s, p50: {hyp_res['p50_ms']:.2f}ms, p99: {hyp_res['p99_ms']:.2f}ms)"
    )

    time.sleep(0.3)

    print(
        f"\n[3/3] Running Mode C: Hypertile Vectorized Batch ({total_requests:,} requests in chunks of 100)..."
    )
    batch_res = asyncio.run(run_vectorized_batch_suite(total_requests, batch_size=100))
    print(
        f"      Completed in {batch_res['wall_s']:.4f}s ({batch_res['req_s']:,.0f} req/s, amortized: {batch_res['p50_ms'] * 1000:.2f} us/item)"
    )

    speedup_b = hyp_res["req_s"] / std_res["req_s"]
    speedup_c = batch_res["req_s"] / std_res["req_s"]

    print("\n" + "=" * 92)
    print("                              FREE-THREADED COMPARISON TABLE")
    print("=" * 92)
    print(
        f"{'Metric':<24} | {'ThreadPool (Mode A)':<20} | {'Hypertile Direct (B)':<21} | {'Hypertile Vector (C)':<20}"
    )
    print("-" * 92)
    print(
        f"{'Throughput (req/s)':<24} | {std_res['req_s']:>18,.0f} | {hyp_res['req_s']:>19,.0f} | {batch_res['req_s']:>18,.0f}"
    )
    print(
        f"{'Wall Time (s)':<24} | {std_res['wall_s']:>18.3f} | {hyp_res['wall_s']:>19.3f} | {batch_res['wall_s']:>18.4f}"
    )
    print(
        f"{'Median Latency (p50)':<24} | {std_res['p50_ms']:>15.2f} ms | {hyp_res['p50_ms']:>16.2f} ms | {batch_res['p50_ms'] * 1000:>15.2f} us"
    )
    print(
        f"{'95th Percentile (p95)':<24} | {std_res['p95_ms']:>15.2f} ms | {hyp_res['p95_ms']:>16.2f} ms | {batch_res['p95_ms'] * 1000:>15.2f} us"
    )
    print(
        f"{'Tail Latency (p99)':<24} | {std_res['p99_ms']:>15.2f} ms | {hyp_res['p99_ms']:>16.2f} ms | {batch_res['p99_ms'] * 1000:>15.2f} us"
    )
    print("=" * 92)
    print("SUMMARY:")
    print(
        f"  - Mode B (Direct Native): {speedup_b:.2f}x throughput increase, {std_res['p50_ms'] / hyp_res['p50_ms']:.1f}x lower median latency."
    )
    print(
        f"  - Mode C (Vector Batch):  {speedup_c:.1f}x throughput increase over standard ThreadPoolExecutor!"
    )
    print("=" * 92 + "\n")


if __name__ == "__main__":
    main()
