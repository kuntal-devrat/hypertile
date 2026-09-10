"""Hypertile Real-World Showcase & Comparative Benchmark (PRD_v2 §1.1, §6.1).

Demonstrates a high-throughput API gateway / event-processing microservice
comparing:
  1. WITHOUT Hypertile: Standard asyncio event loop + standard ThreadPoolExecutor.
  2. WITH Hypertile: Colocated work-stealing pool with single-hop continuation handoff.

Workload Pipeline:
  [Phase 1] Python Async Ingest: payload validation, header parsing (async IO)
  [Phase 2] Rust Native Compute: cryptographic hash & state mixing (CPU bound)
  [Phase 3] Python Async Egress: response formatting, audit telemetry (async IO)
"""

import asyncio
import json
import statistics
import time
from concurrent.futures import ThreadPoolExecutor

import hypertile


def generate_mock_payload(idx: int) -> bytes:
    """Generate a realistic JSON payload for processing."""
    payload = {
        "request_id": f"req-2026-{idx:08d}",
        "timestamp": time.time(),
        "client_id": f"tenant-{idx % 64:03d}",
        "payload": f"telemetry-stream-sample-data-packet-{idx:05d}" * 4,
    }
    return json.dumps(payload).encode("utf-8")


# ============================================================================
# Mode 1: WITHOUT Hypertile (Standard Python asyncio + ThreadPoolExecutor)
# ============================================================================


async def process_request_standard(
    idx: int,
    payload: bytes,
    executor: ThreadPoolExecutor,
) -> tuple[bytes, float]:
    """Standard pipeline using asyncio + disconnected ThreadPoolExecutor."""
    start_time = time.perf_counter()

    # 1. Python async ingest step (simulated non-blocking network IO)
    await asyncio.sleep(0.0001)

    # 2. Native Rust compute step offloaded via run_in_executor
    loop = asyncio.get_running_loop()
    transformed = await loop.run_in_executor(
        executor,
        hypertile.native_pipeline_transform,
        payload,
        150,  # computation rounds
    )

    # 3. Python async egress step (simulated audit / response IO)
    await asyncio.sleep(0.0001)

    latency_ms = (time.perf_counter() - start_time) * 1000.0
    return transformed, latency_ms


async def run_standard_suite(
    total_requests: int,
    concurrency: int,
) -> dict[str, float]:
    """Execute the baseline pipeline without Hypertile."""
    executor = ThreadPoolExecutor(max_workers=8)
    latencies: list[float] = []

    payloads = [generate_mock_payload(i) for i in range(total_requests)]
    sem = asyncio.Semaphore(concurrency)

    async def worker(idx: int, data: bytes):
        async with sem:
            _, lat = await process_request_standard(idx, data, executor)
            latencies.append(lat)

    wall_start = time.perf_counter()
    tasks = [asyncio.create_task(worker(i, payloads[i])) for i in range(total_requests)]
    await asyncio.gather(*tasks)
    total_wall_time = time.perf_counter() - wall_start

    executor.shutdown(wait=True)

    latencies.sort()
    p50 = statistics.median(latencies)
    p90 = latencies[int(len(latencies) * 0.90)]
    p95 = latencies[int(len(latencies) * 0.95)]
    p99 = latencies[int(len(latencies) * 0.99)]
    mean = statistics.mean(latencies)
    throughput = total_requests / total_wall_time

    return {
        "total_requests": total_requests,
        "wall_time_s": total_wall_time,
        "throughput_req_s": throughput,
        "mean_ms": mean,
        "p50_ms": p50,
        "p90_ms": p90,
        "p95_ms": p95,
        "p99_ms": p99,
    }


# ============================================================================
# Mode 2: WITH Hypertile (Colocated Work-Stealing Pool & Single-Hop Handoff)
# ============================================================================


async def process_request_hypertile(
    idx: int,
    payload: bytes,
) -> tuple[bytes, float]:
    """Hypertile pipeline: colocated single-hop execution."""
    start_time = time.perf_counter()

    # 1. Python async ingest step
    await asyncio.sleep(0.0001)

    # 2. Native Rust compute step executed directly on colocated pool
    loop = asyncio.get_running_loop()
    transformed = await loop.run_in_executor(
        None,  # Uses Hypertile's default colocated executor
        hypertile.native_pipeline_transform,
        payload,
        150,
    )

    # 3. Python async egress step: single-hop continuation resumption
    await asyncio.sleep(0.0001)

    latency_ms = (time.perf_counter() - start_time) * 1000.0
    return transformed, latency_ms


async def run_hypertile_suite(
    total_requests: int,
    concurrency: int,
) -> dict[str, float]:
    """Execute the pipeline with Hypertile installed."""
    hypertile.install()
    latencies: list[float] = []

    payloads = [generate_mock_payload(i) for i in range(total_requests)]
    sem = asyncio.Semaphore(concurrency)

    async def worker(idx: int, data: bytes):
        async with sem:
            _, lat = await process_request_hypertile(idx, data)
            latencies.append(lat)

    wall_start = time.perf_counter()
    tasks = [asyncio.create_task(worker(i, payloads[i])) for i in range(total_requests)]
    await asyncio.gather(*tasks)
    total_wall_time = time.perf_counter() - wall_start

    latencies.sort()
    p50 = statistics.median(latencies)
    p90 = latencies[int(len(latencies) * 0.90)]
    p95 = latencies[int(len(latencies) * 0.95)]
    p99 = latencies[int(len(latencies) * 0.99)]
    mean = statistics.mean(latencies)
    throughput = total_requests / total_wall_time

    return {
        "total_requests": total_requests,
        "wall_time_s": total_wall_time,
        "throughput_req_s": throughput,
        "mean_ms": mean,
        "p50_ms": p50,
        "p90_ms": p90,
        "p95_ms": p95,
        "p99_ms": p99,
    }


# ============================================================================
# Benchmark Runner & Presentation Table
# ============================================================================


def print_comparison_table(
    std_res: dict[str, float],
    hyp_res: dict[str, float],
) -> None:
    """Print an aesthetic, high-contrast ASCII comparison table."""
    speedup = hyp_res["throughput_req_s"] / std_res["throughput_req_s"]
    p50_reduction = (
        (std_res["p50_ms"] - hyp_res["p50_ms"]) / std_res["p50_ms"]
    ) * 100.0
    p99_reduction = (
        (std_res["p99_ms"] - hyp_res["p99_ms"]) / std_res["p99_ms"]
    ) * 100.0

    print("\n" + "=" * 80)
    print("        HYPERTILE SHOWCASE BENCHMARK: WITH vs. WITHOUT HYPERTILE")
    print("=" * 80)
    print(
        f"Total Requests: {int(std_res['total_requests']):,} | Workload: Ingest (IO) + Rust Compute + Egress (IO)"
    )
    print("-" * 80)
    print(
        f"{'Metric':<25} | {'WITHOUT Hypertile':<20} | {'WITH Hypertile':<20} | {'Delta':<12}"
    )
    print("-" * 80)
    print(
        f"{'Throughput (req/s)':<25} | {std_res['throughput_req_s']:>17,.0f} | {hyp_res['throughput_req_s']:>17,.0f} | {speedup:>8.2f}x"
    )
    print(
        f"{'Wall Time (s)':<25} | {std_res['wall_time_s']:>17.3f} | {hyp_res['wall_time_s']:>17.3f} | {-((std_res['wall_time_s'] - hyp_res['wall_time_s']) / std_res['wall_time_s']) * 100:>7.1f}%"
    )
    print(
        f"{'Mean Latency (ms)':<25} | {std_res['mean_ms']:>17.3f} | {hyp_res['mean_ms']:>17.3f} | {-((std_res['mean_ms'] - hyp_res['mean_ms']) / std_res['mean_ms']) * 100:>7.1f}%"
    )
    print(
        f"{'p50 Latency (ms)':<25} | {std_res['p50_ms']:>17.3f} | {hyp_res['p50_ms']:>17.3f} | {-p50_reduction:>7.1f}%"
    )
    print(
        f"{'p90 Latency (ms)':<25} | {std_res['p90_ms']:>17.3f} | {hyp_res['p90_ms']:>17.3f} | {-((std_res['p90_ms'] - hyp_res['p90_ms']) / std_res['p90_ms']) * 100:>7.1f}%"
    )
    print(
        f"{'p95 Latency (ms)':<25} | {std_res['p95_ms']:>17.3f} | {hyp_res['p95_ms']:>17.3f} | {-((std_res['p95_ms'] - hyp_res['p95_ms']) / std_res['p95_ms']) * 100:>7.1f}%"
    )
    print(
        f"{'p99 Latency (ms)':<25} | {std_res['p99_ms']:>17.3f} | {hyp_res['p99_ms']:>17.3f} | {-p99_reduction:>7.1f}%"
    )
    print("=" * 80)
    print(f"Outcome: Hypertile delivered a {speedup:.2f}x throughput multiplier with a")
    print(f"         {p50_reduction:.1f}% reduction in median (p50) latency and a")
    print(f"         {p99_reduction:.1f}% reduction in tail (p99) latency.")
    print("=" * 80 + "\n")


def main():
    total_requests = 10_000
    concurrency = 256

    print(
        f"\n[1/2] Running baseline: WITHOUT Hypertile ({total_requests:,} requests @ {concurrency} concurrency)..."
    )
    std_res = asyncio.run(run_standard_suite(total_requests, concurrency))
    print(
        f"      Completed in {std_res['wall_time_s']:.2f}s ({std_res['throughput_req_s']:,.0f} req/s, p50: {std_res['p50_ms']:.2f}ms, p99: {std_res['p99_ms']:.2f}ms)"
    )

    # Short cool down between runs
    time.sleep(1.0)

    print(
        f"\n[2/2] Running optimized: WITH Hypertile ({total_requests:,} requests @ {concurrency} concurrency)..."
    )
    hyp_res = asyncio.run(run_hypertile_suite(total_requests, concurrency))
    print(
        f"      Completed in {hyp_res['wall_time_s']:.2f}s ({hyp_res['throughput_req_s']:,.0f} req/s, p50: {hyp_res['p50_ms']:.2f}ms, p99: {hyp_res['p99_ms']:.2f}ms)"
    )

    print_comparison_table(std_res, hyp_res)


if __name__ == "__main__":
    main()
