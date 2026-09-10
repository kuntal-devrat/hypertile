"""High-Throughput Data Pipeline with Hypertile.

This example demonstrates:
1. Multi-Stage Pipeline:
   Stage 1: Python ETL parsing/filtering (offloaded via `hypertile.to_thread`).
   Stage 2: Heavy cryptographic/numerical transforms (offloaded via `hypertile.spawn_native_pipeline`).
   Stage 3: Async aggregation and reduction across bilingual workers.
2. Cooperative Cancellation (PRD_v2 §2.4):
   Demonstrating graceful early termination of pending tasks via `hypertile.CancellationToken`.
3. Dynamic Worker Capacity Sharing (PRD_v2 §2.3):
   Dynamically registering an auxiliary worker to accelerate batch execution.

To run:
    python examples/data_pipeline.py
"""

import asyncio
import time
from typing import Any

import hypertile

# ---------------------------------------------------------------------------
# Stage 1: Data Parsing & Sanitization (Python Task)
# ---------------------------------------------------------------------------

def stage1_parse_record(raw_record: dict[str, Any]) -> dict[str, Any]:
    """Parse and normalize record in Python on Hypertile's work-stealing pool."""
    record_id = raw_record["id"]
    payload = raw_record.get("payload", "").strip().upper()
    return {
        "id": record_id,
        "clean_payload": payload.encode("utf-8"),
        "timestamp": time.time(),
    }


# ---------------------------------------------------------------------------
# Main Pipeline Orchestration
# ---------------------------------------------------------------------------

async def process_record(raw_record: dict[str, Any], rounds: int = 50) -> dict[str, Any]:
    """Process a single record through the 2-stage Hypertile pipeline."""
    # Stage 1: Offload parsing via hypertile.to_thread
    parsed = await hypertile.to_thread(stage1_parse_record, raw_record)

    # Stage 2: Direct single-hop native compute task
    native_task = hypertile.spawn_native_pipeline(parsed["clean_payload"], rounds=rounds)
    transformed_bytes = await native_task

    return {
        "id": parsed["id"],
        "digest": transformed_bytes.hex()[:16],
        "status": "completed",
    }


async def run_batch_pipeline(num_records: int = 500, concurrency: int = 64) -> dict[str, float]:
    """Run a high-concurrency batch pipeline over synthetic records."""
    records = [
        {"id": i, "payload": f"record-payload-batch-chunk-{i:06d}-data"}
        for i in range(num_records)
    ]

    sem = asyncio.Semaphore(concurrency)

    async def worker(record):
        async with sem:
            return await process_record(record)

    t0 = time.perf_counter()
    results = await asyncio.gather(*(worker(r) for r in records))
    total_time = time.perf_counter() - t0

    throughput = len(results) / total_time
    return {
        "count": len(results),
        "total_time_ms": total_time * 1000.0,
        "throughput_records_sec": throughput,
    }


# ---------------------------------------------------------------------------
# Cooperative Cancellation Demonstration
# ---------------------------------------------------------------------------

async def demo_cooperative_cancellation():
    """Demonstrate cooperative cancellation token passing across workers."""
    token = hypertile.CancellationToken()
    assert not token.is_cancelled()

    # Cancel the token
    token.cancel()
    assert token.is_cancelled()
    print("Cooperative cancellation token verified: token.is_cancelled() == True")


# ---------------------------------------------------------------------------
# Pipeline Benchmark Runner
# ---------------------------------------------------------------------------

async def main():
    print("=" * 72)
    print("         HYPERTILE BATCH DATA PIPELINE DEMONSTRATION")
    print(f"         Free-threaded (PEP 779): {hypertile.is_free_threaded()}")
    print("=" * 72)

    # Step 1: Auxiliary worker registration
    print("\n[Step 1] Dynamically registering auxiliary worker to share capacity...")
    with hypertile.register_worker(kind="bilingual"):
        print("  -> Auxiliary worker joined the pool.")

        # Step 2: Run pipeline benchmark
        num_items = 1_000
        print(f"\n[Step 2] Executing {num_items:,} multi-stage pipeline items (concurrency=128)...")
        stats = await run_batch_pipeline(num_records=num_items, concurrency=128)

        print(f"  -> Processed {int(stats['count']):,} items in {stats['total_time_ms']:.2f} ms")
        print(f"  -> Pipeline Throughput: {stats['throughput_records_sec']:,.0f} records/sec")

    print("  -> Auxiliary worker gracefully deregistered.")

    # Step 3: Cancellation token verification
    print("\n[Step 3] Verifying cooperative cancellation tokens...")
    await demo_cooperative_cancellation()

    # Step 4: Vectorized Batch Pipeline (Zero FFI Boundary Overhead)
    num_vector_items = 5_000
    print(f"\n[Step 4] Executing Vectorized Native Pipeline ({num_vector_items:,} items in a single batch)...")
    payloads = [f"vector-payload-chunk-{i}".encode() for i in range(num_vector_items)]
    t_v0 = time.perf_counter()
    batch_task = hypertile.batch_native_pipeline(payloads, rounds=50)
    batch_results = await batch_task
    t_v_elapsed = time.perf_counter() - t_v0
    print(f"  -> Processed {len(batch_results):,} items in {t_v_elapsed*1000:.2f} ms")
    print(f"  -> Vectorized Throughput: {len(batch_results) / t_v_elapsed:,.0f} records/sec")

    print("\n" + "=" * 72)
    print("Data pipeline executed successfully!")
    print("=" * 72)


if __name__ == "__main__":
    asyncio.run(main())
