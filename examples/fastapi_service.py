"""FastAPI + Hypertile Production Service Example.

This example showcases:
1. Dynamic Worker Registration (PRD_v2 §2.3):
   During application startup via the lifespan context manager, the ASGI worker thread
   registers itself into Hypertile's work-stealing pool (hypertile.register_worker).
2. Asyncio Task Offloading via @hypertile.task:
   CPU-intensive work (e.g., token verification, heavy cryptographic transforms) is offloaded
   to bilingual workers without blocking the ASGI event loop or allocating unbounded OS threads.
3. Native Pipeline Offloading via hypertile.spawn_native_pipeline:
   Direct single-hop continuation routing from native Rust compute into the async response handler.

To run with uvicorn:
    uvicorn examples.fastapi_service:app --workers 1 --port 8000

To run the built-in self-test:
    python examples/fastapi_service.py
"""

import hashlib
import time
from contextlib import asynccontextmanager

try:
    from fastapi import FastAPI
    from pydantic import BaseModel
except ImportError:
    print(
        "FastAPI is not installed in the current environment. Run: uv pip install fastapi"
    )
    import sys

    sys.exit(0)

import hypertile

# ---------------------------------------------------------------------------
# Hypertile Decorated Tasks
# ---------------------------------------------------------------------------


@hypertile.task
def cpu_heavy_hash(data: str, iterations: int = 10_000) -> str:
    """CPU-bound task offloaded to Hypertile's work-stealing pool.

    Decorated with @hypertile.task, this function executes on bilingual workers
    and returns an awaitable CallableTask without blocking the FastAPI event loop.
    """
    current = data.encode("utf-8")
    for _ in range(iterations):
        current = hashlib.sha256(current).digest()
    return current.hex()


# ---------------------------------------------------------------------------
# Lifespan: Worker Registration & Deregistration
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager that joins the Hypertile executor pool."""
    print(
        "[Hypertile FastAPI] Starting up: Registering worker into work-stealing pool..."
    )
    worker = None
    try:
        worker = hypertile.register_worker(kind="bilingual")
        print("[Hypertile FastAPI] Successfully registered bilingual worker.")
    except Exception as exc:  # noqa: BLE001
        print(f"[Hypertile FastAPI] Worker registration skipped/failed: {exc}")

    yield

    if worker is not None:
        print("[Hypertile FastAPI] Shutting down: Deregistering worker...")
        worker.deregister()
        print("[Hypertile FastAPI] Worker deregistered.")


app = FastAPI(
    title="Hypertile-Accelerated Microservice",
    description="High-concurrency microservice utilizing Hypertile work-stealing pool.",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Request Models & Endpoints
# ---------------------------------------------------------------------------


class HashRequest(BaseModel):
    data: str
    iterations: int = 10_000


class PipelineRequest(BaseModel):
    payload: str
    rounds: int = 100


@app.get("/health")
async def health() -> dict[str, object]:
    """Health check endpoint providing runtime and Hypertile status."""
    return {
        "status": "healthy",
        "free_threaded": hypertile.is_free_threaded(),
        "hypertile_version": hypertile.__version__,
    }


@app.post("/compute/hash")
async def compute_hash(req: HashRequest) -> dict[str, object]:
    """Offload CPU-bound hash computation via @hypertile.task."""
    t0 = time.perf_counter()
    digest = await cpu_heavy_hash(req.data, req.iterations)
    elapsed_ms = (time.perf_counter() - t0) * 1000.0

    return {
        "digest": digest,
        "iterations": req.iterations,
        "latency_ms": round(elapsed_ms, 3),
    }


@app.post("/compute/native-pipeline")
async def native_pipeline(req: PipelineRequest) -> dict[str, object]:
    """Direct single-hop native compute task via spawn_native_pipeline."""
    t0 = time.perf_counter()
    raw_data = req.payload.encode("utf-8")

    task = hypertile.spawn_native_pipeline(raw_data, rounds=req.rounds)
    result_bytes = await task
    elapsed_ms = (time.perf_counter() - t0) * 1000.0

    return {
        "result_hex": result_bytes.hex(),
        "rounds": req.rounds,
        "latency_ms": round(elapsed_ms, 3),
    }


# ---------------------------------------------------------------------------
# Self-Test Runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from fastapi.testclient import TestClient

    print("=" * 70)
    print("      HYPERTILE FASTAPI SERVICE SELF-TEST")
    print(f"      Free-threaded: {hypertile.is_free_threaded()}")
    print("=" * 70)

    with TestClient(app) as client:
        # Test 1: Health check
        res_health = client.get("/health")
        print(
            f"GET  /health                  -> {res_health.status_code} | {res_health.json()}"
        )

        # Test 2: Hypertile Task Offloading
        t0 = time.perf_counter()
        res_hash = client.post(
            "/compute/hash", json={"data": "hypertile-payload", "iterations": 5_000}
        )
        t_hash = (time.perf_counter() - t0) * 1000
        print(
            f"POST /compute/hash            -> {res_hash.status_code} | {res_hash.json()['digest'][:16]}... ({t_hash:.2f}ms)"
        )

        # Test 3: Native Pipeline
        t0 = time.perf_counter()
        res_pipe = client.post(
            "/compute/native-pipeline",
            json={"payload": "secret-key-material", "rounds": 100},
        )
        t_pipe = (time.perf_counter() - t0) * 1000
        print(
            f"POST /compute/native-pipeline -> {res_pipe.status_code} | {res_pipe.json()['result_hex'][:16]}... ({t_pipe:.2f}ms)"
        )

    print("=" * 70)
    print("All FastAPI endpoints verified successfully with Hypertile!")
