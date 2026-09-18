"""Comprehensive test suite for Hypertile Python package."""

import asyncio
import os
import subprocess
import sys
import time
import tomllib
from pathlib import Path

import hypertile
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _run_child(
    tmp_path: Path,
    body: str,
    *,
    env: dict[str, str] | None = None,
    timeout: int = 120,
) -> subprocess.CompletedProcess:
    """Run `body` in a fresh interpreter and return the completed process.

    Child interpreters are how these tests exercise process-global state - the worker
    pool's size and lifetime - without disturbing the pool this suite is already using.
    ``env`` is merged onto the parent environment so the child still inherits
    ``PYTHONPATH`` and can import the package under test.
    """
    script = tmp_path / "child.py"
    script.write_text(body, encoding="utf-8")
    return subprocess.run(
        [sys.executable, str(script)],
        capture_output=True,
        timeout=timeout,
        cwd=str(PROJECT_ROOT),
        check=False,
        env=None if env is None else {**os.environ, **env},
    )


def test_version_metadata_is_consistent_across_manifests():
    """Cargo, PyPI and Python metadata must never drift apart."""
    workspace = tomllib.loads((PROJECT_ROOT / "Cargo.toml").read_text(encoding="utf-8"))
    workspace_version = workspace["workspace"]["package"]["version"]

    for crate in ("hypertile-core", "hypertile-sys", "hypertile-capi"):
        manifest = PROJECT_ROOT / crate / "Cargo.toml"
        declared = tomllib.loads(manifest.read_text(encoding="utf-8"))["package"]["version"]
        if isinstance(declared, dict):  # `version.workspace = true`
            assert declared.get("workspace") is True, crate
            declared = workspace_version
        assert declared == workspace_version, f"{crate} version drifts from the workspace"

    pyproject = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert pyproject["project"]["version"] == workspace_version
    assert hypertile.__version__ == workspace_version

    native = pytest.importorskip("hypertile._hypertile_sys")
    assert native.__version__ == workspace_version


def test_shutdown_hook_stops_the_native_pool(tmp_path):
    """The exit hook must actually stop the pool, not just exist.

    Exercised in a child interpreter because it stops the process-wide pool, which the
    rest of this suite still needs.
    """
    assert hypertile._NATIVE_EXTENSION_AVAILABLE
    assert callable(hypertile._shutdown_background_workers)
    assert hasattr(hypertile._hypertile_sys, "shutdown")

    proc = _run_child(
        tmp_path,
        "import asyncio\n"
        "import hypertile\n"
        "\n"
        "async def main():\n"
        "    assert await hypertile.to_thread(lambda: 1) == 1\n"
        "    hypertile._shutdown_background_workers()\n"
        "    # Once stopped, dispatch can never complete - which is what must happen\n"
        "    # before the interpreter is torn down under the workers' feet.\n"
        "    try:\n"
        "        await asyncio.wait_for(hypertile.to_thread(lambda: 2), timeout=1.0)\n"
        "    except (asyncio.TimeoutError, TimeoutError):\n"
        "        return 'stopped'\n"
        "    raise SystemExit('pool still running after shutdown')\n"
        "\n"
        "assert asyncio.run(main()) == 'stopped'\n"
        "# Idempotent: a second call must not hang or raise.\n"
        "hypertile._shutdown_background_workers()\n",
    )
    assert proc.returncode == 0, proc.stderr.decode(errors="replace")


def test_clean_interpreter_exit_after_background_work(tmp_path):
    """Regression: worker threads must not outlive (or hang) the interpreter."""
    proc = _run_child(
        tmp_path,
        "import asyncio, sys\n"
        "import hypertile\n"
        "\n"
        "async def main():\n"
        "    return await hypertile.to_thread(lambda: 1)\n"
        "\n"
        "assert asyncio.run(main()) == 1\n"
        "sys.exit(0)\n",
    )
    assert proc.returncode == 0, proc.stderr.decode(errors="replace")


def test_version_and_exports():
    assert hypertile.__version__ == "0.1.2"
    assert hasattr(hypertile, "run")
    assert hasattr(hypertile, "install")
    assert hasattr(hypertile, "register_worker")
    assert hasattr(hypertile, "CancellationToken")
    assert hasattr(hypertile, "is_free_threaded")
    assert hasattr(hypertile, "to_thread")
    assert hasattr(hypertile, "gather_to_thread")
    assert hasattr(hypertile, "task")
    assert hasattr(hypertile, "spawn_native_pipeline")
    assert hasattr(hypertile, "batch_native_pipeline")
    assert hasattr(hypertile, "native_pipeline_transform")
    assert hasattr(hypertile, "BatchNativeTask")
    assert hasattr(hypertile, "BatchCallableTask")
    assert hasattr(hypertile, "configure")
    assert hasattr(hypertile, "worker_count")
    assert hasattr(hypertile, "default_worker_count")


def test_default_pool_size_never_undercuts_the_cpu_count():
    """The default must serve both workload shapes.

    A pool smaller than the logical CPU count throws away compute parallelism; a pool of
    *exactly* the CPU count serialises blocking calls, which is measurably slower than
    the standard library's default executor for the `to_thread` use case. So the default
    must be at least one worker per logical CPU, and larger on small machines.
    """
    cores = os.cpu_count() or 1
    default = hypertile.default_worker_count()

    assert default >= cores, f"{default} workers cannot saturate {cores} CPUs"
    assert default <= max(cores, 32)
    if cores < 32:
        assert default > cores, (
            f"a {cores}-core machine needs blocking headroom, not exactly {cores} workers"
        )
    # A compute-heavy caller can always ask for fewer; the point is the default.
    assert isinstance(default, int)


def test_worker_count_is_none_until_the_pool_starts(tmp_path):
    """`worker_count()` must report the pool as not-yet-started, not guess.

    Runs in a child interpreter: this process's pool is already running.
    """
    proc = _run_child(
        tmp_path,
        "import hypertile\n"
        "assert hypertile.worker_count() is None, hypertile.worker_count()\n"
        "# Merely importing (and installing the asyncio policy) must not start it.\n"
        "hypertile.install()\n"
        "assert hypertile.worker_count() is None\n",
    )
    assert proc.returncode == 0, proc.stderr.decode(errors="replace")


def test_configure_sizes_the_pool_then_refuses_to_resize(tmp_path):
    """The pool is process-wide and fixed once used, so the API must say so."""
    proc = _run_child(
        tmp_path,
        "import asyncio\n"
        "import hypertile\n"
        "\n"
        "assert hypertile.configure(workers=2) == 2\n"
        "assert hypertile.worker_count() == 2\n"
        "\n"
        "async def main():\n"
        "    # A 2-worker pool must actually run work. Bind `i` per-submission: the\n"
        "    # callable runs later, on a worker, so a bare closure would see the last `i`.\n"
        "    return await asyncio.gather(\n"
        "        *(hypertile.to_thread(lambda i=i: i) for i in range(8))\n"
        "    )\n"
        "\n"
        "assert asyncio.run(main()) == list(range(8))\n"
        "assert hypertile.worker_count() == 2\n"
        "\n"
        "# Resizing a running pool must fail loudly rather than be ignored, and the\n"
        "# message must name the size that is actually in use.\n"
        "try:\n"
        "    hypertile.configure(workers=4)\n"
        "except RuntimeError as exc:\n"
        "    assert 'already started' in str(exc), exc\n"
        "    assert 'HYPERTILE_WORKERS' in str(exc), exc\n"
        "else:\n"
        "    raise SystemExit('resizing a running pool did not raise')\n"
        "assert hypertile.worker_count() == 2, 'a refused configure must not resize'\n",
    )
    assert proc.returncode == 0, proc.stderr.decode(errors="replace")


def test_workers_env_var_sizes_a_pool_started_by_other_code(tmp_path):
    """The env var is the escape hatch for pools someone else starts first."""
    proc = _run_child(
        tmp_path,
        "import asyncio\n"
        "import hypertile\n"
        "\n"
        "assert hypertile.worker_count() is None\n"
        "\n"
        "async def main():\n"
        "    # No configure() call: the env var must size the pool on first use.\n"
        "    await hypertile.to_thread(lambda: 1)\n"
        "\n"
        "asyncio.run(main())\n"
        "assert hypertile.worker_count() == 3, hypertile.worker_count()\n",
        env={"HYPERTILE_WORKERS": "3"},
    )
    assert proc.returncode == 0, proc.stderr.decode(errors="replace")


def test_invalid_workers_env_var_falls_back_to_the_default(tmp_path):
    """A bad value must not silently become an arbitrary pool size."""
    proc = _run_child(
        tmp_path,
        "import asyncio\n"
        "import hypertile\n"
        "\n"
        "async def main():\n"
        "    await hypertile.to_thread(lambda: 1)\n"
        "\n"
        "asyncio.run(main())\n"
        "assert hypertile.worker_count() == hypertile.default_worker_count()\n",
        env={"HYPERTILE_WORKERS": "not-a-number"},
    )
    assert proc.returncode == 0, proc.stderr.decode(errors="replace")
    # The misconfiguration must be reported, not swallowed.
    assert b"HYPERTILE_WORKERS" in proc.stderr


def test_cancellation_token():
    token = hypertile.CancellationToken()
    assert not token.is_cancelled()
    token.cancel()
    assert token.is_cancelled()


def test_dynamic_worker_registration():
    with hypertile.register_worker(kind="bilingual") as worker:
        assert isinstance(worker.worker_id(), int)
        executed = worker.run_one()
        assert executed is False
        worker.run_until_idle()


def test_level1_run_basic():
    async def sample():
        return 42 * 2

    res = hypertile.run(sample())
    assert res == 84


def test_level2_asyncio_integration():
    hypertile.install()

    async def worker(idx):
        await asyncio.sleep(0.01)
        return idx * 10

    async def main():
        tasks = [asyncio.create_task(worker(i)) for i in range(10)]
        for t in tasks:
            assert hasattr(t, "__hypertile_token__")
            assert getattr(t, "__hypertile_token__", None) is not None
        results = await asyncio.gather(*tasks)
        return results

    results = asyncio.run(main())
    assert results == [i * 10 for i in range(10)]


def test_asyncio_cancellation_token_hook():
    hypertile.install()

    async def long_running():
        await asyncio.sleep(100)

    async def main():
        task = asyncio.create_task(long_running())
        token = getattr(task, "__hypertile_token__", None)
        assert token is not None
        assert not token.is_cancelled()

        task.cancel()
        assert token.is_cancelled()

        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(main())


def test_to_thread_execution():
    def compute(a, b, multiplier=1):
        return (a + b) * multiplier

    async def main():
        res1 = await hypertile.to_thread(compute, 10, 20, multiplier=3)
        assert res1 == 90

        res2 = await hypertile.to_thread(lambda x: x * 5, 8)
        assert res2 == 40

    asyncio.run(main())


def test_to_thread_exception():
    def failing():
        raise ArithmeticError("computation overflow")

    async def main():
        with pytest.raises(ArithmeticError, match="computation overflow"):
            await hypertile.to_thread(failing)

    asyncio.run(main())


def test_task_decorator():
    @hypertile.task
    def multiply(x: int, y: int) -> int:
        return x * y

    @hypertile.task
    def err_task():
        raise KeyError("missing_key")

    async def main():
        res = await multiply(6, 7)
        assert res == 42

        with pytest.raises(KeyError, match="missing_key"):
            await err_task()

    asyncio.run(main())


def test_spawn_native_pipeline():
    async def main():
        payload = b"test-pipeline-bytes-data"
        task = hypertile.spawn_native_pipeline(payload, rounds=50)
        assert not task.done() or task.done()
        result = await task
        assert isinstance(result, bytes)
        assert len(result) == 32
        assert task.done()

    asyncio.run(main())


def test_native_pipeline_transform():
    payload = b"direct-transform-input"
    res1 = hypertile.native_pipeline_transform(payload, rounds=20)
    res2 = hypertile.native_pipeline_transform(payload, rounds=20)
    assert isinstance(res1, bytes)
    assert len(res1) == 32
    assert res1 == res2


def test_batch_native_pipeline():
    async def main():
        payloads = [f"batch-item-{i}".encode() for i in range(100)]
        task = hypertile.batch_native_pipeline(payloads, rounds=30)
        results = await task
        assert len(results) == 100
        for r in results:
            assert isinstance(r, bytes)
            assert len(r) == 32
        assert task.done()

    asyncio.run(main())


def test_gather_to_thread():
    def square(x: int) -> int:
        return x * x

    async def main():
        inputs = list(range(50))
        task = hypertile.gather_to_thread(square, inputs)
        results = await task
        assert results == [x * x for x in range(50)]

    asyncio.run(main())


def test_gather_to_thread_tuple_args():
    def add(a: int, b: int) -> int:
        return a + b

    async def main():
        inputs = [(i, i * 2) for i in range(25)]
        task = hypertile.gather_to_thread(add, inputs)
        results = await task
        assert results == [i + i * 2 for i in range(25)]

    asyncio.run(main())


def test_gather_to_thread_exception():
    def faulty(x: int) -> int:
        if x == 13:
            raise ValueError("bad luck 13")
        return x

    async def main():
        task = hypertile.gather_to_thread(faulty, list(range(20)))
        with pytest.raises(ValueError, match="bad luck 13"):
            await task

    asyncio.run(main())


def test_callable_task_repeated_await_and_result():
    async def main():
        t = hypertile.to_thread(lambda: 99)
        res1 = await t
        assert res1 == 99
        res2 = await t
        assert res2 == 99
        res3 = t.result()
        assert res3 == 99

    asyncio.run(main())


def test_done_callback_after_completion_fires_exactly_once():
    """Regression: registering a callback on a finished task must not be lost."""

    async def main():
        task = hypertile.to_thread(lambda: 5)
        assert await task == 5

        calls = []
        task.add_done_callback(lambda: calls.append("first"))
        task.add_done_callback(lambda: calls.append("second"))
        await asyncio.sleep(0)
        return calls

    assert asyncio.run(main()) == ["first", "second"]


def test_done_callback_before_completion_fires_exactly_once():
    """The completion race must not drop or duplicate a pending callback."""

    async def main():
        task = hypertile.to_thread(lambda: (time.sleep(0.05), "done")[1])
        calls = []
        for i in range(8):
            task.add_done_callback(lambda i=i: calls.append(i))

        assert await task == "done"
        await asyncio.sleep(0.1)
        return sorted(calls)

    assert asyncio.run(main()) == list(range(8))


def test_to_thread_rejects_coroutine():
    async def fake_coro():
        return 1

    with pytest.raises(TypeError, match="does not accept coroutines"):
        hypertile.to_thread(fake_coro)


def test_batch_empty_inputs():
    async def main():
        empty_native = await hypertile.batch_native_pipeline([])
        assert empty_native == []

        empty_callable = await hypertile.gather_to_thread(lambda x: x, [])
        assert empty_callable == []

    asyncio.run(main())
