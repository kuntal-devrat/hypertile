"""Comprehensive test suite for Hypertile Python package."""

import asyncio

import hypertile
import pytest


def test_version_and_exports():
    assert hypertile.__version__ == "0.1.0"
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

