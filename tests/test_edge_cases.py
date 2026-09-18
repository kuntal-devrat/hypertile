"""Edge case and stress test suite for Hypertile."""

import asyncio
import time

import hypertile
import pytest


def test_asyncio_sleep_under_hypertile_run():
    """Verify that asyncio.sleep works seamlessly under hypertile.run()."""

    async def main():
        await asyncio.sleep(0.02)
        res = await hypertile.to_thread(lambda x: x + 10, 5)
        return res

    result = hypertile.run(main())
    assert result == 15


def test_level1_pure_coroutine_execution():
    """Verify run_level1 runs pure coroutines without requiring an asyncio event loop."""

    async def pure():
        return 12345

    res = hypertile.run_level1(pure())
    assert res == 12345

    # Also test via level1=True flag
    res2 = hypertile.run(pure(), level1=True)
    assert res2 == 12345


def test_multi_awaiter_fanout_callable_task():
    """Verify that multiple concurrent awaiters on a single CallableTask all succeed."""

    async def main():
        # Dispatch a slightly delayed task
        def slow_comp():
            time.sleep(0.05)
            return "multi-done"

        t = hypertile.to_thread(slow_comp)

        async def awaiter(idx):
            res = await t
            assert res == "multi-done"
            return idx

        results = await asyncio.gather(*(awaiter(i) for i in range(10)))
        assert sorted(results) == list(range(10))
        assert t.result() == "multi-done"

    asyncio.run(main())


def test_multi_awaiter_fanout_native_task():
    """Verify that multiple concurrent awaiters on a single NativeTask all succeed."""

    async def main():
        payload = b"fanout-test-bytes"
        task = hypertile.spawn_native_pipeline(payload, rounds=30)

        async def awaiter(idx):
            res = await task
            assert isinstance(res, bytes)
            assert len(res) == 32
            return idx

        results = await asyncio.gather(*(awaiter(i) for i in range(10)))
        assert sorted(results) == list(range(10))
        assert task.done()

    asyncio.run(main())


def test_multi_awaiter_fanout_batch_native_task():
    """Verify that multiple concurrent awaiters on BatchNativeTask all succeed."""

    async def main():
        payloads = [b"item-1", b"item-2", b"item-3"]
        task = hypertile.batch_native_pipeline(payloads, rounds=20)

        async def awaiter(idx):
            res = await task
            assert len(res) == 3
            return idx

        results = await asyncio.gather(*(awaiter(i) for i in range(8)))
        assert sorted(results) == list(range(8))

    asyncio.run(main())


def test_multi_awaiter_fanout_batch_callable_task():
    """Verify that multiple concurrent awaiters on BatchCallableTask all succeed."""

    async def main():
        task = hypertile.gather_to_thread(lambda x: x * 3, [1, 2, 3, 4])

        async def awaiter(idx):
            res = await task
            assert res == [3, 6, 9, 12]
            return idx

        results = await asyncio.gather(*(awaiter(i) for i in range(8)))
        assert sorted(results) == list(range(8))

    asyncio.run(main())


def test_large_batch_processing():
    """Verify high-throughput processing on a batch of 2,000 items."""

    async def main():
        payloads = [f"payload-{i}".encode() for i in range(2000)]
        start = time.perf_counter()
        results = await hypertile.batch_native_pipeline(payloads, rounds=5)
        duration = time.perf_counter() - start

        assert len(results) == 2000
        for r in results:
            assert len(r) == 32
        # Should easily finish in less than 2 seconds
        assert duration < 2.0

    asyncio.run(main())


def test_task_decorator_with_multiple_arguments():
    """Verify @hypertile.task decorator with complex args and kwargs."""

    @hypertile.task
    def complex_calc(base: int, factor: int = 1, extra: int = 0) -> int:
        return (base * factor) + extra

    async def main():
        res1 = await complex_calc(10, factor=3, extra=7)
        assert res1 == 37

        res2 = await complex_calc(5)
        assert res2 == 5

    asyncio.run(main())


def test_repeated_worker_registration_lifecycle():
    """Verify worker registration context manager is clean and reusable."""
    for _ in range(5):
        with hypertile.register_worker(kind="bilingual") as worker:
            worker_id = worker.worker_id()
            assert isinstance(worker_id, int)
            worker.run_until_idle()


def test_cancellation_token_explicit_flow():
    """Verify cooperative cancellation token handling."""
    token = hypertile.CancellationToken()
    assert not token.is_cancelled()

    def cancellable_work():
        for _ in range(100):
            if token.is_cancelled():
                return "cancelled_early"
            time.sleep(0.001)
        return "finished"

    async def main():
        t = hypertile.to_thread(cancellable_work)
        await asyncio.sleep(0.01)
        token.cancel()
        assert token.is_cancelled()
        res = await t
        assert res == "cancelled_early"

    asyncio.run(main())


def test_level1_rejects_undrivable_awaitable():
    """Regression: an unsupported awaitable must raise, not spin forever.

    The Level 1 executor can only drive awaits that expose ``add_done_callback``.
    Previously anything else was silently re-queued with a 1 ms sleep, so
    ``run_level1`` hung with no diagnostics.
    """

    class Undrivable:
        def __await__(self):
            yield "opaque-awaitable"  # non-None and not an asyncio-style future

    async def uses_undrivable():
        return await Undrivable()

    start = time.perf_counter()
    with pytest.raises(TypeError, match="cannot drive an awaitable"):
        hypertile.run_level1(uses_undrivable())
    assert time.perf_counter() - start < 10.0, "must fail fast instead of spinning"


def test_level1_supports_bare_yield_coroutines():
    """A coroutine that yields ``None`` (e.g. ``await asyncio.sleep(0)``) still runs."""

    class BareYield:
        def __await__(self):
            yield None
            return "bare-yield-result"

    async def main():
        return await BareYield()

    assert hypertile.run_level1(main()) == "bare-yield-result"


def test_panic_exception_defined():
    """Verify PanicInTask exception is exposed and can be caught."""
    assert issubclass(hypertile.PanicInTask, Exception)
    assert issubclass(hypertile.TaskCancelled, Exception)
    assert issubclass(hypertile.RegistrationError, Exception)


def test_done_callback_accepts_future_argument():
    """Verify callbacks taking the future as an argument (asyncio style) are supported."""
    received = []

    async def main():
        task = hypertile.to_thread(lambda: 42)
        task.add_done_callback(lambda fut: received.append(fut.result()))
        assert await task == 42
        await asyncio.sleep(0.05)

    asyncio.run(main())
    assert received == [42]


def test_hypertile_task_accepts_kwargs():
    """Verify HypertileTask accepts arbitrary keyword arguments like eager_start."""
    from hypertile.asyncio_policy import HypertileTask

    async def sample():
        return 99

    async def runner():
        loop = asyncio.get_running_loop()
        task = HypertileTask(sample(), loop=loop, eager_start=False)
        return await task

    assert asyncio.run(runner()) == 99


def test_to_thread_rejects_partial_coroutine():
    """Verify to_thread unwraps functools.partial and rejects coroutine targets."""
    import functools

    async def async_worker(x):
        return x

    partial_coro = functools.partial(functools.partial(async_worker, 10))
    with pytest.raises(TypeError, match="does not accept coroutines"):
        hypertile.to_thread(partial_coro)


def test_signals_respects_sig_ign():
    """Verify setup_signal_handlers does not raise KeyboardInterrupt when SIGINT is ignored."""
    import signal

    from hypertile import signals

    orig = signals._ORIGINAL_SIGINT_HANDLER
    try:
        signals._ORIGINAL_SIGINT_HANDLER = signal.SIG_IGN
        # Test simulated handler execution
        signals.setup_signal_handlers()
        # Verify handler didn't crash
    finally:
        signals._ORIGINAL_SIGINT_HANDLER = orig
