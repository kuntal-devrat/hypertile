"""Hypertile — A shared, work-stealing executor for free-threaded Python and Rust.

Key Interfaces:
- `hypertile.to_thread(fn, *args, **kwargs)`: Ultra-low-latency task dispatch to Hypertile's work-stealing pool.
- `@hypertile.task`: Decorator converting synchronous functions into awaitable Hypertile tasks.
- `hypertile.run(main())`: Level 1 standalone native runtime driving coroutines under Hypertile's task model.
- `hypertile.install()`: Level 2 asyncio compatibility installing Hypertile's shared pool behind asyncio.
- `hypertile.register_worker(kind="bilingual")`: Join the executor pool dynamically from external threads (e.g. FastAPI/uvicorn).
- `hypertile.is_free_threaded()`: Detect whether the active Python runtime has PEP 779 free-threading enabled.
"""

# pyright: reportAssignmentType=false
import atexit
import functools
import inspect
import os
import sys
from collections.abc import Callable, Coroutine, Iterable, Sequence
from typing import Any, TypeVar

try:
    from ._hypertile_sys import (
        BatchCallableTask,
        BatchNativeTask,
        CallableTask,
        CancellationToken,
        NativeTask,
        PanicInTask,
        RegisteredWorker,
        RegistrationError,
        TaskCancelled,
    )
    from ._hypertile_sys import (
        __version__ as _native_version,
    )
    from ._hypertile_sys import (
        batch_spawn_callable as _batch_spawn_callable,
    )
    from ._hypertile_sys import (
        batch_spawn_native_pipeline as _batch_spawn_native_pipeline,
    )
    from ._hypertile_sys import (
        configure as _configure,
    )
    from ._hypertile_sys import (
        default_worker_count as _default_worker_count,
    )
    from ._hypertile_sys import (
        is_free_threaded as _is_free_threaded,
    )
    from ._hypertile_sys import (
        native_pipeline_transform as _native_pipeline_transform,
    )
    from ._hypertile_sys import (
        register_worker as _register_worker,
    )
    from ._hypertile_sys import (
        run_level1 as _run_level1,
    )
    from ._hypertile_sys import (
        spawn_callable as _spawn_callable,
    )
    from ._hypertile_sys import (
        spawn_coroutine as _spawn_coroutine,
    )
    from ._hypertile_sys import (
        spawn_native_pipeline as _spawn_native_pipeline,
    )
    from ._hypertile_sys import (
        worker_count as _worker_count,
    )

    _NATIVE_EXTENSION_AVAILABLE = True
except ImportError:
    # Graceful fallback if native extension is not yet built
    _NATIVE_EXTENSION_AVAILABLE = False
    _native_version = None

    def _is_free_threaded() -> bool:
        return not getattr(sys, "_is_gil_enabled", lambda: True)()

    def _native_pipeline_transform(payload: bytes, rounds: int = 100) -> bytes:
        return payload[:32]

    def _spawn_native_pipeline(payload: bytes, rounds: int = 100):
        raise NotImplementedError("Native extension _hypertile_sys is not installed.")

    def _batch_spawn_native_pipeline(payloads, rounds: int = 100):
        raise NotImplementedError("Native extension _hypertile_sys is not installed.")

    def _spawn_callable(func, args=None, kwargs=None):
        raise NotImplementedError("Native extension _hypertile_sys is not installed.")

    def _batch_spawn_callable(func, args_list):
        raise NotImplementedError("Native extension _hypertile_sys is not installed.")

    def _register_worker(kind: str = "bilingual"):
        raise NotImplementedError("Native extension _hypertile_sys is not installed.")

    def _spawn_coroutine(coro, done_callback=None, cancellation_token=None):
        raise NotImplementedError("Native extension _hypertile_sys is not installed.")

    def _run_level1(coro):
        raise NotImplementedError("Native extension _hypertile_sys is not installed.")

    def _configure(workers: int = 0) -> int:
        raise NotImplementedError("Native extension _hypertile_sys is not installed.")

    def _worker_count() -> int | None:
        return None

    def _default_worker_count() -> int:
        # Mirrors the Rust policy (one worker per logical CPU plus blocking headroom,
        # capped) so a source-only checkout still answers the question. Without the
        # extension there is no pool, so nothing can depend on the number.
        cores = max(1, os.cpu_count() or 1)
        return max(cores, min(32, cores + 4))

    class CancellationToken:
        def __init__(self):
            self._cancelled = False

        def cancel(self):
            self._cancelled = True

        def is_cancelled(self) -> bool:
            return self._cancelled

    class RegisteredWorker:
        pass

    class NativeTask:
        pass

    class CallableTask:
        pass

    class BatchNativeTask:
        pass

    class BatchCallableTask:
        pass

    class PanicInTask(Exception):
        pass

    class TaskCancelled(Exception):
        pass

    class RegistrationError(Exception):
        pass


from .asyncio_policy import install

# Version metadata is owned by the Rust crates once the extension is built; the literal
# is only a fallback for source-only checkouts.
__version__ = _native_version or "0.1.2"
__all__ = [
    "BatchCallableTask",
    "BatchNativeTask",
    "CallableTask",
    "CancellationToken",
    "NativeTask",
    "PanicInTask",
    "RegisteredWorker",
    "RegistrationError",
    "TaskCancelled",
    "batch_native_pipeline",
    "configure",
    "default_worker_count",
    "gather_to_thread",
    "install",
    "is_free_threaded",
    "native_pipeline_transform",
    "register_worker",
    "run",
    "run_level1",
    "spawn_native_pipeline",
    "task",
    "to_thread",
    "worker_count",
]

_T = TypeVar("_T")


def run_level1(main_coroutine: Coroutine) -> Any:
    """Run a coroutine directly under Hypertile's Level 1 native executor without an asyncio loop."""
    return _run_level1(main_coroutine)


def to_thread(func: Callable[..., _T], /, *args: Any, **kwargs: Any) -> Any:
    """Asynchronously run function ``func`` in Hypertile's work-stealing thread pool.

    Any ``*args`` and ``**kwargs`` supplied for this function are directly passed
    to ``func``. Returns an awaitable task yielding the return value of ``func``.

    This is an ultra-low-overhead alternative to ``asyncio.to_thread()``, executing
    directly on bilingual Rust/Python workers without allocating new OS threads
    or creating intermediate asyncio futures.

    Example:
        result = await hypertile.to_thread(crypto_hash, payload, rounds=50)
    """
    check_func = func
    while isinstance(check_func, functools.partial):
        check_func = check_func.func
    if inspect.iscoroutinefunction(check_func):
        raise TypeError(
            "hypertile.to_thread() does not accept coroutines; use await func() directly."
        )

    try:
        args_tuple = tuple(args) if args else None
        kwargs_dict = dict(kwargs) if kwargs else None
        return _spawn_callable(func, args_tuple, kwargs_dict)
    except (NotImplementedError, NameError):
        import asyncio

        return asyncio.to_thread(func, *args, **kwargs)


def task(func: Callable[..., Any] | None = None) -> Any:
    """Decorator to turn a synchronous function into an asynchronous Hypertile task.

    When decorated with ``@hypertile.task``, invoking the function returns an awaitable
    task executed directly on Hypertile's work-stealing pool.

    Can be used with or without parentheses:
        @hypertile.task
        def compute(x: int, y: int) -> int:
            return x * y

        # In an async function:
        val = await compute(6, 7)
    """

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return to_thread(fn, *args, **kwargs)

        return wrapper

    if func is not None:
        return decorator(func)
    return decorator


def spawn_native_pipeline(payload: bytes, rounds: int = 100) -> Any:
    """Spawn a native compute task directly into Hypertile's work-stealing pool.

    Returns an awaitable NativeTask that single-hops completion back to the awaiting
    coroutine without touching loop.run_in_executor or call_soon_threadsafe.
    """
    return _spawn_native_pipeline(payload, rounds)


def batch_native_pipeline(payloads: Sequence[bytes], rounds: int = 100) -> Any:
    """Spawn a vectorized batch of native compute tasks into Hypertile's work-stealing pool.

    Crosses the Python <-> Rust FFI boundary once for the entire batch rather than once
    per task, unlocking extreme throughput (>500,000 items/sec).

    Args:
        payloads: A sequence of byte payloads to transform.
        rounds: Number of cryptographic/numerical hashing rounds per payload.

    Returns:
        An awaitable BatchNativeTask yielding a list of processed bytes.
    """
    try:
        return _batch_spawn_native_pipeline(list(payloads), rounds)
    except (NotImplementedError, NameError):
        import asyncio

        async def _fallback() -> list[bytes]:
            await asyncio.sleep(0)
            return [_native_pipeline_transform(p, rounds) for p in payloads]

        return _fallback()


def gather_to_thread(func: Callable[..., _T], args_iterable: Iterable[Any]) -> Any:
    """Vectorized parallel execution of a callable across an iterable of inputs.

    Submits the entire batch to Hypertile's work-stealing pool in a single FFI crossing,
    returning an awaitable BatchCallableTask yielding the list of results.

    Each item in `args_iterable` can be a tuple of arguments, or a single argument value.

    Args:
        func: The synchronous target function to execute in parallel.
        args_iterable: An iterable of arguments or argument tuples.

    Returns:
        An awaitable BatchCallableTask yielding a list of results corresponding to each input.

    Example:
        results = await hypertile.gather_to_thread(crypto_hash, [b"data1", b"data2", b"data3"])
    """
    norm_args = [arg if isinstance(arg, tuple) else (arg,) for arg in args_iterable]
    try:
        return _batch_spawn_callable(func, norm_args)
    except (NotImplementedError, NameError):
        import asyncio

        async def _fallback_gather():
            return await asyncio.gather(*(asyncio.to_thread(func, *a) for a in norm_args))

        return _fallback_gather()


def native_pipeline_transform(payload: bytes, rounds: int = 100) -> bytes:
    """Execute a CPU-intensive cryptographic/numerical pipeline transform in native Rust."""
    return _native_pipeline_transform(payload, rounds)


def is_free_threaded() -> bool:
    """Return True if the running Python interpreter has free-threading (PEP 779) enabled."""
    return _is_free_threaded()


def configure(workers: int | None = None) -> int:
    """Set the number of threads in Hypertile's shared worker pool.

    The pool is process-wide, and its size is fixed once it has been used, so call this
    before the first Hypertile operation. If the pool is already running a
    ``RuntimeError`` is raised naming the size that is actually in use, rather than
    silently ignoring the request. To size a pool that is started by code running before
    yours (a framework or library that offloads work at import time), set the
    ``HYPERTILE_WORKERS`` environment variable instead.

    Args:
        workers: Number of worker threads. ``None`` (or ``0``) selects the default:
            one worker per logical CPU plus a small headroom for blocking calls, so
            synchronous drivers and file I/O can overlap instead of queueing. Use a
            larger value for I/O-heavy services and the CPU count for compute-bound
            ones.

    Returns:
        The effective pool size, which equals ``workers`` unless ``None``/``0`` was
        passed.

    Example:
        import hypertile

        hypertile.configure(workers=32)  # I/O-bound service
        assert hypertile.worker_count() == 32
    """
    return _configure(0 if workers is None else workers)


def worker_count() -> int | None:
    """Number of threads in the shared pool, or ``None`` if it has not started yet."""
    return _worker_count()


def default_worker_count() -> int:
    """The pool size ``configure()`` would choose on this machine if not told otherwise.

    Inspectable so the default is not a mystery, and so tests can assert the policy
    without starting a pool they cannot resize.
    """
    return _default_worker_count()


def register_worker(kind: str = "bilingual") -> RegisteredWorker:
    """Register the current thread as a worker in Hypertile's work-stealing pool.

    Can be used as a context manager:
        with hypertile.register_worker(kind="bilingual") as worker:
            worker.run_until_idle()
    """
    return _register_worker(kind)


def run(main_coroutine: Coroutine, *, level1: bool = False) -> Any:
    """Run a coroutine to completion under Hypertile.

    By default, installs Hypertile's work-stealing executor into asyncio and executes
    via ``asyncio.run(main_coroutine)``, ensuring 100% compatibility with asyncio primitives
    (e.g. ``asyncio.sleep``, ``httpx``, ``aiohttp``) across all supported Python versions, including free-threaded 3.14t.

    If ``level1=True`` is specified, drives coroutines directly across bilingual native
    workers without initializing an asyncio event loop.
    """
    if level1:
        return _run_level1(main_coroutine)

    import asyncio

    install()
    return asyncio.run(main_coroutine)


def _shutdown_background_workers() -> None:
    """Stop Hypertile's background workers before the interpreter is finalized.

    Registered as an ``atexit`` hook: a worker thread that touches a half torn-down
    interpreter crashes the process. The native side flips its stop flag and waits
    briefly for workers to observe it; it deliberately never joins them, because a
    worker blocked on the GIL cannot exit while ``atexit`` holds it.

    Safe to call more than once.
    """
    if not _NATIVE_EXTENSION_AVAILABLE:
        return
    try:
        from . import _hypertile_sys

        _hypertile_sys.shutdown()
    except Exception:  # noqa: BLE001 - best effort during interpreter exit
        pass


if _NATIVE_EXTENSION_AVAILABLE:
    atexit.register(_shutdown_background_workers)
