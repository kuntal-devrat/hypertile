"""Level 2 Asyncio Compatibility Integration for Hypertile (PRD_v2 §2.4).

Installs Hypertile's shared work-stealing pool behind standard asyncio event loops,
enabling direct continuation handoffs and capacity sharing.

.. warning::

   The ``asyncio`` event-loop *policy* API was deprecated in Python 3.14 and is
   scheduled for removal in 3.16. :func:`install` therefore degrades to a logged
   warning instead of raising on interpreters where the policy API is unavailable;
   :func:`hypertile.to_thread` and the native pipeline APIs keep working regardless,
   because they never depend on the event loop.
"""

import asyncio
import logging
import warnings
from collections.abc import Callable, Coroutine
from concurrent.futures import Future as ConcurrentFuture
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from .signals import register_token, setup_signal_handlers

logger = logging.getLogger("hypertile")


def _default_event_loop_policy_base() -> Any:
    """Return ``asyncio.DefaultEventLoopPolicy`` without emitting its deprecation warning.

    Subclassing the deprecated base still triggers a ``DeprecationWarning`` at import
    time, which would otherwise surface in every user's build logs. Returns ``object``
    when the policy API has been removed entirely (Python 3.16+).
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        return getattr(asyncio, "DefaultEventLoopPolicy", None) or object


class HypertileAsyncioExecutor(ThreadPoolExecutor):
    """Custom ThreadPoolExecutor routing tasks into Hypertile's shared pool."""

    def __init__(self, max_workers: int | None = None):
        super().__init__(max_workers=max_workers or 8)
        self._shutdown = False
        self._pending: set[ConcurrentFuture] = set()

    def submit(self, fn: Callable, *args, **kwargs) -> ConcurrentFuture:
        if self._shutdown:
            raise RuntimeError("cannot schedule new futures after shutdown")

        fut: ConcurrentFuture = ConcurrentFuture()
        self._pending.add(fut)

        def _cleanup(f):
            self._pending.discard(f)

        fut.add_done_callback(_cleanup)

        try:
            from . import _hypertile_sys

            task = _hypertile_sys.spawn_callable(
                fn,
                tuple(args) if args else None,
                dict(kwargs) if kwargs else None,
            )

            def _on_done():
                try:
                    res = task.result()
                    fut.set_result(res)
                except BaseException as e:  # noqa: BLE001
                    fut.set_exception(e)

            task.add_done_callback(_on_done)
        except Exception:  # noqa: BLE001
            # Native extension unavailable: fall back to a plain thread so asyncio
            # callers still observe a completed future.
            def wrapper():
                try:
                    res = fn(*args, **kwargs)
                    fut.set_result(res)
                except BaseException as e:  # noqa: BLE001
                    fut.set_exception(e)

            import threading

            t = threading.Thread(target=wrapper, daemon=True)
            t.start()

        return fut

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False):
        self._shutdown = True
        if cancel_futures:
            for f in list(self._pending):
                f.cancel()
        if wait and self._pending:
            import concurrent.futures

            concurrent.futures.wait(list(self._pending))


class HypertileTask(asyncio.Task):
    """Asyncio Task subclass with integrated cooperative cancellation token."""

    def __init__(self, coro, *args, **kwargs):
        try:
            super().__init__(coro, *args, **kwargs)
        except TypeError:
            # If the runtime's asyncio.Task does not support a passed keyword
            # argument (e.g., eager_start on Python 3.11), filter to core kwargs.
            filtered_kwargs = {k: v for k, v in kwargs.items() if k in ("loop", "name", "context")}
            super().__init__(coro, *args, **filtered_kwargs)
        try:
            from . import _hypertile_sys

            self.__hypertile_token__ = _hypertile_sys.CancellationToken()
            register_token(self.__hypertile_token__)
        except Exception:  # noqa: BLE001
            self.__hypertile_token__ = None

    def cancel(self, msg=None):
        if hasattr(self, "__hypertile_token__") and self.__hypertile_token__ is not None:
            self.__hypertile_token__.cancel()
        return super().cancel(msg) if msg is not None else super().cancel()


def hypertile_task_factory(
    loop: asyncio.AbstractEventLoop, coro: Coroutine, **kwargs
) -> asyncio.Task:
    """Task factory injecting cooperative cancellation tokens into asyncio Tasks."""
    return HypertileTask(coro, loop=loop, **kwargs)


class HypertileEventLoopPolicy(_default_event_loop_policy_base()):  # type: ignore[misc, valid-type]
    """Event loop policy that configures asyncio loops to colocate with Hypertile.

    Only usable where the (deprecated) policy API still exists; see the module
    docstring.
    """

    def new_event_loop(self) -> asyncio.AbstractEventLoop:
        loop = super().new_event_loop()
        configure_loop(loop)
        return loop

    def set_event_loop(self, loop: asyncio.AbstractEventLoop | None):
        if loop is not None:
            configure_loop(loop)
        super().set_event_loop(loop)


def configure_loop(loop: asyncio.AbstractEventLoop) -> None:
    """Point ``loop`` at Hypertile's pool and install the cancellation task factory."""
    try:
        loop.set_default_executor(HypertileAsyncioExecutor())
        loop.set_task_factory(hypertile_task_factory)  # type: ignore[arg-type]
    except Exception as e:  # noqa: BLE001
        logger.debug("Failed to configure loop with Hypertile settings: %s", e)


def install() -> None:
    """Install Hypertile as the global asyncio event loop policy (Level 2 Integration).

    Configures:
    1. Custom EventLoopPolicy with Hypertile's shared executor
    2. Cooperative cancellation task factory
    3. KeyboardInterrupt signal routing

    Safe to call repeatedly and on interpreters that removed the policy API; in that
    case a warning is logged and the runtime-integrated APIs remain available.
    """
    setup_signal_handlers()

    with warnings.catch_warnings():
        # Python 3.14 deprecates the policy API; installing it is still the only way to
        # affect asyncio globally before 3.16 removes the mechanism.
        warnings.simplefilter("ignore", DeprecationWarning)
        try:
            asyncio.set_event_loop_policy(HypertileEventLoopPolicy())
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "Hypertile: asyncio event-loop policy integration unavailable (%s). "
                "hypertile.to_thread() and the native pipeline APIs still work.",
                e,
            )

    from . import is_free_threaded

    if not is_free_threaded():
        logger.info(
            "Hypertile: Running in cooperative mode (standard GIL Python build detected per PRD v2 §2.5)."
        )
