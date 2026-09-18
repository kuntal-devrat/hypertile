"""Signal handling and cooperative cancellation coordination for Hypertile."""

import contextlib
import signal
from weakref import WeakSet

_ACTIVE_TOKENS: WeakSet = WeakSet()
_ORIGINAL_SIGINT_HANDLER = None


def register_token(token) -> None:
    """Register an active cancellation token to be triggered on SIGINT."""
    _ACTIVE_TOKENS.add(token)


def setup_signal_handlers() -> None:
    """Install signal handlers that forward KeyboardInterrupt to cancellation tokens."""
    global _ORIGINAL_SIGINT_HANDLER
    if _ORIGINAL_SIGINT_HANDLER is None:
        try:
            _ORIGINAL_SIGINT_HANDLER = signal.getsignal(signal.SIGINT)

            def sigint_handler(sig, frame):
                # Trigger cancellation on all active in-flight native tasks
                for token in list(_ACTIVE_TOKENS):
                    with contextlib.suppress(Exception):
                        token.cancel()
                if _ORIGINAL_SIGINT_HANDLER == signal.SIG_IGN:
                    return
                if callable(_ORIGINAL_SIGINT_HANDLER):
                    _ORIGINAL_SIGINT_HANDLER(sig, frame)
                else:
                    raise KeyboardInterrupt()

            signal.signal(signal.SIGINT, sigint_handler)
        except (ValueError, AttributeError):
            # Not in main thread or platform limitation
            pass
