from collections.abc import Callable, Coroutine, Iterable, Sequence
from typing import (
    Any,
    ParamSpec,
    TypeVar,
    overload,
)

from ._hypertile_sys import (
    BatchCallableTask as BatchCallableTask,
)
from ._hypertile_sys import (
    BatchNativeTask as BatchNativeTask,
)
from ._hypertile_sys import (
    CallableTask as CallableTask,
)
from ._hypertile_sys import (
    CancellationToken as CancellationToken,
)
from ._hypertile_sys import (
    NativeTask as NativeTask,
)
from ._hypertile_sys import (
    PanicInTask as PanicInTask,
)
from ._hypertile_sys import (
    RegisteredWorker as RegisteredWorker,
)
from ._hypertile_sys import (
    RegistrationError as RegistrationError,
)
from ._hypertile_sys import (
    TaskCancelled as TaskCancelled,
)
from ._hypertile_sys import (
    is_free_threaded as is_free_threaded,
)
from ._hypertile_sys import (
    native_pipeline_transform as native_pipeline_transform,
)
from ._hypertile_sys import (
    register_worker as register_worker,
)
from ._hypertile_sys import (
    spawn_native_pipeline as spawn_native_pipeline,
)

__version__: str

# Internal, but relied on by the test suite and by embedders that need to stop the
# background pool explicitly.
_NATIVE_EXTENSION_AVAILABLE: bool

def _shutdown_background_workers() -> None: ...

_P = ParamSpec("_P")
_T = TypeVar("_T")

def batch_native_pipeline(payloads: Sequence[bytes], rounds: int = 100) -> BatchNativeTask: ...
def to_thread(func: Callable[_P, _T], /, *args: _P.args, **kwargs: _P.kwargs) -> CallableTask: ...
def gather_to_thread(
    func: Callable[..., _T], args_iterable: Iterable[Any]
) -> BatchCallableTask: ...
@overload
def task(func: Callable[_P, _T]) -> Callable[_P, CallableTask]: ...
@overload
def task() -> Callable[[Callable[_P, Any]], Callable[_P, CallableTask]]: ...
def install() -> None: ...
def run(main_coroutine: Coroutine[Any, Any, _T], *, level1: bool = ...) -> _T: ...
def run_level1(main_coroutine: Coroutine[Any, Any, _T]) -> _T: ...
def configure(workers: int | None = None) -> int: ...
def worker_count() -> int | None: ...
def default_worker_count() -> int: ...
