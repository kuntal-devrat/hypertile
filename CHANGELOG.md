# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.2] - 2026-09-18

### Fixed

- **Python 3.14 / 3.14t could not be built at all.** `pyo3` was pinned to `0.23`, whose
  build script refuses interpreters newer than 3.13 — and the
  `PYO3_USE_ABI3_FORWARD_COMPATIBILITY` escape hatch explicitly cannot apply to
  free-threaded builds. The dependency is now `pyo3 0.29`, so the free-threaded
  platforms this project targets actually build. (`Python::with_gil` →
  `Python::attach`, `allow_threads` → `detach`, explicit `from_py_object` /
  `skip_from_py_object` on every `#[pyclass]`.)
- Cancelling a task from inside its own `poll` deadlocked the worker: `TaskCell::run`
  held the future mutex across `poll`, and `JoinHandle::cancel` takes the same
  (non-reentrant) mutex. The executor no longer holds the lock while user code runs.
- Re-registering a thread as a worker silently dropped every task still sitting in its
  previous local deque, leaving their `JoinHandle`s pending forever. The old deque is
  now drained into the shared injector before the new one is installed, and worker
  guards are tagged with their worker ID so a stale guard can never deregister (or
  flush) a newer worker's queue.
- `add_done_callback` could silently drop a callback. The "check completion, then push"
  sequence on `NativeTask`, `CallableTask`, `BatchNativeTask` and `BatchCallableTask`
  was not atomic with the producer's drain; the check is now repeated under the
  callback lock.
- A Rust panic inside a `gather_to_thread` chunk never decremented the batch counter,
  so awaiting the batch hung forever. Chunks are now panic-contained and surface
  `PanicInTask` instead.
- The Level 1 executor silently re-queued undrivable awaitables on a 1 ms timer, so
  `hypertile.run(..., level1=True)` hung with no diagnostics. It now fails fast with a
  `TypeError` naming the unsupported type. Bare `yield None` (for example
  `await asyncio.sleep(0)`) is stepped immediately instead of sleeping.
- `hypertile_init(num_workers)` ignored its argument, always sizing the pool to the
  logical CPU count. The requested worker count is now honoured (first call wins).
- The C ABI's `hypertile_worker_run_until_idle` held a `RefCell` borrow across user
  code, so a task that re-registered or deregistered its own thread panicked.
- Worker IDs started at `0`, which the C ABI documents as the "failure" return value.
  IDs now start at 1.
- Importing `hypertile` emitted a `DeprecationWarning` on Python 3.14 for subclassing
  `asyncio.DefaultEventLoopPolicy`. The warning is suppressed and `install()` now
  degrades to a logged warning (rather than an exception) on interpreters where the
  deprecated policy API has been removed.

### Changed

- **The default worker count is no longer just the logical CPU count.** It is now the CPU
  count plus a small headroom for blocking calls (capped at 32, never below one worker per
  CPU). One pool serves both compute and blocking offload, and those want opposite sizes:
  a CPU-sized pool capped `to_thread` concurrency at 8 on an 8-thread machine, making 24
  blocking tasks take 0.31 s where 12 workers took 0.21 s — slower than the
  `asyncio.to_thread` default it replaces, on the very operation `to_thread` exists for.
  The CPU-bound side is unaffected (0.95-1.00 s vs 0.95-0.97 s for the same batch on both
  pool sizes). Set `HYPERTILE_WORKERS` or call `hypertile.configure(workers=...)` to pick
  either end of the trade-off explicitly.
- **Python 3.13t (free-threaded) support has been dropped.** PyO3 removed free-threaded
  3.13 support in the same release that added 3.14, so no PyO3 version can target both.
  Hypertile now tracks 3.14t, which is the CPython release that declared free-threading
  supported (PEP 779). If you need 3.13t, stay on Hypertile 0.1.1 — it pins PyO3 0.23 and
  cannot build for 3.14. This is a hard upstream constraint, not a local regression.
- Worker threads are stopped by an `atexit` hook before the interpreter finalizes, so
  they can no longer touch a half torn-down interpreter. The hook never joins workers,
  because a worker blocked on the interpreter lock cannot exit while the caller holds it.
- The global runtime and timer use `OnceLock` instead of `static mut` + raw pointers,
  removing all `unsafe` from the runtime plumbing.
- Hot-path spawn no longer clones a `TaskHandle` (`Arc` refcount traffic) when the
  caller is already a worker.
- `hypertile_version()` and `_hypertile_sys.__version__` are derived from the crate
  version instead of hard-coded strings.
- The workspace declares a `rust-version` floor and inherits shared package metadata;
  `hypertile-core/Cargo.lock` (a stale nested lock file) was removed.
- `cargo fmt` is now enforced across the workspace, and `ruff` across all Python.
- Packaging metadata was modernized: PEP 639 SPDX `license` expression, `license-files`,
  project URLs, keywords, and a `Typing :: Typed` classifier. The wheel now reports
  `Metadata-Version: 2.4` with a `License-Expression`.
- CI now gates on `cargo fmt`, `cargo clippy -D warnings` and `ruff`, runs the C ABI
  tests, and exercises GIL (3.11–3.14) plus free-threaded **3.14t** on Linux, macOS and
  Windows.

### Removed

- The unused `slab`/`ObjectPool` module, and the unused `futures-core`,
  `futures-util`, `futures-lite`, `rand` and `thiserror` dependencies.

### Added

- **The shared pool's size is now configurable from Python**: `hypertile.configure(workers=N)`,
  with `hypertile.worker_count()` to inspect the running size and
  `hypertile.default_worker_count()` to see what the default would be. The `HYPERTILE_WORKERS`
  environment variable covers pools started by code running before yours (a framework that
  offloads work at import time). Resizing is refused with a `RuntimeError` rather than being
  silently ignored, because the pool's size is fixed once it is used.
  `hypertile_core::configure_global_runtime` and
  `hypertile_core::RuntimeAlreadyStarted` expose the same behaviour to Rust embedders.
- `pyright` now type-checks the whole Python surface in CI, so the shipped `.pyi` stubs
  cannot silently drift from the implementation. The stubs were missing
  `_hypertile_sys.__version__` (and `shutdown`), `hypertile._NATIVE_EXTENSION_AVAILABLE`
  and `hypertile._shutdown_background_workers`.
- Community and project files: `CONTRIBUTING.md`, `SECURITY.md` (with the threat model),
  `CODE_OF_CONDUCT.md`, `CHANGELOG.md`, issue/PR templates and Dependabot config.
- A deterministic + seeded concurrency harness (`hypertile-core/tests/race_harness.rs`)
  for the task state machine and the worker registry: pinned wake-dedup, lost-wake-up and
  cancel/completion-exclusivity invariants, plus randomized contention and
  registry-churn scenarios. Every iteration runs under a watchdog that reports a
  deadlock as a seed-named failure instead of hanging, and every scenario is replayable
  from its seed. Both the task-loss and self-cancel-deadlock fixes above are verified by
  reintroducing the bug and watching the harness catch it.
- `scripts/race_stress.sh`, which sweeps many seeds at high iteration counts (the
  harness's own runs stay short enough for every PR), and a scheduled
  `Sanitizers` workflow that runs the harness under ThreadSanitizer and the core plus C ABI
  tests under AddressSanitizer. The per-PR gate pins `HYPERTILE_RACE_SEED` so it cannot
  flake, while the scheduled sweep explores freely.
- Regression tests for the self-cancel deadlock, the re-registration flush, worker-count
  handling, callback registration races, version-metadata consistency and clean
  interpreter exit.

## [0.1.1] - 2026-09-10

### Added

- `hypertile-capi`: `extern "C"` ABI plus `include/hypertile.h` for C, C++, Go (cgo)
  and Zig embedders.
- Panic containment across native, callable and batch task types.

### Fixed

- Runtime deadlocks on chained wakes, and deprecation warnings under newer runtimes.

## [0.1.0] - 2026-09-09

### Added

- Initial release: shared work-stealing executor with Chase-Lev deques, a global
  injector, single-hop continuation handoff, dynamic worker registration, panic
  containment, `CachePadded` hot state, batch stealing and an adaptive chunked
  vectorized dispatch path.

[Unreleased]: https://github.com/kuntal-devrat/hypertile/compare/v0.1.2...HEAD
[0.1.2]: https://github.com/kuntal-devrat/hypertile/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/kuntal-devrat/hypertile/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/kuntal-devrat/hypertile/releases/tag/v0.1.0
