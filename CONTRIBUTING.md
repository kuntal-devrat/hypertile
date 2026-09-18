# Contributing to Hypertile

Thanks for your interest in Hypertile! This document covers everything you need to
build, test and submit changes.

By participating you agree to the [Code of Conduct](CODE_OF_CONDUCT.md).

## Project layout

| Path | Contents |
|---|---|
| `hypertile-core/` | The executor: Chase-Lev deques, worker loop, task model, timer. No Python involved. |
| `hypertile-sys/` | The PyO3 extension module (`hypertile._hypertile_sys`) and the bilingual stepping engine. |
| `hypertile-capi/` | The `extern "C"` ABI, with the public header in `include/hypertile.h`. |
| `python/hypertile/` | The Python package: `__init__.py`, asyncio integration, signal handling, type stubs. |
| `tests/` | Python test suite. |
| `examples/`, `showcase/`, `benches/` | Runnable examples and benchmarks. |

`hypertile-core` never links the interpreter, so it can be tested without Python and is
the right place to land scheduler changes first.

## Prerequisites

- Rust **stable** (the workspace declares `rust-version = "1.79"` as its floor)
- Python **3.11+**. The free-threaded **3.14t** build is what the project is designed
  around, so please test there when touching scheduling or stepping code. (3.13t is not
  supported: PyO3 dropped free-threaded 3.13 in the same release that added 3.14 — see
  the CHANGELOG.)
- [`maturin`](https://maturin.rs) `>= 1.8` for building the extension
- [`ruff`](https://docs.astral.sh/ruff/) for Python linting

## Setting up

```bash
# Optional but recommended: uv manages interpreters and venvs quickly.
uv python install 3.14t
uv venv --python 3.14t .venv-314t

# Install build/test tooling (use your venv's python explicitly).
python -m pip install --upgrade "maturin>=1.8,<2.0" pytest ruff

# Build the extension in place.
maturin develop --release
```

On Windows the venv lives in `Scripts/` rather than `bin/`, and `maturin develop`
needs `VIRTUAL_ENV` set (or `--interpreter <path>`):

```powershell
$env:VIRTUAL_ENV = "$PWD\.venv-314t"
maturin develop --release
```

## Running the checks

Run the same gates CI runs:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test -p hypertile-core -p hypertile-capi --locked
ruff check .
pytest -v tests/
```

`hypertile-sys` is a `cdylib` that needs an interpreter to link, so it has no Rust
integration tests of its own — its behaviour is covered by the Python suite.

If you change the extension, rebuild it before running `pytest`, otherwise you are
testing a stale binary:

```bash
maturin develop --release && pytest -v tests/
```

## Testing guidance

- Scheduler or task-lifecycle change? Add a **Rust** test in
  `hypertile-core/tests/integration_tests.rs`. Tests that touch process-global state
  (the global pool and timer) belong in `global_runtime_tests.rs`, which is a separate
  test binary so it cannot interfere with the rest.
- A test that would `hang` on regression is only useful if it *fails*. Bound the wait
  (a channel `recv_timeout`, or a child interpreter for interpreter-exit behaviour) so
  CI reports a failure instead of timing out.
- Anything that can deadlock should be verified by temporarily reintroducing the bug and
  confirming the new test fails.
- Concurrency bugs are usually timing-dependent: prefer deterministic tests (fewer
  workers, no stealing) over sleeps.

### The race harness

`hypertile-core/tests/race_harness.rs` is the tool for scheduler, task-lifecycle and
registry changes. It has three layers:

- **`deterministic_*`** — pinned invariants that need no randomness: a wake is enqueued
  exactly once and never after completion, a `Pending`-without-wake task needs exactly
  one wake, and cancellation and completion are mutually exclusive. These run first and
  are what you extend when you can pin the property exactly.
- **`randomized_task_state_under_contention`** — several threads hammering `run`,
  wake-storms, cooperative cancellation and self-cancellation at once.
- **`randomized_worker_registry_churn`** — threads registering and deregistering while a
  producer keeps injecting, which is the regression test for the flush-on-deregistration
  path.

The randomized layers are seeded and replayed exactly: every iteration prints (and any
failure names) a seed, so a flake becomes a permanent test.

```bash
# One scenario, one seed, with output:
HYPERTILE_RACE_SEED=7 cargo test -p hypertile-core --test race_harness -- --nocapture

# Hunt: sweep many seeds at high iteration counts (this is what CI runs weekly).
scripts/race_stress.sh            # debug: keeps overflow checks on
scripts/race_stress.sh release    # faster, for long sweeps
```

Tuning knobs: `HYPERTILE_RACE_SEED`, `HYPERTILE_RACE_ITERATIONS`,
`HYPERTILE_RACE_CHURN_ITERATIONS`, `HYPERTILE_RACE_TIMEOUT_SECS`.

The harness is a **seeded stress test, not a model checker**. It makes the *schedule*
reproducible — which thread does what, in which order — but deliberately lets the OS
interleave them freely, because that is what surfaces real races.

Two properties make it usable, and any scenario you add must keep them:

1. **It fails instead of hanging.** Each iteration runs under a watchdog, because a
   harness that deadlocks when it finds a deadlock is worse than useless. A timeout is
   reported as a failure naming the seed.
2. **Failures name their seed**, so a report is actionable rather than "it went red once".

When a sweep finds a bad schedule, please add it as a *named regression test* (a
`deterministic_*` test if the property can be pinned, otherwise a scenario run at that
exact seed) rather than changing the seed CI pins.

**Every concurrency fix should be validated by reintroducing the bug** and confirming the
harness catches it. A concurrency test that passes against both the fixed and broken code
proves nothing. This is cheap: revert the fix, run one seed, restore.

Sanitizers are not part of every PR run — they need a nightly toolchain and `-Zbuild-std`,
and ThreadSanitizer needs a lot of shadow memory. The scheduled `Sanitizers` workflow runs
the harness under ThreadSanitizer and the core plus C ABI tests under AddressSanitizer; to
reproduce it locally on Linux:

```bash
rustup toolchain install nightly --component rust-src
RUSTFLAGS=-Zsanitizer=thread cargo test -Zbuild-std \
  --target x86_64-unknown-linux-gnu -p hypertile-core --test race_harness -- --test-threads=1
```

## Benchmarking

```bash
cargo bench -p hypertile-core            # queue / throughput
python benches/bench_python.py           # end-to-end Python harness
python showcase/free_threaded_showcase.py  # vs. ThreadPoolExecutor on 3.14t
```

Benchmark numbers in the README were measured on the author's machine (Windows AMD64,
8 cores / 16 threads). Re-measure locally before claiming an improvement; report the
machine, interpreter build (GIL vs free-threaded) and workload.

## Pull requests

- Keep changes focused; separate mechanical reformatting from behavioural changes.
- Update `CHANGELOG.md` under `[Unreleased]` for anything user-visible.
- Add or update tests and docs (`README.md`, `include/hypertile.h`, and the `.pyi` stubs
  when the Python surface changes).
- Make sure the checks above pass; a description of *why* the change is needed helps
  reviewers a lot.

## Reporting bugs

Please include:

- OS, CPU count, and whether Python is a free-threaded build (`hypertile.is_free_threaded()`)
- Python and Rust versions, and `hypertile.__version__`
- Whether the extension was installed from a wheel or built locally
- A minimal reproduction, and the full traceback

Deadlocks and hangs are the most valuable reports: include what the threads were doing if
you can get a stack dump.

## Security

Please do not open public issues for security problems — see [SECURITY.md](SECURITY.md).
