## What changed

<!-- A short summary of the change and the problem it solves. -->

## Why

<!-- The motivation. For a bug fix, describe the failure mode; "fixes a bug" is not enough. -->

Fixes #

## How it was verified

<!-- Commands you ran and what you observed. For concurrency changes, say how you
     convinced yourself the race/deadlock is actually gone. -->

## Checklist

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --workspace --all-targets --locked -- -D warnings`
- [ ] `cargo test -p hypertile-core -p hypertile-capi --locked`
- [ ] `ruff check .`
- [ ] `pytest -v tests/` (after rebuilding the extension with `maturin develop --release`)
- [ ] `CHANGELOG.md` updated under `[Unreleased]` for user-visible changes
- [ ] Public API changes reflected in docs and the `.pyi` stubs / `include/hypertile.h`
