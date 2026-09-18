# Security Policy

## Reporting a vulnerability

Please **do not** open a public issue for security problems.

Use GitHub's private reporting flow: go to the
[Security tab](https://github.com/kuntal-devrat/hypertile/security) →
**Report a vulnerability**. If you cannot use that, email the maintainers listed in
`Cargo.toml`'s `authors` field with a description and a reproduction.

Please include:

- affected version(s) and platform,
- whether Python is a free-threaded build,
- whether the extension was built from source or installed from a wheel,
- a minimal reproduction or proof of concept,
- the impact you believe the issue has.

We aim to acknowledge reports within 5 working days and to agree on a disclosure
timeline with you. Credit is given in the release notes unless you prefer otherwise.

## Supported versions

Hypertile is pre-1.0. Fixes are released against the latest published version; please
reproduce on `main` or the newest release before reporting.

## Threat model

Hypertile is an in-process concurrency library, not a sandbox or a privilege boundary.

**In scope**

- Memory unsafety, data races, and undefined behaviour in the Rust crates, including the
  `extern "C"` ABI surface.
- Use-after-free or reference-counting errors in the PyO3 bindings, especially across the
  thread boundary.
- Deadlocks, hangs, or task loss reachable from normal documented use.
- Crashes at interpreter shutdown caused by worker threads outliving the interpreter.
- Unsound `unsafe` code in `hypertile-capi` (it takes raw pointers and function pointers
  from foreign callers).
- Any case where a panic in one task corrupts unrelated state or takes down the pool.

**Out of scope**

- Arbitrary code execution or data exfiltration by code you deliberately pass to
  `hypertile.to_thread`, `gather_to_thread`, `spawn_callable`, or the C `hypertile_spawn`.
  Such code already runs with your process's privileges by design.
- Consequences of passing invalid pointers to the C ABI. `hypertile.h` documents the
  caller's obligations; violating them is undefined behaviour, not a vulnerability.
- Denial of service that requires the attacker to already control the workload submitted
  to the pool.
- Issues in dependencies (please report those upstream), unless Hypertile uses the
  dependency in an unsound way.

## Safety notes for embedders

- The C ABI is `unsafe`: `work` must be a valid, thread-safe function pointer, and `arg`
  must stay alive for the duration of the task. Work functions may be invoked on any
  worker thread.
- `hypertile_shutdown` must not race with other Hypertile calls from the same process.
- A panicking work function is caught and reported as `HYPERTILE_ERR_PANIC`; it does not
  unwind across the C boundary.
