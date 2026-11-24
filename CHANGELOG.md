# Changelog

## [Unreleased]

### Added

- Publish verified release distributions to PyPI with GitHub Actions Trusted Publishing.

### Fixed

- Measure cooldown expiry and entry capacity when a block acquires the registry lock.
- Correct the bilingual guides for async permits, retry deadlines, and periodic scheduling.

## [v1.0.0] - 2026-11-23

After FlowEvent's [v1.4.2](https://github.com/TahsinCr/python-flowevent/releases/tag/v1.4.2) release, its concurrency, resilience, and task and resource management components became FlowCoord. The [source at v1.4.2](https://github.com/TahsinCr/python-flowevent/tree/v1.4.2) is in the `TahsinCr/python-flowevent` repository, where these APIs shipped under the `eventflow` import. `flowcoord` is developed and versioned independently.

### Added

- Expose the package version as `flowcoord.__version__`.
- Add keyed limiters, single-flight execution, retry policies, circuit breakers, and cooldown tracking.
- Provide task and resource scopes for explicit cleanup ownership.
- Keep work scheduling and coalescing, bounded worker pools, and event-loop portals together in `flowcoord`.
- Retain operation metrics with optional C acceleration and provide standalone tests and benchmarks.
- Add thread-safe keyed concurrency and rate limiters, single-flight execution, a scheduler, and task scopes with bounded admission and retryable shutdown.
- Add a thread-based latest-task coordinator and serialize result callbacks per key in both modes.

- Bound admission for async task scopes, schedulers, and loop portals. Portals retain capacity until cancelled work finishes.
- Audit declared runtime dependencies in CI.

### Changed

- Give asyncio implementations `Async*` names and load them on demand from their `async_*` modules. Sync imports leave `asyncio` unloaded.
- Split retry, circuit breaker, cooldown, and resource scope APIs by execution mode. Share policies, state, validation, and snapshots in `base.py`.
- Keep one native metrics backend for both execution modes and skip counter staging for simple records on existing operation names.

### Fixed

- Handle races between portal future cancellation and task completion, and retrieve late task errors after cancellation.

- Release an async latest task's ownership after cancellation before its first step.
- Reject recursive same-key single-flight waits and retain running thread work across shutdown timeouts.
