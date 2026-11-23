[← Documentation Map](README.md) · [Previous: 05 · Scheduling, Coalescing, Observability, and C Core](scheduling-coalescing-and-observability.md)

# 06 · FlowCoord API Reference

Complete reference for all 46 public exports in `flowcoord` (`v1.0.0`).

---

## Table of Contents

- [Concurrency Limiters & SingleFlight](#concurrency-limiters--singleflight)
- [Resilience: Retry, Circuit Breaker, Cooldown & Deadline](#resilience-retry-circuit-breaker-cooldown--deadline)
- [Task Scopes, Resource Scopes, Work Pool & Portal](#task-scopes-resource-scopes-work-pool--portal)
- [Scheduling, Coalescing & Observability](#scheduling-coalescing--observability)
- [Statistics Dataclasses & Exceptions](#statistics-dataclasses--exceptions)

---

## Concurrency Limiters & SingleFlight

| Class | Constructor | Key Methods |
| --- | --- | --- |
| `KeyedConcurrencyLimiter[K]` | `(max_concurrency: int, *, observer=None)` | `acquire(key, timeout=None)`, `acquire_nowait(key)`, `release(key)`, `stats()`, `key_stats(key)`, `close()` |
| `AsyncKeyedConcurrencyLimiter[K]` | `(max_concurrency: int, *, observer=None)` | `acquire(key)`, `acquire_nowait(key)`, `async release(key)`, `stats()`, `key_stats(key)`, `async aclose()` |
| `KeyedRateLimiter[K]` | `(rate: float, period=1.0, burst=None, *, max_keys=4096, observer=None)` | `acquire(key, timeout=None)`, `try_acquire(key)`, `available(key)`, `prune()`, `stats()`, `close()` |
| `AsyncKeyedRateLimiter[K]` | `(rate: float, period=1.0, burst=None, *, max_keys=4096, observer=None)` | `async acquire(key)`, `try_acquire(key)`, `available(key)`, `prune()`, `stats()`, `async aclose()` |
| `SingleFlight[K]` | `()` | `run(key, factory)`, `stats()`, `close(timeout=None)` |
| `AsyncSingleFlight[K]` | `()` | `async run(key, factory)`, `stats()`, `async aclose()` |

[Back to top](#table-of-contents)

---

## Resilience: Retry, Circuit Breaker, Cooldown & Deadline

| Class | Constructor | Key Methods |
| --- | --- | --- |
| `RetryPolicy` / `AsyncRetryPolicy` | `(max_attempts=3, *, backoff='exponential', initial_delay=0.0, max_delay=60.0, jitter=0.0, observer=None)` | `run(operation, *, should_retry=None, deadline=None)` (`async def` on `Async*`) |
| `CircuitBreaker` / `AsyncCircuitBreaker` | `(failure_threshold=5, recovery_timeout=30.0, *, half_open_max=1, should_count=None, observer=None, clock=time.monotonic)` | `call(operation)` (`async def` on `Async*`), `state` (property), `stats()`, `reset()` |
| `CooldownRegistry` / `AsyncCooldownRegistry` | `(*, max_entries=4096, clock=time.monotonic)` | `block(key, duration, *, extend=True)`, `is_blocked(key)`, `remaining(key)`, `clear(key)`, `prune()`, `stats()`, `wait(key, timeout=None)` (`async def` on `Async*`) |
| `Deadline` | `Deadline.after(timeout, *, clock=...)` · `Deadline.at(ts, *, clock=...)` | `.remaining`, `.expired`, `.child(timeout)` |

[Back to top](#table-of-contents)

---

## Task Scopes, Resource Scopes, Work Pool & Portal

| Class | Constructor | Key Methods |
| --- | --- | --- |
| `TaskScope` | `(*, max_workers=4, max_pending=128, on_error=None, observer=None)` | `start(op, *, name=None, shutdown='cancel') -> TaskHandle`, `cancel(handle=None)`, `join(timeout=None)`, `close(timeout=None)`, `snapshot()`, `closed` |
| `AsyncTaskScope` | `(*, max_pending=128, on_error=None, observer=None)` | `start(op, *, name=None, shutdown='cancel') -> AsyncTaskHandle`, `cancel(handle=None)`, `async join(timeout=None)`, `async aclose(timeout=None)`, `snapshot()`, `closed` |
| `TaskHandle` / `AsyncTaskHandle` | Created by scope | `done`, `cancel()`, `join(timeout=None)` (`async def` + `.task` on async) |
| `LatestTaskCoordinator` | `(*, max_workers=4, max_pending=128)` | `submit(key, factory, on_result, *, cancel_previous=True, on_error=None, name=None)`, `cancel(key)`, `close(timeout=None)`, `snapshot()` |
| `AsyncLatestTaskCoordinator` | `()` | `submit(...) -> asyncio.Task`, `cancel(key)`, `async aclose(timeout=None)`, `snapshot()` |
| `ResourceScope` | `()` | `add(res, *, close='close', owned=True)`, `defer(fn)`, `detach(res)`, `pending_cleanup()`, `close()`, `closed` (`with` support) |
| `AsyncResourceScope` | `()` | `add(res, *, close='aclose', owned=True)`, `defer(fn)`, `detach(res)`, `pending_cleanup()`, `async aclose()`, `closed` (`async with` support) |
| `BoundedWorkPool` | `(*, max_workers, max_pending, observer=None, thread_name_prefix='FlowCoordWorker')` | `submit(fn, /, *args, **kwargs) -> Future`, `stats()`, `shutdown(wait=True, cancel_futures=False)`, `close(...)` (`with` support) |
| `AsyncLoopPortal` | `(*, name='FlowCoordLoopPortal', max_pending=128)` | `start()`, `submit(op, *args, **kwargs) -> Future`, `call(op, *args, timeout=None, **kwargs)`, `cancel(future)`, `close(timeout=None)`, `running` (`with` support) |

[Back to top](#table-of-contents)

---

## Scheduling, Coalescing & Observability

| Class | Constructor | Key Methods |
| --- | --- | --- |
| `Scheduler` | `(*, max_workers=4, max_jobs=4096, on_error=None, observer=None)` | `schedule_once(cb, *args, delay=0.0, name=None, **kwargs)`, `periodic(cb, *args, interval, overlap='skip', initial_delay=None, max_parallel=16, name=None, **kwargs)`, `cancel(handle)`, `close(timeout=None)`, `snapshot()` |
| `AsyncScheduler` | `(*, max_jobs=4096, on_error=None, observer=None)` | `schedule_once(...)`, `periodic(...)`, `cancel(handle)`, `async aclose(timeout=None)`, `snapshot()` |
| `ScheduledHandle` / `AsyncScheduledHandle` | Returned by scheduler | `done`, `cancel()`, `join(timeout=None)` (`.name` + `async join` on async) |
| `KeyedCoalescer[T, K]` | `(*, interval, key, merge, callback, max_pending_keys=128, on_error=None, observer=None)` | `push(value)`, `pending_count`, `stats()`, `flush(timeout=None)`, `close(*, flush=True, timeout=None)` |
| `OperationStats` | `(*, max_operations=128)` | `record(name, *, status='success', elapsed=0.0, queue_depth=None, dropped=0, retries=0, rejected=0, timeouts=0, shutdown_seconds=0.0)`, `snapshot()`, `reset()` |

[Back to top](#table-of-contents)

---

## Statistics Dataclasses & Exceptions

### Statistics & Observation Types

- `KeyedConcurrencyStats(keys, active, waiters, closed)` and `KeyedConcurrencyKeyStats(active, waiters)`
- `KeyedRateStats(keys, waiters, closed)`
- `SingleFlightStats(active, shared_waiters)`
- `RetryObservation(attempt, elapsed, status, error=None)`
- `CircuitBreakerStats(state, failures, probes, calls, rejected)`
- `CooldownStats(keys, active, waiters)`
- `TaskScopeSnapshot(active, completed, failed, cancelled, closing)`
- `LatestTaskSnapshot(active, completed, discarded_results)`
- `WorkPoolStats(workers, pending, running, completed, rejected, closed)`
- `SchedulerStats(active, completed, failed, cancelled, closed)`
- `CoalescerStats(pending_keys, submitted, merged, delivered, failures, rejected, discarded, unreported_failures, closed)`
- `OperationMetrics(calls, successful, failed, cancelled, total_seconds, max_seconds, queue_depth, max_queue_depth, dropped, retries, rejected, timeouts, shutdown_seconds)`
- `OperationStatsSnapshot(operations: Mapping[str, OperationMetrics])`

### Exceptions

- `AsyncLoopPortalClosedError(RuntimeError)`: Raised when submitting work to a closed `AsyncLoopPortal`.
- `CircuitOpenError(RuntimeError)`: Raised when a `CircuitBreaker` or `AsyncCircuitBreaker` rejects calls while open or saturated.

[Back to top](#table-of-contents)

---

**Previous:** [05 · Scheduling, Coalescing, Observability, and C Core](scheduling-coalescing-and-observability.md) · **Map:** [Documentation Map](README.md)
