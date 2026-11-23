[← Documentation Map](README.md) · [Previous: 03 · Resilience](resilience-and-recovery.md) · [Next: 05 · Scheduling, Coalescing, Observability, and C Core →](scheduling-coalescing-and-observability.md)

# 04 · Tasks, Resource Scopes, Work Pool, and Portal

This guide covers structured task ownership (`TaskScope`, `LatestTaskCoordinator`), retryable LIFO cleanup (`ResourceScope`), bounded thread pools (`BoundedWorkPool`), and thread-to-asyncio bridging (`AsyncLoopPortal`).

---

## Table of Contents

- [The Problem: Fire-and-Forget Tasks and Orphaned Work](#the-problem-fire-and-forget-tasks-and-orphaned-work)
- [`TaskScope` and `AsyncTaskScope`](#taskscope-and-asynctaskscope)
- [`LatestTaskCoordinator` and `AsyncLatestTaskCoordinator`](#latesttaskcoordinator-and-asynclatesttaskcoordinator)
- [`ResourceScope` and `AsyncResourceScope`](#resourcescope-and-asyncresourcescope)
- [`BoundedWorkPool` — Why Plain `ThreadPoolExecutor` Isn't Enough](#boundedworkpool--why-plain-threadpoolexecutor-isnt-enough)
- [`AsyncLoopPortal` — Safe Thread-to-Loop Bridge](#asyncloopportal--safe-thread-to-loop-bridge)

---

## The Problem: Fire-and-Forget Tasks and Orphaned Work

Spawning unowned `asyncio.create_task(job())` or `threading.Thread(...).start()` causes three classic bugs:

1. **Silent drops and GC cancellation:** Unreferenced tasks can be garbage-collected mid-flight, and unhandled failures go unnoticed.
2. **Dangling shutdown cleanup:** Background jobs stay half-finished when requests or services exit.
3. **Unbounded executor OOM:** Standard `ThreadPoolExecutor` has no queue limit—slow consumers queue unlimited `Future`s until memory runs out.

[Back to top](#table-of-contents)

---

## `TaskScope` and `AsyncTaskScope`

Own all background work under a single bounded scope:

- **`max_pending`** (default `128`): Enforces backpressure; `start()` raises `RuntimeError` immediately when full.
- **`shutdown="cancel"` vs `"drain"`:** Per-task shutdown policy specifying whether closing the scope cancels queued/running work or waits for completion.
- **Cancellation slot accounting (`AsyncTaskScope`):** Cancelled tasks continue occupying their `max_pending` slot until `finally` cleanup completes, preventing hidden capacity leaks.
- Returns `TaskHandle` / `AsyncTaskHandle` with `.join(timeout=...)`, `.cancel()`, `.done`, plus optional scope-level `on_error(handle, exc)`.

[Back to top](#table-of-contents)

---

## `LatestTaskCoordinator` and `AsyncLatestTaskCoordinator`

When fast UI searches (`"a"`, `"ab"`, `"abc"`) or rapid per-device refreshes overlap, stale responses can arrive out of order and overwrite fresher state. `LatestTaskCoordinator` tracks monotonic generations per key:

- `submit(key, factory, on_result, *, cancel_previous=True, on_error=None)` optionally cancels prior work and **only invokes `on_result` if that job is still the latest generation** (`snapshot().discarded_results` counts dropped stale results).

[Back to top](#table-of-contents)

---

## `ResourceScope` and `AsyncResourceScope`

Provides `ExitStack`-style cleanup with explicit retry guarantees:

- `add(resource, close="close", owned=True)` (`close="aclose"` in async variant; supports both sync and async close hooks). Setting `owned=False` borrows an object without closing it on exit.
- `defer(fn)` registers custom cleanup callbacks; `detach(resource)` transfers ownership out.
- **Reverse registration order (LIFO) + retry:** Every resource close is attempted even if an earlier one fails; failed items remain owned (`pending_cleanup() > 0`) for retry and raise an `ExceptionGroup`. `AsyncResourceScope.aclose()` finishes pending cleanup even if caller cancellation arrives mid-way.

[Back to top](#table-of-contents)

---

## `BoundedWorkPool` — Why Plain `ThreadPoolExecutor` Isn't Enough

Standard `ThreadPoolExecutor` accepts unbounded queued work. **`BoundedWorkPool(*, max_workers, max_pending, observer=None, thread_name_prefix="FlowCoordWorker")`**:

- Caps submissions strictly at `max_workers` + `max_pending`, rejecting overflow immediately and recording `rejected` in `WorkPoolStats` / `OperationStats`.
- Copies active `ContextVar` contexts automatically into worker threads on `submit()`.
- Supports `with BoundedWorkPool(...) as pool:`.

[Back to top](#table-of-contents)

---

## `AsyncLoopPortal` — Safe Thread-to-Loop Bridge

Calling `asyncio.run(...)` repeatedly from synchronous code destroys and recreates connection pools on every call. **`AsyncLoopPortal`** owns one dedicated long-lived event loop thread:

```python
import asyncio
from flowcoord import AsyncLoopPortal


async def async_add(a: int, b: int) -> int:
    await asyncio.sleep(0)
    return a + b


with AsyncLoopPortal(max_pending=32) as portal:
    value = portal.call(async_add, 19, 23, timeout=1.0)
    future = portal.submit(async_add, 1, 2)
    assert value == 42
    assert future.result() == 3
```

- `max_pending` (default `128`) bounds in-flight submissions.
- Timeouts/cancellations cancel the backing `asyncio.Task` cleanly and close unstarted input awaitables so coroutine leaks never happen.
- Submitting to a closed portal raises `AsyncLoopPortalClosedError`.

[Back to top](#table-of-contents)

---

**Previous:** [03 · Resilience: Retry, Circuit Breaker, and Cooldown](resilience-and-recovery.md) · **Next:** [05 · Scheduling, Coalescing, Observability, and C Core →](scheduling-coalescing-and-observability.md)
