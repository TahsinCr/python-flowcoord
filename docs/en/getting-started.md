[← Documentation Map](README.md) · [Next: 02 · Keyed Limiting and SingleFlight →](concurrency-and-deduplication.md)

# 01 · Getting Started and Architecture

This guide covers installing `flowcoord`, why its synchronous path avoids importing `asyncio`, CPython GIL implications, cooperative cancellation, and shared `Deadline` budgets.

---

## Table of Contents

- [Installation](#installation)
- [Architectural Decisions ("Why Is It Designed This Way?")](#architectural-decisions-why-is-it-designed-this-way)
  - [Independent Sync Import Path (Zero `asyncio` Load)](#independent-sync-import-path-zero-asyncio-load)
  - [The CPython GIL Reality: When Threads Help](#the-cpython-gil-reality-when-threads-help)
  - [Cooperative Cancellation (Never Abandon Threads or Cleanup)](#cooperative-cancellation-never-abandon-threads-or-cleanup)
  - [Bounded Capacities and Fail-Fast Rejection](#bounded-capacities-and-fail-fast-rejection)
- [Shared Time Budgets: `Deadline`](#shared-time-budgets-deadline)
- [First Working Example](#first-working-example)

---

## Installation

`flowcoord` requires **Python 3.10 or newer**. Python 3.10 installs the `exceptiongroup` library. There are no runtime dependencies on Python 3.11 and later. The optional C extension comes with supported platform wheels. If there is no suitable wheel, installing from source can fall back to pure Python.

```bash
# Install from PyPI
python -m pip install flowcoord

# Editable install from the repository root
python -m pip install -e ".[dev]"
```

[Back to top](#table-of-contents)

---

## Architectural Decisions ("Why Is It Designed This Way?")

### Independent Sync Import Path (Zero `asyncio` Load)

Importing synchronous primitives (`from flowcoord import RetryPolicy, TaskScope, OperationStats`) **does not load Python's `asyncio` module**. `Async*` classes are resolved lazily via `__getattr__` only when requested, keeping startup and memory clean for CLI tools, Flask/Django apps, and pure sync workers.

### The CPython GIL Reality: When Threads Help

Threaded primitives (`TaskScope`, `Scheduler`, `BoundedWorkPool`) excel for:

- **Blocking I/O** (network sockets, database drivers, filesystem calls)
- **Native libraries that release the GIL** (C extensions, NumPy, cryptography)

On GIL-enabled CPython, pure-Python CPU loops do not run in parallel across threads—use separate processes or free-threaded CPython (`3.13t+`) for CPU-bound Python computation.

### Cooperative Cancellation (Never Abandon Threads or Cleanup)

Neither OS threads nor uncooperative coroutines (`except CancelledError:`) can be safely terminated mid-instruction. Instead of faking timeouts by abandoning orphaned workers in the background:

- Queued work is cancelled immediately before starting.
- In-flight sync work finishes cooperatively on worker threads.
- In `AsyncTaskScope`, a cancelled task keeps holding its `max_pending` slot **until its `finally` cleanup completes**, preventing hidden resource leaks.

### Bounded Capacities and Fail-Fast Rejection

`AsyncTaskScope(max_pending=128)`, `AsyncLoopPortal(max_pending=128)`, `BoundedWorkPool(max_workers=..., max_pending=...)`, and `AsyncScheduler(max_jobs=4096)` reject excess submissions immediately with `RuntimeError` rather than buffering indefinitely until OOM.

[Back to top](#table-of-contents)

---

## Shared Time Budgets: `Deadline`

Passing an identical `timeout=2.0` into three nested calls (retry + lock acquire + HTTP request) lets total latency exceed 6+ seconds. **`Deadline`** tracks one absolute monotonic expiration across all nested operations:

- `Deadline.after(timeout)`: Creates a budget expiring in `timeout` seconds (`None` = unlimited).
- `deadline.remaining` / `deadline.expired`: Inspects seconds left (`0.0` when elapsed).
- `deadline.child(sub_timeout)`: Caps a nested step without outliving the outer deadline (`min(now + sub_timeout, parent_expires_at)`).

```python
from flowcoord import Deadline

total_budget = Deadline.after(5.0)
db_step_budget = total_budget.child(2.0)  # At most 2.0s and never past 5.0s
assert db_step_budget.remaining is not None and db_step_budget.remaining <= 2.0
```

[Back to top](#table-of-contents)

---

## First Working Example

```python
from flowcoord import CircuitBreaker, Deadline, OperationStats, RetryPolicy

stats = OperationStats(max_operations=16)
breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=10.0)
retry = RetryPolicy(max_attempts=3, backoff="exponential", initial_delay=0.01)

attempts = 0


def flaky_call() -> str:
    global attempts
    attempts += 1
    if attempts < 2:
        raise TimeoutError("transient network glitch")
    return "ok"


budget = Deadline.after(1.0)
result = retry.run(
    lambda: breaker.call(flaky_call),
    should_retry=lambda exc: isinstance(exc, TimeoutError),
    deadline=budget,
)
stats.record("api.fetch", status="success")

assert result == "ok"
assert stats.snapshot().operations["api.fetch"].calls == 1
```

[Back to top](#table-of-contents)

---

**Previous:** [Documentation Map](README.md) · **Next:** [02 · Keyed Limiting and SingleFlight →](concurrency-and-deduplication.md)
