[← Documentation Map](README.md) · [Previous: 04 · Tasks, Scopes, and Portal](tasks-scopes-and-portal.md) · [Next: 06 · API Reference →](api-reference.md)

# 05 · Scheduling, Coalescing, Observability, and C Core

This guide covers `Scheduler` / `AsyncScheduler` overlap policies, deduplicating rapid updates with `KeyedCoalescer`, bounded-cardinality metrics via `OperationStats`, and the optional `flowcoord._core` C extension.

---

## Table of Contents

- [`Scheduler` and `AsyncScheduler` (One-Shot & Periodic)](#scheduler-and-asyncscheduler-one-shot--periodic)
  - [Overlap Policies (`overlap`: `skip`, `queue`, `parallel`)](#overlap-policies-overlap-skip-queue-parallel)
- [`KeyedCoalescer`: Collapsing Rapid Keyed Updates](#keyedcoalescer-collapsing-rapid-keyed-updates)
- [`OperationStats`: Metrics Without Cardinality Explosion](#operationstats-metrics-without-cardinality-explosion)
- [`flowcoord._core` C Core and Free-Threaded Python](#flowcoord_core-c-core-and-free-threaded-python)

---

## `Scheduler` and `AsyncScheduler` (One-Shot & Periodic)

`Scheduler` uses a single timer thread dispatching to bounded workers (`max_workers=4`, `max_jobs=4096`). `AsyncScheduler` runs on the caller's event loop (`max_jobs=4096`) and accepts both sync and async callbacks without creating threads.

- `schedule_once(cb, *args, delay=0.0, name=None, **kwargs)` → `ScheduledHandle` / `AsyncScheduledHandle`
- `periodic(cb, *args, interval=..., overlap="skip", initial_delay=None, max_parallel=16, name=None, **kwargs)`

### Overlap Policies (`overlap`: `skip`, `queue`, `parallel`)

When a job scheduled every `1.0` s runs for `1.5` s, `overlap` dictates behavior:

| Policy | Behavior | When to Use |
| --- | --- | --- |
| `"skip"` *(default)* | Skips firing again while a previous run is active. | Health checks, periodic syncs where backlog hurts. |
| `"queue"` | Queues the next run sequentially once the current finishes. | Periodic ticks that must not be skipped. |
| `"parallel"` | Runs overlapping triggers concurrently up to `max_parallel` (default `16`). | Independent periodic batches. |

[Back to top](#table-of-contents)

---

## `KeyedCoalescer`: Collapsing Rapid Keyed Updates

When sensors, UI clients, or event producers emit hundreds of updates per second for the same entity, `KeyedCoalescer` buffers per key over `interval` seconds:

- New arrivals for an already-pending key collapse immediately via `merge(existing, incoming)` (`merged` increments without growing queue depth).
- Distinct pending keys are capped by `max_pending_keys` (default `128`).
- One owned worker thread delivers merged results cleanly (`flush()` and `close(flush=True)` supported).

```python
from flowcoord import KeyedCoalescer

delivered: list[tuple[str, int]] = []
with KeyedCoalescer[tuple[str, int], str](
    interval=0.02,
    key=lambda item: item[0],
    merge=lambda old, new: (old[0], old[1] + new[1]),
    callback=delivered.append,
) as coalescer:
    coalescer.push(("cpu", 10))
    coalescer.push(("cpu", 15))
    coalescer.flush()

assert delivered == [("cpu", 25)]
```

[Back to top](#table-of-contents)

---

## `OperationStats`: Metrics Without Cardinality Explosion

Unbounded metric label dicts leak memory quickly if a caller accidentally formats user IDs or UUIDs into operation names. **`OperationStats(max_operations=128)`** bounds label cardinality:

- Once `max_operations` distinct names exist, further unseen names automatically fold into `"other"`.
- Tracks `OperationMetrics` (`calls`, `successful`, `failed`, `cancelled`, `total_seconds`, `max_seconds`, `queue_depth`, `max_queue_depth`, `dropped`, `retries`, `rejected`, `timeouts`, `shutdown_seconds`).
- `snapshot()` returns an immutable `OperationStatsSnapshot` (`MappingProxyType`), unaffected by future `record()` or `reset()` calls.

[Back to top](#table-of-contents)

---

## `flowcoord._core` C Core and Free-Threaded Python

While limiters, retries, scopes, and schedulers remain readable pure Python, high-frequency **`OperationStats`** recording/aggregation (`OperationStatsCore`) accelerates cleanly via **`flowcoord._core`** (`flowcoord/_core.c`):

- Compiles with `optional=True` in `setup.py`; falls back seamlessly to pure Python when unavailable.
- Declares `Py_MOD_GIL_NOT_USED` for free-threaded CPython (`3.13t–3.15t`) and fails closed if importing `_core` ever re-enables the GIL.

| Environment Variable | Stage | Effect |
| --- | --- | --- |
| `FLOWCOORD_NO_EXTENSIONS=1` | Runtime | Disables `_core` and forces pure-Python `OperationStats`. |
| `FLOWCOORD_PURE_PYTHON=1` | Build / install | Skips building `_core`. |
| `FLOWCOORD_REQUIRE_NATIVE=1` | Build / install | Makes C compilation mandatory (`optional=False`). |

[Back to top](#table-of-contents)

---

**Previous:** [04 · Tasks, Resource Scopes, Work Pool, and Portal](tasks-scopes-and-portal.md) · **Next:** [06 · API Reference →](api-reference.md)
