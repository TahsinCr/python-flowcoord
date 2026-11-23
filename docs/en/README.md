# FlowCoord English Documentation

`flowcoord` provides zero-dependency **in-process** execution primitives for managing concurrent work, transient failures, keyed rate/concurrency limits, scheduled jobs, and task/resource lifetimes in Python. Every core primitive ships with matching **thread-based (sync)** and **`asyncio`-based (async)** APIs.

> **Scope note:** `flowcoord` coordinates execution within a single Python process. It does not replace distributed Redis locks, external API gateway rate limiters, or durable cross-process job queues.

---

## Table of Contents

- [Where Should You Start?](#where-should-you-start)
- [Documentation Map](#documentation-map)
- [Sync / Async Symmetry](#sync--async-symmetry)
- [Choosing the Right Primitive](#choosing-the-right-primitive)

---

## Where Should You Start?

- Start with [01 · Getting Started and Architecture](getting-started.md) to learn why synchronous imports never load `asyncio`, how the CPython GIL shapes thread pools, and how `Deadline` coordinates shared time budgets.
- To isolate noisy tenants (`KeyedConcurrencyLimiter`, `KeyedRateLimiter`) and deduplicate thundering herds (`SingleFlight`), read [02 · Keyed Limiting and SingleFlight](concurrency-and-deduplication.md).
- For exponential retries (`RetryPolicy`), circuit breaking (`CircuitBreaker`), cooldown tracking (`CooldownRegistry`), and proper layering order, see [03 · Resilience: Retry, Circuit Breaker, and Cooldown](resilience-and-recovery.md).
- For bounded task ownership (`TaskScope`, `LatestTaskCoordinator`), retryable LIFO cleanup (`ResourceScope`), bounded worker pools (`BoundedWorkPool`), and thread-to-loop bridging (`AsyncLoopPortal`), jump to [04 · Tasks, Resource Scopes, Work Pool, and Portal](tasks-scopes-and-portal.md).
- For scheduled jobs (`Scheduler` / `AsyncScheduler`), update merging (`KeyedCoalescer`), bounded-cardinality `OperationStats`, and `flowcoord._core`, read [05 · Scheduling, Coalescing, Observability, and C Core](scheduling-coalescing-and-observability.md).
- For every constructor, method signature, stats dataclass, and exception across all 46 public exports, see [06 · API Reference](api-reference.md).

[Back to top](#table-of-contents)

---

## Documentation Map

| Guide | Contents |
| --- | --- |
| **[01 · Getting Started and Architecture](getting-started.md)** | Installation, independent sync import path, GIL considerations, cooperative cancellation, and `Deadline`. |
| **[02 · Keyed Limiting and SingleFlight](concurrency-and-deduplication.md)** | `KeyedConcurrencyLimiter`, `KeyedRateLimiter` (token bucket + `max_keys`), and `SingleFlight` (stampede defense). |
| **[03 · Resilience: Retry, Circuit Breaker, and Cooldown](resilience-and-recovery.md)** | `RetryPolicy` (jitter, `Deadline`), `CircuitBreaker` (`closed`/`open`/`half_open`), `CooldownRegistry`, and composition order. |
| **[04 · Tasks, Resource Scopes, Work Pool, and Portal](tasks-scopes-and-portal.md)** | `TaskScope`, `LatestTaskCoordinator`, `ResourceScope` (LIFO + retry), `BoundedWorkPool`, and `AsyncLoopPortal`. |
| **[05 · Scheduling, Coalescing, Observability, and C Core](scheduling-coalescing-and-observability.md)** | `Scheduler` / `AsyncScheduler` (`overlap`), `KeyedCoalescer`, `OperationStats` (`max_operations`), `flowcoord._core`, and free-threaded Python. |
| **[06 · API Reference](api-reference.md)** | Complete constructors, methods, stats snapshots, and exceptions across all 46 exports. |

[Back to top](#table-of-contents)

---

## Sync / Async Symmetry

Importing synchronous classes in `flowcoord` **never imports `asyncio`**. `Async*` classes load lazily via `__getattr__` (`_ASYNC_EXPORTS`). Both modes keep method verbs consistent (`run()`, `call()`, `wait()`):

| Thread (Sync) | Asyncio (Async) | Purpose |
| --- | --- | --- |
| `KeyedConcurrencyLimiter` | `AsyncKeyedConcurrencyLimiter` | Independent per-key concurrency cap |
| `KeyedRateLimiter` | `AsyncKeyedRateLimiter` | Per-key monotonic token bucket rate limit |
| `SingleFlight` | `AsyncSingleFlight` | Share one in-flight work item per key |
| `RetryPolicy` | `AsyncRetryPolicy` | Fixed/exponential backoff + jitter retries |
| `CircuitBreaker` | `AsyncCircuitBreaker` | Fail-fast state machine for failing dependencies |
| `CooldownRegistry` | `AsyncCooldownRegistry` | Per-key cooldown registry and waits |
| `Scheduler` | `AsyncScheduler` | One-shot and periodic job dispatch |
| `TaskScope` | `AsyncTaskScope` | Bounded background task ownership |
| `LatestTaskCoordinator` | `AsyncLatestTaskCoordinator` | Apply only the latest generation per key |
| `ResourceScope` | `AsyncResourceScope` | Reverse-order retryable cleanup |

[Back to top](#table-of-contents)

---

## Choosing the Right Primitive

| Problem | Primitive | Why? |
| --- | --- | --- |
| One tenant exhausts shared threads or sockets | `KeyedConcurrencyLimiter` / `KeyedRateLimiter` | Enforces fairness per hashable key without stalling unrelated keys. |
| 100 concurrent misses hit the same expensive lookup | `SingleFlight` / `AsyncSingleFlight` | Executes the factory once and shares the outcome with all callers. |
| A failing dependency stalls threads during retries | `CircuitBreaker` + `RetryPolicy` + `Deadline` | Shares a total deadline and trips the breaker to fail fast when down. |
| Standard `ThreadPoolExecutor` queues unbounded work until OOM | `BoundedWorkPool` / `TaskScope` | Bounds both active workers and queued pending slots explicitly. |
| Sync threads need to invoke an async SDK or loop client | `AsyncLoopPortal` | Owns one long-lived background loop thread with bounded pending work. |
| Rapid updates per key flood downstream writers | `KeyedCoalescer` | Merges repeated updates per key inside each time interval. |

[Back to top](#table-of-contents)

---

**Next:** [01 · Getting Started and Architecture →](getting-started.md)
