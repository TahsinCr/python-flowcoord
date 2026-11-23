[![CI status][ci-shield]][ci]
[![PyPI version][pypi-shield]][pypi]
[![Python 3.10+][python-shield]][python]
[![MIT License][license-shield]][license]
**English** | [Türkçe][readme-tr]

# FlowCoord

[Repository][repository] · [Changelog][changelog] · [MIT License][license]

Execution primitives for Python applications that manage concurrent work, transient failures, and the lifetime of tasks and resources.

## Contents

- [Getting started](#getting-started)
- [Core concepts](#core-concepts)
- [Concurrency and task ownership](#concurrency)
- [Operation metrics](#operation-metrics)
- [Performance and the C extension](#performance)
- [Development and license](#development)

<a id="getting-started"></a>

## Getting started

### Install

Install from the project root with Python 3.10 or later.

~~~sh
python -m pip install .
~~~

### Retry a transient failure

A retry policy bounds the number of attempts. The predicate below permits a timeout to be retried and lets other exceptions propagate.

~~~python
from flowcoord import RetryPolicy

outcomes = iter([False, True])


def fetch() -> int:
    if not next(outcomes):
        raise TimeoutError("temporary")
    return 42


retry = RetryPolicy(max_attempts=2)
result = retry.run(fetch, should_retry=lambda error: isinstance(error, TimeoutError))
assert result == 42
~~~

<a id="core-concepts"></a>

## Core concepts

Choose a keyed limiter to cap concurrency or rate, `SingleFlight` to share one in-flight operation, and task or resource scopes to manage cleanup. The library also provides scheduling, retries, cooldowns, and a circuit breaker.

Synchronous classes coordinate work across threads and can be imported without loading `asyncio`. `Async*` classes use asyncio and are loaded on demand. Retry, circuit breaker, and cooldown expose `run()`, `call()`, and `wait()` in both modes. Await these calls on async instances.

| Thread | Asyncio |
| --- | --- |
| `KeyedConcurrencyLimiter`, `KeyedRateLimiter` | `AsyncKeyedConcurrencyLimiter`, `AsyncKeyedRateLimiter` |
| `SingleFlight` | `AsyncSingleFlight` |
| `RetryPolicy`, `CircuitBreaker`, `CooldownRegistry` | `AsyncRetryPolicy`, `AsyncCircuitBreaker`, `AsyncCooldownRegistry` |
| `Scheduler` | `AsyncScheduler` |
| `TaskScope`, `LatestTaskCoordinator` | `AsyncTaskScope`, `AsyncLatestTaskCoordinator` |
| `ResourceScope` | `AsyncResourceScope` |

`AsyncLoopPortal` lets a thread submit work to an asyncio loop. For thread-only background work, use `TaskScope` or `BoundedWorkPool`.

`AsyncTaskScope(max_pending=128)` and `AsyncLoopPortal(max_pending=128)` bound the number of accepted operations. A cancellation request keeps its slot until the operation has finished cleanup. `AsyncScheduler(max_jobs=4096)` limits one-shot and periodic jobs. Each parallel periodic job also has its own `max_parallel` limit. Submitting at capacity raises `RuntimeError` immediately.

Thread workers are suited to blocking I/O or native work that releases the GIL. On CPython with the GIL enabled, threads do not run CPU-bound Python code in parallel.

<a id="concurrency"></a>

## Concurrency and task ownership

### Acquire keyed permits in a thread pool

Queued calls can be cancelled. A running Python callback completes cooperatively. If `close(timeout=...)` times out, the scope continues to track the remaining work.

~~~python
from flowcoord import KeyedConcurrencyLimiter, TaskScope

limiter = KeyedConcurrencyLimiter[str](1)


def process(account: str) -> str:
    with limiter.acquire(account, timeout=1):
        return account


with TaskScope(max_workers=4, max_pending=16) as scope:
    first = scope.start(lambda: process("account-42"), shutdown="drain")
    second = scope.start(lambda: process("account-55"), shutdown="drain")
    assert (first.join(), second.join()) == ("account-42", "account-55")
~~~

### Limit work by key

A keyed limiter allows unrelated keys to make progress independently. A task scope keeps ownership of the workers and drains them before leaving the context.

~~~python
import asyncio

from flowcoord import AsyncKeyedConcurrencyLimiter, AsyncTaskScope


async def main() -> None:
    limiter = AsyncKeyedConcurrencyLimiter[str](1)

    async def process(account: str) -> str:
        async with limiter.acquire(account):
            await asyncio.sleep(0)
            return account

    try:
        async with AsyncTaskScope() as scope:
            task = scope.start(lambda: process("account-42"), shutdown="drain")
            assert await task.join() == "account-42"
    finally:
        await limiter.aclose()


asyncio.run(main())
~~~

<a id="operation-metrics"></a>

## Operation metrics

### Inspect a work pool snapshot

Attach `OperationStats` where you need operational counters. A bounded pool lets you set separate limits for workers and pending work.

~~~python
from flowcoord import BoundedWorkPool, OperationStats

metrics = OperationStats(max_operations=16)
with BoundedWorkPool(max_workers=2, max_pending=4, observer=metrics) as pool:
    result = pool.submit(abs, -42).result()
    assert result == 42

assert metrics.snapshot().operations
~~~

<a id="performance"></a>

## Performance and the C extension

The optional C extension reduces overhead when `OperationStats` frequently records and aggregates counters. Tasks, scheduling, and retries run in Python. Both execution modes share the same thread-safe metrics backend. The pure-Python metrics backend supports the same API. Run `python -m benchmarks.benchmark_observability` to measure the recording workload on your machine.

<a id="development"></a>

## Development and license

~~~sh
python -m pip install -e ".[dev]"
python run_tests.py
~~~

Run `python run_tests.py --native` to rebuild and require the C extension for the tests.

This project is licensed under the [MIT License][license].

## Related projects

[FlowEvent][repo-flowevent] · [FlowPipeline][repo-flowpipeline]

[ci-shield]: https://img.shields.io/github/actions/workflow/status/TahsinCr/python-flowcoord/ci.yml?branch=main&event=push&label=CI&style=for-the-badge&logo=github&logoColor=white
[pypi-shield]: https://img.shields.io/pypi/v/flowcoord?label=PyPI&style=for-the-badge&logo=pypi&logoColor=white
[python-shield]: https://img.shields.io/badge/Python-3.10%2B-3776AB.svg?style=for-the-badge&logo=python&logoColor=white
[license-shield]: https://img.shields.io/badge/license-MIT-blue.svg?style=for-the-badge
[ci]: https://github.com/TahsinCr/python-flowcoord/actions/workflows/ci.yml?query=branch%3Amain+event%3Apush
[pypi]: https://pypi.org/project/flowcoord/
[python]: https://www.python.org/downloads/
[readme-tr]: https://github.com/TahsinCr/python-flowcoord/blob/main/README-TR.md
[repository]: https://github.com/TahsinCr/python-flowcoord
[changelog]: https://github.com/TahsinCr/python-flowcoord/blob/main/CHANGELOG.md
[license]: https://github.com/TahsinCr/python-flowcoord/blob/main/LICENSE
[repo-flowevent]: https://github.com/TahsinCr/python-flowevent
[repo-flowpipeline]: https://github.com/TahsinCr/python-flowpipeline
