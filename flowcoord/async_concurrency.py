"""Loop-owned keyed concurrency and rate controls."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Hashable
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

from ._async_lifecycle import bind_running_loop, create_task_safely, finish_cleanup
from .base import (
    KeyedConcurrencyKeyStats,
    KeyedConcurrencyStats,
    KeyedRateStats,
    SingleFlightStats,
    _BaseKeyedRateLimiter,
    _TokenBucket,
)
from .observability import OperationStats, _require_operation_stats

K = TypeVar("K", bound=Hashable)
T = TypeVar("T")


@dataclass(slots=True)
class _ConcurrencyEntry:
    active: int = 0
    waiters: dict[asyncio.Future[None], None] = field(default_factory=dict)
    users: int = 0


class _Permit:
    def __init__(self, limiter: AsyncKeyedConcurrencyLimiter[Any], key: Hashable) -> None:
        self._limiter = limiter
        self._key = key
        self._entered = False

    async def __aenter__(self) -> None:
        if self._entered:
            raise RuntimeError("A keyed concurrency permit can only be entered once")
        started = asyncio.get_running_loop().time()
        try:
            await self._limiter._acquire(self._key)
        except asyncio.CancelledError:
            self._limiter._record("cancelled", asyncio.get_running_loop().time() - started)
            raise
        except Exception:
            self._limiter._record("error", asyncio.get_running_loop().time() - started)
            raise
        self._limiter._record("success", asyncio.get_running_loop().time() - started)
        self._entered = True
        return None

    async def __aexit__(self, *_args: object) -> None:
        if self._entered:
            self._entered = False
            self._limiter._release(self._key)


class AsyncKeyedConcurrencyLimiter(Generic[K]):
    """Limit simultaneous async work independently for each hashable key."""

    def __init__(self, max_concurrency: int, *, observer: OperationStats | None = None) -> None:
        if isinstance(max_concurrency, bool) or not isinstance(max_concurrency, int):
            raise TypeError("max_concurrency must be an integer")
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be positive")
        _require_operation_stats(observer)
        self._limit = max_concurrency
        self._observer = observer
        self._loop: asyncio.AbstractEventLoop | None = None
        self._entries: dict[K, _ConcurrencyEntry] = {}
        self._waiter_tasks: set[asyncio.Task[Any]] = set()
        self._total_waiters = 0
        self._closed = False

    def _bind_loop(self) -> asyncio.AbstractEventLoop:
        loop = bind_running_loop(self._loop, "AsyncKeyedConcurrencyLimiter")
        self._loop = loop
        return loop

    def _entry(self, key: K) -> _ConcurrencyEntry:
        entry = self._entries.get(key)
        if entry is None:
            entry = _ConcurrencyEntry()
            self._entries[key] = entry
        return entry

    def acquire(self, key: K) -> _Permit:
        """Return an async context manager that releases its permit on exit."""
        return _Permit(self, key)

    def _record(self, status: str, elapsed: float) -> None:
        if self._observer is not None:
            self._observer.record(
                "keyed_concurrency.acquire",
                status=status,
                elapsed=elapsed,
                queue_depth=self._total_waiters,
            )

    async def _acquire(self, key: K) -> None:
        loop = self._bind_loop()
        if self._closed:
            raise RuntimeError("AsyncKeyedConcurrencyLimiter is closed")
        entry = self._entry(key)
        entry.users += 1
        acquired = False
        waiter: asyncio.Future[None] | None = None
        task = asyncio.current_task()
        if task is not None:
            self._waiter_tasks.add(task)
        try:
            if self._closed:
                raise RuntimeError("AsyncKeyedConcurrencyLimiter is closed")
            if entry.active < self._limit and not entry.waiters:
                entry.active += 1
                acquired = True
            else:
                waiter = loop.create_future()
                entry.waiters[waiter] = None
                self._total_waiters += 1
                self._wake_waiters(entry)
                while True:
                    if self._closed:
                        raise RuntimeError("AsyncKeyedConcurrencyLimiter is closed")
                    if next(iter(entry.waiters)) is waiter and entry.active < self._limit:
                        entry.waiters.pop(waiter)
                        self._total_waiters -= 1
                        waiter = None
                        entry.active += 1
                        acquired = True
                        self._wake_waiters(entry)
                        break
                    await asyncio.shield(waiter)
        finally:
            if waiter is not None:
                is_first = next(iter(entry.waiters)) is waiter
                entry.waiters.pop(waiter)
                self._total_waiters -= 1
                if not waiter.done():
                    waiter.cancel()
                if is_first:
                    self._wake_waiters(entry)
            if task is not None:
                self._waiter_tasks.discard(task)
            if not acquired:
                entry.users -= 1
                self._prune(key, entry)

    def _wake_waiters(self, entry: _ConcurrencyEntry) -> None:
        if not entry.waiters:
            return
        if self._closed:
            for waiter in tuple(entry.waiters):
                if not waiter.done():
                    waiter.set_result(None)
        elif entry.active < self._limit:
            waiter = next(iter(entry.waiters))
            if not waiter.done():
                waiter.set_result(None)

    def _release(self, key: K) -> None:
        self._bind_loop()
        entry = self._entries.get(key)
        if entry is None or entry.active == 0:
            raise RuntimeError("No active permit exists for this key")
        entry.active -= 1
        entry.users -= 1
        self._wake_waiters(entry)
        self._prune(key, entry)

    def _prune(self, key: K, entry: _ConcurrencyEntry) -> None:
        if entry.users == 0 and entry.active == 0 and not entry.waiters:
            self._entries.pop(key, None)

    def acquire_nowait(self, key: K) -> bool:
        """Reserve a slot immediately; pair a successful call with release(key)."""
        self._bind_loop()
        if self._closed:
            return False
        entry = self._entry(key)
        if entry.active >= self._limit or entry.waiters:
            self._prune(key, entry)
            return False
        entry.active += 1
        entry.users += 1
        return True

    async def release(self, key: K) -> None:
        """Release a slot previously acquired with acquire_nowait()."""
        self._release(key)

    def stats(self) -> KeyedConcurrencyStats:
        self._bind_loop()
        return KeyedConcurrencyStats(
            len(self._entries),
            sum(entry.active for entry in self._entries.values()),
            sum(len(entry.waiters) for entry in self._entries.values()),
            self._closed,
        )

    def key_stats(self, key: K) -> KeyedConcurrencyKeyStats:
        """Return active and waiting counts for one key, or zeroes if unused."""
        self._bind_loop()
        entry = self._entries.get(key)
        return KeyedConcurrencyKeyStats(
            0 if entry is None else entry.active,
            0 if entry is None else len(entry.waiters),
        )

    async def aclose(self) -> None:
        self._bind_loop()
        self._closed = True
        for entry in tuple(self._entries.values()):
            self._wake_waiters(entry)
        for key, entry in tuple(self._entries.items()):
            self._prune(key, entry)
        waiters = tuple(task for task in self._waiter_tasks if task is not asyncio.current_task())
        if waiters:
            await finish_cleanup(asyncio.gather(*waiters, return_exceptions=True))


@dataclass(slots=True)
class _Flight(Generic[T]):
    task: asyncio.Task[T]
    waiters: int = 0


class AsyncSingleFlight(Generic[K]):
    """Share one in-flight async factory result among callers with the same key."""

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._flights: dict[K, _Flight[Any]] = {}
        self._executing: dict[asyncio.Task[Any], K] = {}
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None

    def _bind_loop(self) -> asyncio.AbstractEventLoop:
        loop = bind_running_loop(self._loop, "AsyncSingleFlight")
        self._loop = loop
        return loop

    async def run(self, key: K, factory: Callable[[], Awaitable[T]]) -> T:
        """Join the active operation; cancelling one waiter leaves shared work running."""
        loop = self._bind_loop()
        if not callable(factory):
            raise TypeError("factory must be callable")
        if self._closed:
            raise RuntimeError("AsyncSingleFlight is closed")
        current = asyncio.current_task()
        if current is not None and current in self._executing and self._executing[current] == key:
            raise RuntimeError("A single-flight operation cannot wait for itself")
        flight = self._flights.get(key)
        if flight is None or flight.task.done():

            async def execute() -> T:
                owner = asyncio.current_task()
                assert owner is not None
                self._executing[owner] = key
                try:
                    return await factory()
                finally:
                    del self._executing[owner]

            task = create_task_safely(execute(), name="FlowCoordSingleFlight", loop=loop)
            flight = _Flight(task)
            self._flights[key] = flight
            task.add_done_callback(
                lambda completed, flight_key=key, record=flight: self._finished(
                    flight_key, record, completed
                )
            )
        flight.waiters += 1
        try:
            return await asyncio.shield(flight.task)
        finally:
            flight.waiters -= 1

    async def aclose(self) -> None:
        """Cancel and drain shared operations owned by this instance.

        Factories must cooperate with cancellation for shutdown to finish.
        Cancelling one waiter still leaves shared work running; call ``aclose``
        when the instance is no longer needed.
        """
        loop = self._bind_loop()
        current = asyncio.current_task()
        if current in self._executing:
            raise RuntimeError("An AsyncSingleFlight operation cannot close its own instance")
        if self._close_task is None:
            self._closed = True
            tasks = tuple(flight.task for flight in self._flights.values())

            async def close_tasks() -> None:
                for task in tasks:
                    task.cancel()
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

            self._close_task = create_task_safely(
                close_tasks(), name="FlowCoordSingleFlightClose", loop=loop
            )
        await asyncio.shield(self._close_task)

    def _finished(self, key: K, flight: _Flight[Any], task: asyncio.Task[Any]) -> None:
        if self._flights.get(key) is flight:
            self._flights.pop(key, None)
        if not task.cancelled():
            error = task.exception()
            if error is not None and flight.waiters == 0:
                loop = self._loop
                if loop is not None:
                    loop.call_exception_handler(
                        {
                            "message": "AsyncSingleFlight operation failed after every waiter left",
                            "exception": error,
                            "task": task,
                        }
                    )

    def stats(self) -> SingleFlightStats:
        self._bind_loop()
        return SingleFlightStats(
            len(self._flights), sum(flight.waiters for flight in self._flights.values())
        )


@dataclass(slots=True)
class _RateEntry(_TokenBucket):
    signals: set[asyncio.Event] | None = None


class AsyncKeyedRateLimiter(_BaseKeyedRateLimiter[K, _RateEntry]):
    """Async token bucket limiter with independent buckets and monotonic refill."""

    def __init__(
        self,
        rate: float,
        period: float = 1.0,
        burst: float | None = None,
        *,
        max_keys: int = 4096,
        observer: OperationStats | None = None,
    ) -> None:
        super().__init__(rate, period, burst, max_keys=max_keys, observer=observer)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._waiter_tasks: set[asyncio.Task[Any]] = set()

    def _new_bucket(self, now: float) -> _RateEntry:
        return _RateEntry(self._burst, now)

    def _bind_loop(self) -> asyncio.AbstractEventLoop:
        loop = bind_running_loop(self._loop, "AsyncKeyedRateLimiter")
        self._loop = loop
        return loop

    async def acquire(self, key: K) -> None:
        loop = self._bind_loop()
        started = loop.time()
        if self._closed:
            if self._observer is not None:
                self._observer.record("keyed_rate.acquire", status="error")
            raise RuntimeError("AsyncKeyedRateLimiter is closed")
        entry = self._entry(key, started)
        if not entry.waiters and entry.tokens >= 1.0:
            entry.tokens -= 1.0
            if self._observer is not None:
                self._observer.record(
                    "keyed_rate.acquire", elapsed=loop.time() - started, queue_depth=0
                )
            return
        entry.waiters += 1
        task = asyncio.current_task()
        if task is not None:
            self._waiter_tasks.add(task)
        if entry.signals is None:
            entry.signals = set()
        try:
            while True:
                if self._closed:
                    raise RuntimeError("AsyncKeyedRateLimiter is closed")
                now = loop.time()
                self._refill(entry, now)
                if entry.tokens >= 1.0:
                    entry.tokens -= 1.0
                    if self._observer is not None:
                        self._observer.record(
                            "keyed_rate.acquire",
                            elapsed=loop.time() - started,
                            queue_depth=entry.waiters - 1,
                        )
                    return
                delay = (1.0 - entry.tokens) * self._period / self._rate
                changed = asyncio.Event()
                entry.signals.add(changed)
                try:
                    await asyncio.wait_for(changed.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
                finally:
                    entry.signals.discard(changed)
        finally:
            entry.waiters -= 1
            if task is not None:
                self._waiter_tasks.discard(task)
            self._prune(key, entry)

    async def aclose(self) -> None:
        self._bind_loop()
        self._closed = True
        for entry in self._entries.values():
            if entry.signals:
                for signal in tuple(entry.signals):
                    signal.set()
        waiters = tuple(task for task in self._waiter_tasks if task is not asyncio.current_task())
        if waiters:
            await finish_cleanup(asyncio.gather(*waiters, return_exceptions=True))
        for key, entry in tuple(self._entries.items()):
            self._prune(key, entry)

    def try_acquire(self, key: K) -> bool:
        acquired = self._try_acquire_at(key, self._bind_loop().time())
        self._record_try_acquire(acquired)
        return acquired

    def available(self, key: K) -> float:
        return self._available_at(key, self._bind_loop().time())

    def prune(self) -> int:
        return self._prune_at(self._bind_loop().time())

    def stats(self) -> KeyedRateStats:
        self._bind_loop()
        return self._stats()
