"""Thread-safe keyed permits, token buckets, and shared in-flight work."""

from __future__ import annotations

import threading
import time
from collections import deque
from collections.abc import Callable, Hashable, Iterator
from concurrent.futures import Future
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

from ._callbacks import synchronous_result
from ._validation import validate_nonnegative_timeout
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
class _Entry:
    active: int = 0
    waiters: deque[threading.Event] = field(default_factory=deque)


class KeyedConcurrencyLimiter(Generic[K]):
    """FIFO permits for each key. Waiting threads wake one at a time."""

    def __init__(self, max_concurrency: int, *, observer: OperationStats | None = None) -> None:
        if isinstance(max_concurrency, bool) or not isinstance(max_concurrency, int):
            raise TypeError("max_concurrency must be an integer")
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be positive")
        _require_operation_stats(observer)
        self._limit = max_concurrency
        self._observer = observer
        self._lock = threading.Lock()
        self._entries: dict[K, _Entry] = {}
        self._waiters = 0
        self._closed = False

    @contextmanager
    def acquire(self, key: K, timeout: float | None = None) -> Iterator[None]:
        """Acquire one permit, releasing it on exit even if the body fails."""
        timeout = validate_nonnegative_timeout(timeout, name="permit timeout")
        started = time.monotonic() if self._observer is not None else 0.0
        self._acquire(key, timeout)
        try:
            if self._observer is not None:
                with self._lock:
                    waiting = self._waiters
                self._observer.record(
                    "keyed_concurrency.acquire",
                    elapsed=time.monotonic() - started,
                    queue_depth=waiting,
                )
            yield
        finally:
            self.release(key)

    def _wake_head(self, entry: _Entry) -> None:
        if entry.active < self._limit and entry.waiters:
            entry.waiters[0].set()

    def _prune(self, key: K, entry: _Entry) -> None:
        if not entry.active and not entry.waiters:
            del self._entries[key]

    def _acquire(self, key: K, timeout: float | None) -> None:
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._lock:
            if self._closed:
                raise RuntimeError("KeyedConcurrencyLimiter is closed")
            entry = self._entries.get(key)
            if entry is None:
                self._entries[key] = _Entry(active=1)
                return
            if entry.active < self._limit and not entry.waiters:
                entry.active += 1
                return
            if timeout == 0:
                raise TimeoutError("Timed out waiting for a permit")
            signal = threading.Event()
            entry.waiters.append(signal)
            self._waiters += 1
        try:
            while True:
                remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
                if not signal.wait(
                    None if remaining is None else min(remaining, threading.TIMEOUT_MAX)
                ):
                    if deadline is not None and time.monotonic() >= deadline:
                        raise TimeoutError("Timed out waiting for a permit")
                    continue
                with self._lock:
                    if self._closed:
                        raise RuntimeError("KeyedConcurrencyLimiter is closed")
                    if entry.active < self._limit and entry.waiters[0] is signal:
                        entry.active += 1
                        return
                    signal.clear()
        finally:
            with self._lock:
                entry.waiters.remove(signal)
                self._waiters -= 1
                self._wake_head(entry)
                self._prune(key, entry)

    def acquire_nowait(self, key: K) -> bool:
        with self._lock:
            if self._closed:
                return False
            entry = self._entries.get(key)
            if entry is None:
                self._entries[key] = _Entry(active=1)
                return True
            if entry.waiters or entry.active >= self._limit:
                return False
            entry.active += 1
            return True

    def release(self, key: K) -> None:
        with self._lock:
            entry = self._entries.get(key)
            if entry is None or not entry.active:
                raise RuntimeError("No active permit for this key")
            entry.active -= 1
            self._wake_head(entry)
            self._prune(key, entry)

    def stats(self) -> KeyedConcurrencyStats:
        with self._lock:
            return KeyedConcurrencyStats(
                len(self._entries),
                sum(e.active for e in self._entries.values()),
                self._waiters,
                self._closed,
            )

    def key_stats(self, key: K) -> KeyedConcurrencyKeyStats:
        with self._lock:
            entry = self._entries.get(key)
            return (
                KeyedConcurrencyKeyStats(entry.active, len(entry.waiters))
                if entry
                else KeyedConcurrencyKeyStats(0, 0)
            )

    def close(self) -> None:
        """Reject new permits and wake waiters; active permits still release normally."""
        with self._lock:
            self._closed = True
            for entry in self._entries.values():
                for signal in entry.waiters:
                    signal.set()

    def __enter__(self) -> KeyedConcurrencyLimiter[K]:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


@dataclass(slots=True)
class _RateEntry(_TokenBucket):
    changed: threading.Condition | None = None


class KeyedRateLimiter(_BaseKeyedRateLimiter[K, _RateEntry]):
    """A bounded token bucket for each key, with monotonic refill."""

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
        self._lock = threading.Lock()

    def _new_bucket(self, now: float) -> _RateEntry:
        return _RateEntry(self._burst, now)

    def acquire(self, key: K, timeout: float | None = None) -> None:
        """Wait for one token. A caller timeout does not consume a token."""
        timeout = validate_nonnegative_timeout(timeout, name="rate limit timeout")
        started = time.monotonic()
        deadline = None if timeout is None else started + timeout
        with self._lock:
            if self._closed:
                raise RuntimeError("KeyedRateLimiter is closed")
            entry = self._entry(key, started)
            if not entry.waiters and entry.tokens >= 1.0:
                entry.tokens -= 1.0
                waiting = 0
            else:
                if entry.changed is None:
                    entry.changed = threading.Condition(self._lock)
                entry.waiters += 1
                try:
                    while True:
                        if self._closed:
                            raise RuntimeError("KeyedRateLimiter is closed")
                        now = time.monotonic()
                        if deadline is not None and now >= deadline:
                            raise TimeoutError("Timed out waiting for a rate-limit token")
                        self._refill(entry, now)
                        if entry.tokens >= 1.0:
                            entry.tokens -= 1.0
                            waiting = entry.waiters - 1
                            break
                        delay = (1.0 - entry.tokens) * self._period / self._rate
                        if deadline is not None:
                            delay = min(delay, deadline - now)
                        entry.changed.wait(min(delay, threading.TIMEOUT_MAX))
                finally:
                    entry.waiters -= 1
                    self._prune(key, entry)
        if self._observer is not None:
            self._observer.record(
                "keyed_rate.acquire", elapsed=time.monotonic() - started, queue_depth=waiting
            )

    def try_acquire(self, key: K) -> bool:
        with self._lock:
            acquired = self._try_acquire_at(key, time.monotonic())
        self._record_try_acquire(acquired)
        return acquired

    def available(self, key: K) -> float:
        with self._lock:
            return self._available_at(key, time.monotonic())

    def prune(self) -> int:
        with self._lock:
            return self._prune_at(time.monotonic())

    def stats(self) -> KeyedRateStats:
        with self._lock:
            return self._stats()

    def close(self) -> None:
        with self._lock:
            self._closed = True
            for entry in self._entries.values():
                if entry.changed is not None and entry.waiters:
                    entry.changed.notify_all()


@dataclass(slots=True)
class _Flight:
    owner: int
    result: Future[Any] | None = None


class SingleFlight(Generic[K]):
    """Share a running call and its result with threads using the same key."""

    def __init__(self) -> None:
        self._changed = threading.Condition()
        self._flights: dict[K, _Flight] = {}
        self._shared = 0
        self._closed = False

    def run(self, key: K, factory: Callable[[], T]) -> T:
        owner = threading.get_ident()
        with self._changed:
            if self._closed:
                raise RuntimeError("SingleFlight is closed")
            flight = self._flights.get(key)
            if flight is None:
                self._flights[key] = _Flight(owner)
                shared = None
            else:
                if flight.owner == owner:
                    raise RuntimeError("A single-flight operation cannot wait for itself")
                if flight.result is None:
                    flight.result = Future()
                shared = flight.result
                self._shared += 1
        if shared is not None:
            try:
                return shared.result()
            finally:
                with self._changed:
                    self._shared -= 1
        try:
            result = synchronous_result(factory())
        except BaseException as error:
            self._finish(key, None, error)
            raise
        self._finish(key, result, None)
        return result

    def _finish(self, key: K, result: object, error: BaseException | None) -> None:
        with self._changed:
            shared = self._flights.pop(key).result
            if self._closed:
                self._changed.notify_all()
        if shared is not None:
            if error is None:
                shared.set_result(result)
            else:
                shared.set_exception(error)

    def close(self, timeout: float | None = None) -> None:
        timeout = validate_nonnegative_timeout(timeout, name="single-flight close timeout")
        end = None if timeout is None else time.monotonic() + timeout
        with self._changed:
            if any(flight.owner == threading.get_ident() for flight in self._flights.values()):
                raise RuntimeError("A single-flight operation cannot close its own owner")
            self._closed = True
            while self._flights:
                remaining = None if end is None else end - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError("Single-flight operations are still running")
                self._changed.wait(
                    None if remaining is None else min(remaining, threading.TIMEOUT_MAX)
                )

    def stats(self) -> SingleFlightStats:
        with self._changed:
            return SingleFlightStats(len(self._flights), self._shared)
