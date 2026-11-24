"""Backend-neutral contracts, snapshots, and coordination state."""

from __future__ import annotations

import math
import random
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Hashable, Mapping
from dataclasses import dataclass
from typing import Generic, Literal, TypeVar

from ._callbacks import _NON_AWAITABLE_TYPES, close_awaitable, is_awaitable, synchronous_result
from ._compatibility import ExceptionGroup
from ._validation import (
    validate_finite_duration,
    validate_nonnegative_timeout,
    validate_positive_capacity,
)
from .errors import CircuitOpenError
from .observability import OperationStats, _require_operation_stats
from .timing import Deadline

K = TypeVar("K", bound=Hashable)
T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class KeyedConcurrencyKeyStats:
    active: int
    waiters: int


@dataclass(frozen=True, slots=True)
class KeyedConcurrencyStats:
    keys: int
    active: int
    waiters: int
    closed: bool


@dataclass(frozen=True, slots=True)
class SingleFlightStats:
    active: int
    shared_waiters: int


@dataclass(frozen=True, slots=True)
class KeyedRateStats:
    keys: int
    waiters: int
    closed: bool


@dataclass(frozen=True, slots=True)
class RetryObservation:
    attempt: int
    elapsed: float
    status: Literal["success", "retry", "error"]
    error: Exception | None = None


@dataclass(frozen=True, slots=True)
class CooldownStats:
    keys: int
    active: int
    waiters: int


@dataclass(frozen=True, slots=True)
class CircuitBreakerStats:
    state: Literal["closed", "open", "half_open"]
    failures: int
    probes: int
    calls: int
    rejected: int


class _UnexpectedAwaitable(TypeError):
    pass


@dataclass(frozen=True, slots=True)
class TaskScopeSnapshot:
    active: int
    completed: int
    failed: int
    cancelled: int
    closing: bool


@dataclass(frozen=True, slots=True)
class LatestTaskSnapshot:
    active: Mapping[Hashable, int]
    completed: int
    discarded_results: int


@dataclass(frozen=True, slots=True)
class SchedulerStats:
    active: int
    completed: int
    failed: int
    cancelled: int
    closed: bool


@dataclass(slots=True)
class _ResourceEntry:
    resource: object
    cleanup: Callable[[], object] | None


def _resolve_cleanup(
    resource: object, close: str | Callable[[object], object]
) -> Callable[[], object]:
    if isinstance(close, str):
        cleanup = getattr(resource, close, None)
        if not callable(cleanup):
            raise TypeError(f"resource must provide callable {close!r}")
        return cleanup
    if not callable(close):
        raise TypeError("close must be a method name or callable")
    return lambda: close(resource)


class _BaseRetryPolicy:
    """Common backoff, deadline, and observation policy."""

    def __init__(
        self,
        max_attempts: int = 3,
        *,
        backoff: Literal["fixed", "exponential"] = "exponential",
        initial_delay: float = 0.0,
        max_delay: float = 60.0,
        jitter: float = 0.0,
        observer: Callable[[RetryObservation], None] | None = None,
    ) -> None:
        self._attempts = validate_positive_capacity(max_attempts, name="Retry max attempts")
        if backoff not in ("fixed", "exponential"):
            raise ValueError("backoff must be 'fixed' or 'exponential'")
        self._backoff = backoff
        self._initial = validate_finite_duration(
            initial_delay, name="initial_delay", allow_zero=True
        )
        self._maximum = validate_finite_duration(max_delay, name="max_delay", allow_zero=True)
        if self._maximum < self._initial:
            raise ValueError("max_delay must be at least initial_delay")
        self._jitter = validate_finite_duration(jitter, name="jitter", allow_zero=True)
        if self._jitter > 1:
            raise ValueError("jitter must be between 0 and 1")
        if observer is not None and not callable(observer):
            raise TypeError("observer must be callable")
        self._observer = observer

    def _delay(self, attempt: int) -> float:
        delay = self._initial
        if self._backoff == "exponential" and attempt > 1 and delay > 0:
            if self._maximum == 0 or attempt - 1 >= (math.log2(self._maximum) - math.log2(delay)):
                delay = self._maximum
            else:
                delay = math.ldexp(delay, attempt - 1)
        delay = min(delay, self._maximum)
        if self._jitter:
            delay *= random.uniform(1.0 - self._jitter, 1.0 + self._jitter)
        return min(delay, self._maximum)

    def _observe(self, observation: RetryObservation, primary: Exception | None = None) -> None:
        if self._observer is None:
            return
        try:
            synchronous_result(self._observer(observation))
        except Exception as observer_error:
            if primary is None:
                raise
            raise ExceptionGroup(
                "Retry operation and observer both failed", [primary, observer_error]
            ) from primary

    @staticmethod
    def _should_retry(predicate: Callable[[Exception], bool] | None, error: Exception) -> bool:
        if predicate is None:
            return False
        result = predicate(error)
        if type(result) is bool:
            return result
        if type(result) not in _NON_AWAITABLE_TYPES:
            synchronous_result(result)
        raise TypeError("should_retry must return bool")

    def _failure(
        self,
        attempt: int,
        started: float,
        predicate: Callable[[Exception], bool] | None,
        error: Exception,
    ) -> bool:
        retry = attempt < self._attempts and self._should_retry(predicate, error)
        if self._observer is not None:
            self._observe(
                RetryObservation(
                    attempt, time.monotonic() - started, "retry" if retry else "error", error
                ),
                error,
            )
        return retry

    def _pause_budget(self, attempt: int, deadline: Deadline | None) -> tuple[float, bool]:
        delay = self._delay(attempt)
        remaining = deadline.remaining if deadline is not None else None
        if remaining is not None and remaining <= delay:
            return remaining, True
        return delay, False


class _BaseCircuitBreaker:
    """Common closed, open, and half-open transition state."""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        *,
        half_open_max: int = 1,
        should_count: Callable[[Exception], bool] | None = None,
        observer: Callable[[CircuitBreakerStats], object] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._threshold = validate_positive_capacity(failure_threshold, name="failure_threshold")
        self._recovery = validate_finite_duration(recovery_timeout, name="recovery_timeout")
        if self._recovery == 0:
            raise ValueError("recovery_timeout must be positive")
        self._half_open_max = validate_positive_capacity(half_open_max, name="half_open_max")
        if should_count is not None and not callable(should_count):
            raise TypeError("should_count must be callable")
        if observer is not None and not callable(observer):
            raise TypeError("observer must be callable")
        if not callable(clock):
            raise TypeError("clock must be callable")
        self._should_count = should_count if should_count is not None else (lambda _error: True)
        self._observer = observer
        self._clock = clock
        self._lock = threading.Lock()
        self._state: Literal["closed", "open", "half_open"] = "closed"
        self._failures = 0
        self._probes = 0
        self._opened_at = 0.0
        self._generation = 0
        self._calls = 0
        self._rejected = 0

    @property
    def state(self) -> str:
        with self._lock:
            self._advance()
            return self._state

    def _advance(self) -> None:
        if self._state == "open" and self._clock() - self._opened_at >= self._recovery:
            self._state = "half_open"
            self._probes = 0
            self._generation += 1

    def _enter(self) -> tuple[bool, int]:
        with self._lock:
            self._advance()
            if self._state == "open" or (
                self._state == "half_open" and self._probes >= self._half_open_max
            ):
                self._rejected += 1
                raise CircuitOpenError(f"Circuit breaker is {self._state}")
            self._calls += 1
            probe = self._state == "half_open"
            if probe:
                self._probes += 1
            return probe, self._generation

    def _finish(
        self,
        probe: bool,
        epoch: int,
        error: Exception | None,
        cancelled: bool = False,
    ) -> None:
        counted = False
        classification_error: Exception | None = None
        if error is not None:
            try:
                decision = self._should_count(error)
                if type(decision) is bool:
                    counted = decision
                else:
                    if is_awaitable(decision):
                        close_awaitable(decision)
                        raise TypeError("CircuitBreaker should_count must be synchronous")
                    raise TypeError("should_count must return bool")
            except Exception as exc:
                counted = True
                classification_error = exc
        with self._lock:
            if epoch != self._generation:
                return
            if cancelled:
                if probe:
                    self._probes = max(0, self._probes - 1)
                return
            if probe:
                self._probes = max(0, self._probes - 1)
                if counted:
                    self._state = "open"
                    self._opened_at = self._clock()
                    self._failures = self._threshold
                    self._probes = 0
                    self._generation += 1
                elif self._probes == 0:
                    self._state = "closed"
                    self._failures = 0
                    self._generation += 1
            elif self._state == "closed":
                self._failures = self._failures + 1 if counted else 0
                if self._failures >= self._threshold:
                    self._state = "open"
                    self._opened_at = self._clock()
                    self._generation += 1
        if self._observer is not None:
            try:
                self._notify()
            except Exception as observer_error:
                if error is not None:
                    raise ExceptionGroup(
                        "Circuit operation and observer both failed", [error, observer_error]
                    ) from error
                raise
        if classification_error is not None and error is not None:
            raise ExceptionGroup(
                "Circuit failure classifier failed", [error, classification_error]
            ) from error

    def _notify(self) -> None:
        if self._observer is not None:
            result = self._observer(self.stats())
            if type(result) not in _NON_AWAITABLE_TYPES and is_awaitable(result):
                close_awaitable(result)
                raise TypeError("CircuitBreaker observer must be synchronous")

    def stats(self) -> CircuitBreakerStats:
        with self._lock:
            self._advance()
            return CircuitBreakerStats(
                self._state, self._failures, self._probes, self._calls, self._rejected
            )

    def reset(self) -> None:
        with self._lock:
            self._state = "closed"
            self._failures = self._probes = 0
            self._opened_at = 0.0
            self._generation += 1
        if self._observer is not None:
            self._notify()


class _BaseCooldownRegistry(ABC, Generic[K]):
    """Thread-safe per-key monotonic cooldowns with sync and cancellable async waits."""

    def __init__(
        self,
        *,
        max_entries: int = 4096,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._max_entries = validate_positive_capacity(max_entries, name="Cooldown max entries")
        if not callable(clock):
            raise TypeError("clock must be callable")
        self._clock = clock
        self._lock = threading.RLock()
        self._deadlines: dict[K, float] = {}

    def _prune_locked(self, now: float) -> int:
        expired = [key for key, deadline in self._deadlines.items() if deadline <= now]
        for key in expired:
            self._deadlines.pop(key, None)
        return len(expired)

    def block(self, key: K, duration: float, *, extend: bool = True) -> float:
        """Block a key from the time the update acquires the registry lock.

        The clock runs under that lock and must be fast and free of side effects.
        By default, a new block only extends the existing deadline.
        """
        seconds = validate_finite_duration(duration, name="duration", allow_zero=True)
        if not isinstance(extend, bool):
            raise TypeError("extend must be a bool")
        with self._lock:
            now = self._clock()
            current = self._deadlines.get(key)
            if current is not None and current <= now:
                self._deadlines.pop(key)
                current = None
            if current is None and len(self._deadlines) >= self._max_entries:
                self._prune_locked(now)
                if len(self._deadlines) >= self._max_entries:
                    raise RuntimeError("CooldownRegistry entry capacity is full")
            deadline = now + seconds
            if current is not None:
                deadline = max(current, deadline) if extend else deadline
            if deadline <= now:
                self._deadlines.pop(key, None)
            else:
                self._deadlines[key] = deadline
            self._notify_waiters_locked(key)
            return max(0.0, deadline - now)

    def remaining(self, key: K) -> float:
        with self._lock:
            deadline = self._deadlines.get(key)
            if deadline is None:
                return 0.0
            remaining = deadline - self._clock()
            if remaining <= 0:
                self._deadlines.pop(key, None)
                return 0.0
            return remaining

    def is_blocked(self, key: K) -> bool:
        return self.remaining(key) > 0.0

    def clear(self, key: K) -> bool:
        with self._lock:
            removed = self._deadlines.pop(key, None) is not None
            self._notify_waiters_locked(key)
            return removed

    def prune(self) -> int:
        with self._lock:
            if not self._deadlines:
                return 0
            return self._prune_locked(self._clock())

    @abstractmethod
    def _notify_waiters_locked(self, key: K) -> None:
        """Wake waiters for a changed key while holding the registry lock."""

    @abstractmethod
    def _waiter_count_locked(self) -> int:
        """Return the number of registered waiters."""

    def stats(self) -> CooldownStats:
        with self._lock:
            self._prune_locked(self._clock())
            count = len(self._deadlines)
            return CooldownStats(count, count, self._waiter_count_locked())


@dataclass(slots=True)
class _TokenBucket:
    tokens: float
    updated: float
    waiters: int = 0


R = TypeVar("R", bound=_TokenBucket)


class _BaseKeyedRateLimiter(ABC, Generic[K, R]):
    """Own token bucket math and eviction. Callers serialize access."""

    def __init__(
        self,
        rate: float,
        period: float = 1.0,
        burst: float | None = None,
        *,
        max_keys: int = 4096,
        observer: OperationStats | None = None,
    ) -> None:
        self._rate = validate_finite_duration(rate, name="rate")
        self._period = validate_finite_duration(period, name="period")
        default_burst = max(self._rate, 1.0)
        self._burst = validate_finite_duration(
            burst if burst is not None else default_burst, name="burst"
        )
        if self._burst < 1:
            raise ValueError("burst must be at least one token")
        if isinstance(max_keys, bool) or not isinstance(max_keys, int):
            raise TypeError("max_keys must be an integer")
        if max_keys < 1:
            raise ValueError("max_keys must be positive")
        _require_operation_stats(observer)
        self._max_keys = max_keys
        self._observer = observer
        self._entries: dict[K, R] = {}
        self._closed = False

    @abstractmethod
    def _new_bucket(self, now: float) -> R:
        """Create a full bucket using the caller's clock."""

    def _refill(self, entry: _TokenBucket, now: float) -> None:
        entry.tokens = min(
            self._burst, entry.tokens + (now - entry.updated) * self._rate / self._period
        )
        entry.updated = now

    def _entry(self, key: K, now: float) -> R:
        entry = self._entries.get(key)
        if entry is None:
            if len(self._entries) >= self._max_keys:
                self._prune_at(now)
            if len(self._entries) >= self._max_keys:
                raise RuntimeError("KeyedRateLimiter key capacity is full")
            entry = self._new_bucket(now)
            self._entries[key] = entry
        else:
            self._refill(entry, now)
        return entry

    def _prune(self, key: K, entry: R) -> None:
        if entry.waiters == 0 and entry.tokens >= self._burst:
            self._entries.pop(key, None)

    def _prune_at(self, now: float) -> int:
        before = len(self._entries)
        for key, entry in tuple(self._entries.items()):
            self._refill(entry, now)
            self._prune(key, entry)
        return before - len(self._entries)

    def _available_at(self, key: K, now: float) -> float:
        entry = self._entries.get(key)
        if entry is None:
            return self._burst
        self._refill(entry, now)
        self._prune(key, entry)
        return entry.tokens

    def _try_acquire_at(self, key: K, now: float) -> bool:
        if self._closed:
            return False
        entry = self._entry(key, now)
        acquired = not entry.waiters and entry.tokens >= 1.0
        if acquired:
            entry.tokens -= 1.0
        self._prune(key, entry)
        return acquired

    def _record_try_acquire(self, acquired: bool) -> None:
        if self._observer is not None:
            if acquired:
                self._observer.record("keyed_rate.try_acquire")
            else:
                self._observer.record("keyed_rate.try_acquire", status="error", rejected=1)

    def _stats(self) -> KeyedRateStats:
        return KeyedRateStats(
            len(self._entries), sum(entry.waiters for entry in self._entries.values()), self._closed
        )


class _BaseResourceScope:
    """Own the cleanup stack. Callers enforce their synchronization boundary."""

    def __init__(self) -> None:
        self._entries: list[_ResourceEntry] = []
        self._resource_ids: set[int] = set()
        self._closed = False
        self._inflight = 0

    def _add(self, resource: T, cleanup: Callable[[], object] | None) -> T:
        if self._closed:
            raise RuntimeError("Cannot add a resource to a closed scope")
        resource_id = id(resource)
        if resource_id not in self._resource_ids:
            self._entries.append(_ResourceEntry(resource, cleanup))
            self._resource_ids.add(resource_id)
        return resource

    def _defer(self, cleanup: Callable[[], object]) -> Callable[[], object]:
        if self._closed:
            raise RuntimeError("Cannot add cleanup to a closed scope")
        self._entries.append(_ResourceEntry(cleanup, cleanup))
        return cleanup

    def _detach(self, resource: object) -> object:
        for index in range(len(self._entries) - 1, -1, -1):
            if self._entries[index].resource is resource:
                detached = self._entries.pop(index).resource
                self._resource_ids.discard(id(detached))
                return detached
        raise KeyError("resource is not registered in this scope")

    def _pending_cleanup(self) -> int:
        return self._inflight + sum(entry.cleanup is not None for entry in self._entries)

    def _take_pending(self) -> list[_ResourceEntry]:
        pending, self._entries = self._entries, []
        self._inflight = sum(entry.cleanup is not None for entry in pending)
        pending.reverse()
        return pending

    def _retain_failures(self, failed: list[_ResourceEntry]) -> None:
        failed.reverse()
        self._entries = failed
        self._resource_ids = {id(entry.resource) for entry in failed}


def _periodic_parameters(
    interval: float,
    initial_delay: float | None,
    max_parallel: int,
    overlap: Literal["skip", "queue", "parallel"],
) -> tuple[float, float, int]:
    period = validate_finite_duration(interval, name="interval")
    if overlap not in ("skip", "queue", "parallel"):
        raise ValueError("overlap must be 'skip', 'queue', or 'parallel'")
    first = validate_nonnegative_timeout(initial_delay, name="initial delay")
    return (
        period,
        period if first is None else first,
        validate_positive_capacity(max_parallel, name="max_parallel"),
    )
