from __future__ import annotations

import threading
import time
from collections.abc import Callable, Hashable
from dataclasses import dataclass
from threading import Condition
from typing import TypeVar

from ._callbacks import _NON_AWAITABLE_TYPES, close_awaitable, is_awaitable
from ._validation import validate_nonnegative_timeout
from .base import (
    RetryObservation,
    _BaseCircuitBreaker,
    _BaseCooldownRegistry,
    _BaseRetryPolicy,
    _UnexpectedAwaitable,
)
from .timing import Deadline

K = TypeVar("K", bound=Hashable)
T = TypeVar("T")


class RetryPolicy(_BaseRetryPolicy):
    """Retry operations using this backend's clock and waiting mechanism."""

    def run(
        self,
        operation: Callable[[], T],
        *,
        should_retry: Callable[[Exception], bool] | None = None,
        deadline: Deadline | None = None,
    ) -> T:
        """Run sync attempts within a retry budget; running work is not interrupted."""
        if not callable(operation):
            raise TypeError("operation must be callable")
        if should_retry is not None and not callable(should_retry):
            raise TypeError("should_retry must be callable")
        for attempt in range(1, self._attempts + 1):
            if deadline is not None and deadline.expired:
                raise TimeoutError("Retry deadline expired before the next attempt")
            started = time.monotonic() if self._observer is not None else 0.0
            try:
                result = operation()
                if type(result) not in _NON_AWAITABLE_TYPES and is_awaitable(result):
                    close_awaitable(result)
                    raise _UnexpectedAwaitable("RetryPolicy.run requires a synchronous operation")
            except _UnexpectedAwaitable:
                raise
            except Exception as error:
                if not self._failure(attempt, started, should_retry, error):
                    raise
                delay, expired = self._pause_budget(attempt, deadline)
                if delay:
                    time.sleep(delay)
                if expired:
                    raise TimeoutError("Retry deadline expired before another attempt") from error
            else:
                if self._observer is not None:
                    self._observe(RetryObservation(attempt, time.monotonic() - started, "success"))
                return result
        raise AssertionError("unreachable retry state")


@dataclass(slots=True)
class _CooldownWaitGroup:
    condition: Condition
    count: int = 0


class CooldownRegistry(_BaseCooldownRegistry[K]):
    """Thread-safe cooldown registry with blocking waits."""

    def __init__(
        self, *, max_entries: int = 4096, clock: Callable[[], float] = time.monotonic
    ) -> None:
        super().__init__(max_entries=max_entries, clock=clock)
        self._waiters: dict[K, _CooldownWaitGroup] = {}

    def _notify_waiters_locked(self, key: K) -> None:
        group = self._waiters.get(key)
        if group is not None:
            group.condition.notify_all()

    def _waiter_count_locked(self) -> int:
        return sum(group.count for group in self._waiters.values())

    def wait(self, key: K, timeout: float | None = None) -> bool:
        """Block the calling thread until cooldown ends; return False on timeout."""
        timeout = validate_nonnegative_timeout(timeout, name="cooldown wait timeout")
        end = None if timeout is None else time.monotonic() + timeout
        group: _CooldownWaitGroup | None = None
        with self._lock:
            try:
                while True:
                    now = self._clock()
                    deadline = self._deadlines.get(key)
                    if deadline is None or deadline <= now:
                        self._deadlines.pop(key, None)
                        return True
                    remaining = deadline - now
                    if end is not None:
                        remaining_timeout = end - time.monotonic()
                        if remaining_timeout <= 0:
                            return False
                        remaining = min(remaining, remaining_timeout)
                    if group is None:
                        group = self._waiters.get(key)
                        if group is None:
                            group = _CooldownWaitGroup(Condition(self._lock))
                            self._waiters[key] = group
                        group.count += 1
                    group.condition.wait(min(remaining, threading.TIMEOUT_MAX))
            finally:
                if group is not None:
                    group.count -= 1
                    if group.count == 0:
                        del self._waiters[key]


class CircuitBreaker(_BaseCircuitBreaker):
    """Protect a callable with the shared circuit state machine."""

    def call(self, operation: Callable[[], T]) -> T:
        if not callable(operation):
            raise TypeError("operation must be callable")
        probe, epoch = self._enter()
        try:
            result = operation()
            if type(result) not in _NON_AWAITABLE_TYPES and is_awaitable(result):
                close_awaitable(result)
                raise _UnexpectedAwaitable("CircuitBreaker.call requires a synchronous operation")
        except _UnexpectedAwaitable:
            self._finish(probe, epoch, None, cancelled=True)
            raise
        except Exception as error:
            self._finish(probe, epoch, error)
            raise
        except BaseException:
            self._finish(probe, epoch, None, cancelled=True)
            raise
        self._finish(probe, epoch, None)
        return result
