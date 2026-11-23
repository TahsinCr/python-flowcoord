from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import Awaitable, Callable, Hashable
from typing import TypeVar

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


class AsyncRetryPolicy(_BaseRetryPolicy):
    """Retry operations using this backend's clock and waiting mechanism."""

    async def run(
        self,
        operation: Callable[[], Awaitable[T]],
        *,
        should_retry: Callable[[Exception], bool] | None = None,
        deadline: Deadline | None = None,
    ) -> T:
        """Run async attempts within a retry budget; cancellation is never retried.

        The deadline gates new attempts and caps retry delays. It does not
        interrupt an operation that has already started.
        """
        if not callable(operation):
            raise TypeError("operation must be callable")
        if should_retry is not None and not callable(should_retry):
            raise TypeError("should_retry must be callable")
        for attempt in range(1, self._attempts + 1):
            if deadline is not None and deadline.expired:
                raise TimeoutError("Retry deadline expired before the next attempt")
            started = time.monotonic() if self._observer is not None else 0.0
            try:
                pending = operation()
                if not inspect.isawaitable(pending):
                    raise _UnexpectedAwaitable(
                        "AsyncRetryPolicy.run requires an awaitable operation"
                    )
                result = await pending
            except _UnexpectedAwaitable:
                raise
            except asyncio.CancelledError as cancelled:
                if self._observer is not None:
                    try:
                        self._observe(
                            RetryObservation(attempt, time.monotonic() - started, "error")
                        )
                    except BaseException as observer_error:
                        raise cancelled from observer_error
                raise
            except Exception as error:
                if not self._failure(attempt, started, should_retry, error):
                    raise
                delay, expired = self._pause_budget(attempt, deadline)
                await asyncio.sleep(delay)
                if expired:
                    raise TimeoutError("Retry deadline expired before another attempt") from error
            else:
                if self._observer is not None:
                    self._observe(RetryObservation(attempt, time.monotonic() - started, "success"))
                return result
        raise AssertionError("unreachable retry state")


class AsyncCooldownRegistry(_BaseCooldownRegistry[K]):
    """Cancellable waits on an event loop; cooldown changes may come from threads."""

    def __init__(
        self, *, max_entries: int = 4096, clock: Callable[[], float] = time.monotonic
    ) -> None:
        super().__init__(max_entries=max_entries, clock=clock)
        self._waiters: dict[K, set[tuple[asyncio.AbstractEventLoop, asyncio.Future[None]]]] = {}

    def _notify_waiters_locked(self, key: K) -> None:
        for loop, future in self._waiters.get(key, ()):
            if not loop.is_closed():
                try:
                    loop.call_soon_threadsafe(self._resolve, future)
                except RuntimeError:
                    pass

    def _waiter_count_locked(self) -> int:
        return sum(map(len, self._waiters.values()))

    @staticmethod
    def _resolve(future: asyncio.Future[None]) -> None:
        if not future.done():
            future.set_result(None)

    async def wait(self, key: K, timeout: float | None = None) -> bool:
        """Wait without blocking the loop; cancellation unregisters the waiter."""
        timeout = validate_nonnegative_timeout(timeout, name="cooldown wait timeout")
        loop = asyncio.get_running_loop()
        end = None if timeout is None else loop.time() + timeout
        while True:
            with self._lock:
                now = self._clock()
                deadline = self._deadlines.get(key)
                if deadline is None or deadline <= now:
                    self._deadlines.pop(key, None)
                    return True
                delay = deadline - now
                if end is not None:
                    remaining_timeout = end - loop.time()
                    if remaining_timeout <= 0:
                        return False
                    delay = min(delay, remaining_timeout)
                future: asyncio.Future[None] = loop.create_future()
                self._waiters.setdefault(key, set()).add((loop, future))
            try:
                await asyncio.wait_for(future, timeout=delay)
            except asyncio.TimeoutError:
                pass
            finally:
                with self._lock:
                    waiters = self._waiters.get(key)
                    if waiters is not None:
                        waiters.discard((loop, future))
                        if not waiters:
                            self._waiters.pop(key, None)


class AsyncCircuitBreaker(_BaseCircuitBreaker):
    """Protect a callable with the shared circuit state machine."""

    async def call(self, operation: Callable[[], Awaitable[T]]) -> T:
        if not callable(operation):
            raise TypeError("operation must be callable")
        probe, epoch = self._enter()
        try:
            pending = operation()
            if not inspect.isawaitable(pending):
                raise _UnexpectedAwaitable(
                    "AsyncCircuitBreaker.call requires an awaitable operation"
                )
            result = await pending
        except _UnexpectedAwaitable:
            self._finish(probe, epoch, None, cancelled=True)
            raise
        except asyncio.CancelledError:
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
