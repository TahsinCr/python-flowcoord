"""Thread-based keyed coalescing and scheduled work."""

from __future__ import annotations

import heapq
import inspect
import logging
import math
import threading
import time
from collections import deque
from collections.abc import Callable, Hashable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Generic, Literal, TypeVar

from ._callbacks import _NON_AWAITABLE_TYPES, close_awaitable, is_async_callable, synchronous_result
from ._compatibility import BaseExceptionGroup, ExceptionGroup
from ._validation import (
    validate_finite_duration,
    validate_nonnegative_timeout,
    validate_positive_capacity,
)
from .base import SchedulerStats, _periodic_parameters
from .observability import OperationStats, _require_operation_stats

K = TypeVar("K", bound=Hashable)

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class CoalescerStats:
    pending_keys: int
    submitted: int
    merged: int
    delivered: int
    failures: int
    rejected: int
    discarded: int
    unreported_failures: int
    closed: bool


@dataclass(slots=True)
class _Coalesced(Generic[T]):
    value: T
    deadline: float
    sequence: int


class KeyedCoalescer(Generic[K, T]):
    """Merge repeated keyed updates and deliver them from one owned worker thread.

    The first update for a key starts its coalescing window. User key and merge
    callbacks run on the calling thread; they should be quick and non-reentrant.
    Delivery always runs on the coalescer's worker thread.
    """

    def __init__(
        self,
        *,
        interval: float,
        key: Callable[[T], K],
        merge: Callable[[T, T], T],
        callback: Callable[[T], object],
        max_pending_keys: int = 128,
        on_error: Callable[[BaseException], object] | None = None,
        observer: OperationStats | None = None,
    ) -> None:
        self._interval = validate_finite_duration(interval, name="interval")
        self._key = key
        self._merge = merge
        self._callback = callback
        self._capacity = validate_positive_capacity(max_pending_keys, name="max_pending_keys")
        if not all(callable(item) for item in (key, merge, callback)):
            raise TypeError("key, merge and callback must be callable")
        if on_error is not None and not callable(on_error):
            raise TypeError("on_error must be callable")
        _require_operation_stats(observer)
        self._on_error = on_error
        self._observer = observer
        self._condition = threading.Condition()
        self._pending: dict[K, _Coalesced[T]] = {}
        self._due_heap: list[tuple[float, int, K]] = []
        self._sequence = 0
        self._closed = False
        self._flushing = False
        self._flush_requests = 0
        self._close_flush = False
        self._active = 0
        self._submitted = self._merged = self._delivered = self._failures = self._rejected = 0
        self._errors: deque[BaseException] = deque(maxlen=32)
        self._unreported_failures = 0
        self._discarded = 0
        self._thread = threading.Thread(target=self._run, name="FlowCoordCoalescer", daemon=True)
        self._thread_started = False

    def push(self, value: T) -> None:
        key = self._key(value)
        if type(key) not in _NON_AWAITABLE_TYPES:
            key = synchronous_result(key)
        with self._condition:
            if self._closed:
                raise RuntimeError("KeyedCoalescer is closed")
            entry = self._pending.get(key)
            if entry is None:
                if len(self._pending) >= self._capacity:
                    self._rejected += 1
                    raise RuntimeError("KeyedCoalescer pending-key capacity is full")
                if not self._thread_started:
                    self._thread.start()
                    self._thread_started = True
                self._sequence += 1
                entry = _Coalesced(value, time.monotonic() + self._interval, self._sequence)
                self._pending[key] = entry
                heapq.heappush(self._due_heap, (entry.deadline, entry.sequence, key))
                self._submitted += 1
                self._condition.notify()
            else:
                merged = self._merge(entry.value, value)
                if type(merged) not in _NON_AWAITABLE_TYPES:
                    merged = synchronous_result(merged)
                entry.value = merged
                self._merged += 1

    def _next_due(self) -> tuple[K, T] | None:
        if self._flushing and self._pending:
            key = next(iter(self._pending))
            entry = self._pending.pop(key)
            self._compact_due_heap()
            return key, entry.value
        if not self._pending:
            self._due_heap.clear()
            return None
        while self._due_heap:
            deadline, sequence, key = self._due_heap[0]
            entry = self._pending.get(key)
            if entry is not None and entry.sequence == sequence:
                break
            heapq.heappop(self._due_heap)
        else:
            raise RuntimeError("KeyedCoalescer deadline index is inconsistent")
        delay = deadline - time.monotonic()
        if delay > 0:
            self._condition.wait(delay)
            return None
        heapq.heappop(self._due_heap)
        self._pending.pop(key)
        return key, entry.value

    def _compact_due_heap(self) -> None:
        if not self._pending:
            self._due_heap.clear()
        elif len(self._due_heap) > max(64, len(self._pending) * 2):
            self._due_heap = [
                (entry.deadline, entry.sequence, key) for key, entry in self._pending.items()
            ]
            heapq.heapify(self._due_heap)

    def _run(self) -> None:
        while True:
            with self._condition:
                while True:
                    item = self._next_due()
                    if item is not None:
                        self._active += 1
                        break
                    if self._closed and not self._pending:
                        self._condition.notify_all()
                        return
                    if not self._pending:
                        self._condition.wait()
            started = time.monotonic()
            status = "success"
            try:
                result = self._callback(item[1])
                if inspect.isawaitable(result):
                    close_awaitable(result)
                    raise TypeError("KeyedCoalescer callback must be synchronous")
            except BaseException as error:
                status = "error"
                with self._condition:
                    self._failures += 1
                    self._capture_error(error)
                if self._on_error is not None:
                    try:
                        result = self._on_error(error)
                        if inspect.isawaitable(result):
                            close_awaitable(result)
                            raise TypeError("KeyedCoalescer on_error must be synchronous")
                    except BaseException as observer_error:
                        with self._condition:
                            self._capture_error(observer_error)
            else:
                with self._condition:
                    self._delivered += 1
            finally:
                if self._observer is not None:
                    try:
                        self._observer.record(
                            "keyed_coalescer.delivery",
                            status=status,
                            elapsed=time.monotonic() - started,
                            queue_depth=self.pending_count,
                        )
                    except BaseException as observer_error:
                        with self._condition:
                            self._capture_error(observer_error)
                with self._condition:
                    self._active -= 1
                    self._condition.notify_all()

    def _capture_error(self, error: BaseException) -> None:
        if len(self._errors) == self._errors.maxlen:
            self._unreported_failures += 1
        self._errors.append(error)

    @property
    def pending_count(self) -> int:
        with self._condition:
            return len(self._pending)

    def stats(self) -> CoalescerStats:
        with self._condition:
            return CoalescerStats(
                len(self._pending),
                self._submitted,
                self._merged,
                self._delivered,
                self._failures,
                self._rejected,
                self._discarded,
                self._unreported_failures,
                self._closed,
            )

    def _raise_errors(self) -> None:
        with self._condition:
            errors = list(self._errors)
            self._errors.clear()
            unreported, self._unreported_failures = self._unreported_failures, 0
        if unreported:
            errors.append(
                RuntimeError(f"{unreported} earlier coalescer error(s) were not retained")
            )
        if errors:
            if all(isinstance(error, Exception) for error in errors):
                raise ExceptionGroup(
                    "KeyedCoalescer delivery failed",
                    [error for error in errors if isinstance(error, Exception)],
                )
            raise BaseExceptionGroup("KeyedCoalescer delivery failed", errors)

    def flush(self, timeout: float | None = None) -> None:
        """Deliver all pending values and wait for the worker to become idle."""
        if threading.current_thread() is self._thread:
            raise RuntimeError("KeyedCoalescer worker cannot flush itself")
        timeout = validate_nonnegative_timeout(timeout, name="coalescer flush timeout")
        end = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            discarded = self._discarded
            self._flush_requests += 1
            self._flushing = True
            self._condition.notify_all()
            try:
                while self._pending or self._active:
                    remaining = None if end is None else end - time.monotonic()
                    if remaining is not None and remaining <= 0:
                        raise TimeoutError("KeyedCoalescer flush did not finish before the timeout")
                    self._condition.wait(remaining)
                if self._discarded != discarded:
                    raise RuntimeError("KeyedCoalescer closed and discarded pending values")
            finally:
                self._flush_requests -= 1
                self._flushing = self._flush_requests > 0 or self._close_flush
                self._condition.notify_all()
        self._raise_errors()

    def close(self, *, flush: bool = True, timeout: float | None = None) -> None:
        """Reject updates and apply the first close call's flush/discard policy."""
        if not isinstance(flush, bool):
            raise TypeError("flush must be a bool")
        timeout = validate_nonnegative_timeout(timeout, name="coalescer close timeout")
        if threading.current_thread() is self._thread:
            raise RuntimeError("KeyedCoalescer worker cannot close itself")
        end = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            if not self._closed:
                self._closed = True
                self._close_flush = flush
                if not flush:
                    self._discarded += len(self._pending)
                    self._pending.clear()
                self._due_heap.clear()
            self._flushing = self._flush_requests > 0 or self._close_flush
            self._condition.notify_all()
        if self._thread_started:
            remaining = None if end is None else max(0.0, end - time.monotonic())
            self._thread.join(remaining)
            if self._thread.is_alive():
                raise TimeoutError("KeyedCoalescer callback is still running")
        self._raise_errors()

    def __enter__(self) -> KeyedCoalescer[K, T]:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


@dataclass(eq=False, slots=True)
class _ScheduledJob:
    callback: Callable[[], object]
    name: str
    interval: float | None
    overlap: Literal["skip", "queue", "parallel"]
    max_parallel: int
    future: Future[object] = field(default_factory=Future)
    active: int = 0
    cancelled: bool = False
    next_tick: float = 0.0
    armed: bool = False
    catching_up: bool = False
    failure: BaseException | None = None


class ScheduledHandle:
    """A scheduled thread callback; running code cooperates with shutdown."""

    def __init__(self, owner: Scheduler, job: _ScheduledJob) -> None:
        self._owner = owner
        self._job = job
        self.name = job.name

    @property
    def done(self) -> bool:
        return self._job.future.done()

    def cancel(self) -> bool:
        return self._owner.cancel(self)

    def join(self, timeout: float | None = None) -> object:
        timeout = validate_nonnegative_timeout(timeout, name="scheduled task timeout")
        if not self.done:
            self._owner._require_non_worker()
        if timeout is not None:
            timeout = min(timeout, threading.TIMEOUT_MAX)
        return self._job.future.result(timeout)


class Scheduler:
    """Dispatch timers to a bounded number of workers using one timer thread."""

    def __init__(
        self,
        *,
        max_workers: int = 4,
        max_jobs: int = 4096,
        on_error: Callable[[BaseException], object] | None = None,
        observer: OperationStats | None = None,
    ) -> None:
        self._workers = validate_positive_capacity(max_workers, name="scheduler max workers")
        self._capacity = validate_positive_capacity(max_jobs, name="scheduler max jobs")
        if on_error is not None and (not callable(on_error) or is_async_callable(on_error)):
            raise TypeError("on_error must be a synchronous callable")
        _require_operation_stats(observer)
        self._on_error = on_error
        self._observer = observer
        self._changed = threading.Condition()
        self._worker = threading.local()
        self._pool = ThreadPoolExecutor(
            max_workers=self._workers, thread_name_prefix="FlowCoordScheduled"
        )
        self._thread: threading.Thread | None = None
        self._jobs: set[_ScheduledJob] = set()
        self._due: list[tuple[float, int, _ScheduledJob]] = []
        self._sequence = self._running = 0
        self._completed = self._failed = self._cancelled = 0
        self._closed = False

    def _enqueue(self, when: float, job: _ScheduledJob) -> None:
        self._sequence += 1
        job.armed = True
        heapq.heappush(self._due, (when, self._sequence, job))

    def _register(
        self,
        callback: Callable[..., object],
        args: tuple[object, ...],
        kwargs: dict[str, object],
        first: float,
        interval: float | None,
        overlap: Literal["skip", "queue", "parallel"],
        max_parallel: int,
        name: str,
    ) -> ScheduledHandle:
        if not callable(callback) or is_async_callable(callback):
            raise TypeError("callback must be a synchronous callable")
        if not isinstance(name, str) or not name:
            raise ValueError("job name must be a non-empty string")
        job = _ScheduledJob(
            lambda: callback(*args, **kwargs), name, interval, overlap, max_parallel
        )
        with self._changed:
            if self._closed:
                raise RuntimeError("Scheduler is closed")
            if len(self._jobs) >= self._capacity:
                raise RuntimeError("Scheduler capacity is full")
            if self._thread is None:
                timer = threading.Thread(
                    target=self._dispatch, name="FlowCoordScheduler", daemon=True
                )
                timer.start()
                self._thread = timer
            self._jobs.add(job)
            self._enqueue(time.monotonic() + first, job)
            self._changed.notify()
        return ScheduledHandle(self, job)

    def schedule_once(
        self,
        callback: Callable[..., object],
        *args: object,
        delay: float = 0.0,
        name: str | None = None,
        **kwargs: object,
    ) -> ScheduledHandle:
        first = validate_nonnegative_timeout(delay, name="schedule delay")
        return self._register(
            callback,
            args,
            kwargs,
            first or 0.0,
            None,
            "skip",
            1,
            "FlowCoordScheduledOnce" if name is None else name,
        )

    def periodic(
        self,
        callback: Callable[..., object],
        *args: object,
        interval: float,
        overlap: Literal["skip", "queue", "parallel"] = "skip",
        initial_delay: float | None = None,
        max_parallel: int = 16,
        name: str | None = None,
        **kwargs: object,
    ) -> ScheduledHandle:
        period, first, limit = _periodic_parameters(interval, initial_delay, max_parallel, overlap)
        return self._register(
            callback,
            args,
            kwargs,
            first,
            period,
            overlap,
            limit,
            "FlowCoordPeriodic" if name is None else name,
        )

    @staticmethod
    def _next_tick(previous: float, now: float, interval: float) -> float:
        remainder = math.fmod(now - previous, interval)
        # Keep the next deadline strictly in the future even below clock precision.
        return max(math.nextafter(now, math.inf), now + (interval - remainder))

    def _dispatch(self) -> None:
        while True:
            with self._changed:
                while not self._closed:
                    if not self._due or self._running >= self._workers:
                        self._changed.wait()
                        continue
                    when, _, job = self._due[0]
                    if job.cancelled or job.failure is not None or job.future.done():
                        heapq.heappop(self._due)
                        continue
                    now = time.monotonic()
                    if when > now:
                        self._changed.wait(min(when - now, threading.TIMEOUT_MAX))
                        continue
                    heapq.heappop(self._due)
                    job.armed = False
                    job.active += 1
                    self._running += 1
                    if job.interval is not None:
                        job.next_tick = self._next_tick(when, now, job.interval)
                        if job.overlap == "parallel" and job.active < job.max_parallel:
                            self._enqueue(job.next_tick, job)
                    break
                else:
                    return
            try:
                self._pool.submit(self._run, job)
            except BaseException as error:
                self._complete(job, None, error, submission_failed=True)
                self._report_error(error)

    def _run(self, job: _ScheduledJob) -> None:
        self._worker.owned = True
        observer = self._observer
        started = time.monotonic() if observer is not None else 0.0
        with self._changed:
            cancelled = job.cancelled
        result: object = None
        error: BaseException | None = None
        try:
            if not cancelled:
                result = synchronous_result(job.callback())
        except BaseException as exc:
            error = exc
        try:
            self._complete(job, result, error)
            if observer is not None and not cancelled:
                observer.record(
                    "scheduler.once" if job.interval is None else "scheduler.periodic",
                    status="error" if error is not None else "success",
                    elapsed=time.monotonic() - started,
                )
            if error is not None:
                self._report_error(error)
        finally:
            self._worker.owned = False

    def _complete(
        self,
        job: _ScheduledJob,
        result: object,
        error: BaseException | None,
        *,
        submission_failed: bool = False,
    ) -> None:
        with self._changed:
            job.active -= 1
            self._running -= 1
            if error is not None:
                if job.overlap == "parallel" and not submission_failed:
                    self._failed += 1
                elif job.failure is None:
                    job.failure = error
            if job.cancelled:
                if not job.active:
                    self._cancel_job(job)
            elif (job.interval is None or job.failure is not None) and not job.active:
                self._jobs.remove(job)
                self._prune_timers()
                if job.failure is None:
                    self._completed += 1
                    job.future.set_result(result)
                else:
                    self._failed += 1
                    job.future.set_exception(job.failure)
            elif job.failure is None and not job.armed and job.interval is not None:
                now = time.monotonic()
                when = job.next_tick
                if when < now and job.overlap == "queue":
                    when = now + job.interval if job.catching_up else now
                    job.catching_up = not job.catching_up
                elif when < now:
                    when = self._next_tick(when, now, job.interval)
                else:
                    job.catching_up = False
                self._enqueue(when, job)
            self._changed.notify_all()

    def _report_error(self, error: BaseException) -> None:
        if self._on_error is not None:
            try:
                synchronous_result(self._on_error(error))
                return
            except BaseException as observer_error:
                error = BaseExceptionGroup(
                    "Scheduled job and error observer both failed", [error, observer_error]
                )
        logging.getLogger(__name__).error("Scheduled job failed", exc_info=error)

    def _cancel_job(self, job: _ScheduledJob) -> None:
        self._jobs.remove(job)
        self._cancelled += 1
        job.future.cancel()

    def cancel(self, handle: ScheduledHandle) -> bool:
        if not isinstance(handle, ScheduledHandle):
            raise TypeError("handle must be a ScheduledHandle")
        if handle._owner is not self:
            return False
        job = handle._job
        with self._changed:
            if job not in self._jobs or job.cancelled:
                return False
            job.cancelled = True
            if not job.active:
                self._cancel_job(job)
            self._prune_timers()
            self._changed.notify_all()
            return True

    def _prune_timers(self) -> None:
        if len(self._due) > 2 * len(self._jobs) + 32:
            self._due = [
                entry for entry in self._due if entry[2] in self._jobs and not entry[2].cancelled
            ]
            heapq.heapify(self._due)

    def _require_non_worker(self) -> None:
        if getattr(self._worker, "owned", False) or threading.current_thread() is self._thread:
            raise RuntimeError("A scheduler callback cannot wait on its own scheduler")

    def close(self, timeout: float | None = None) -> None:
        self._require_non_worker()
        timeout = validate_nonnegative_timeout(timeout, name="scheduler close timeout")
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._changed:
            self._closed = True
            for job in tuple(self._jobs):
                job.cancelled = True
                if not job.active:
                    self._cancel_job(job)
            self._due.clear()
            self._changed.notify_all()
            while self._jobs:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError("Scheduled jobs are still running")
                self._changed.wait(
                    None if remaining is None else min(remaining, threading.TIMEOUT_MAX)
                )
        if self._thread is not None:
            self._thread.join(
                None
                if deadline is None
                else min(max(0.0, deadline - time.monotonic()), threading.TIMEOUT_MAX)
            )
            if self._thread.is_alive():
                raise TimeoutError("Scheduler timer is still stopping")
        self._pool.shutdown(wait=False)

    def snapshot(self) -> SchedulerStats:
        with self._changed:
            return SchedulerStats(
                len(self._jobs), self._completed, self._failed, self._cancelled, self._closed
            )

    def __enter__(self) -> Scheduler:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()
