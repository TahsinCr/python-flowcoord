"""Bounded thread task ownership and latest-result coordination."""

from __future__ import annotations

import logging
import threading
import time
from _thread import LockType
from collections.abc import Callable, Hashable
from concurrent.futures import Future
from types import MappingProxyType
from typing import Any, Generic, Literal, TypeVar

from ._callbacks import is_async_callable, synchronous_result
from ._compatibility import BaseExceptionGroup, ExceptionGroup
from ._validation import validate_nonnegative_timeout
from .base import LatestTaskSnapshot, TaskScopeSnapshot
from .executors import BoundedWorkPool
from .observability import OperationStats, _require_operation_stats

T = TypeVar("T")
K = TypeVar("K", bound=Hashable)
Shutdown = Literal["cancel", "drain"]


class TaskHandle(Generic[T]):
    """Control a submitted call. Cancellation applies while it is queued."""

    def __init__(self, future: Future[T], name: str, worker: threading.local) -> None:
        self._future = future
        self.name = name
        self._worker = worker

    @property
    def done(self) -> bool:
        return self._future.done()

    def cancel(self) -> bool:
        return self._future.cancel()

    def join(self, timeout: float | None = None) -> T:
        timeout = validate_nonnegative_timeout(timeout, name="task join timeout")
        if not self.done and getattr(self._worker, "owned", False):
            raise RuntimeError("A scope worker cannot wait on another unfinished task in its scope")
        if timeout is not None:
            timeout = min(timeout, threading.TIMEOUT_MAX)
        return self._future.result(timeout)


class TaskScope:
    """Own bounded background work; running thread callbacks finish cooperatively."""

    def __init__(
        self,
        *,
        max_workers: int = 4,
        max_pending: int = 128,
        on_error: Callable[[TaskHandle[Any], BaseException], object] | None = None,
        observer: OperationStats | None = None,
    ) -> None:
        if on_error is not None and (not callable(on_error) or is_async_callable(on_error)):
            raise TypeError("on_error must be a synchronous callable")
        _require_operation_stats(observer)
        self._pool = BoundedWorkPool(max_workers=max_workers, max_pending=max_pending)
        self._changed = threading.Condition()
        self._worker = threading.local()
        self._tasks: dict[Future[Any], tuple[TaskHandle[Any], Shutdown]] = {}
        self._closed = False
        self._completed = self._failed = self._cancelled = 0
        self._on_error = on_error
        self._observer = observer

    @property
    def closed(self) -> bool:
        with self._changed:
            return self._closed

    def start(
        self, operation: Callable[[], T], *, name: str | None = None, shutdown: Shutdown = "cancel"
    ) -> TaskHandle[T]:
        if shutdown not in ("cancel", "drain"):
            raise ValueError("shutdown must be 'cancel' or 'drain'")
        if not callable(operation) or is_async_callable(operation):
            raise TypeError("operation must be a synchronous callable")
        task_name = "FlowCoordThreadTask" if name is None else name
        if not isinstance(task_name, str) or not task_name:
            raise ValueError("task name must be a non-empty string")
        observer = self._observer
        started = time.monotonic() if observer is not None else 0.0

        def invoke() -> T:
            self._worker.owned = True
            try:
                return synchronous_result(operation())
            finally:
                self._worker.owned = False

        with self._changed:
            if self._closed:
                raise RuntimeError("Cannot start a task in a closed scope")
            future = self._pool.submit(invoke)
            handle = TaskHandle(future, task_name, self._worker)
            self._tasks[future] = (handle, shutdown)
        future.add_done_callback(lambda complete: self._finished(complete, started))
        return handle

    def _finished(self, future: Future[Any], started: float) -> None:
        cancelled = future.cancelled()
        error = None if cancelled else future.exception()
        with self._changed:
            handle, _ = self._tasks[future]
            pending = len(self._tasks) - 1
        was_owned = getattr(self._worker, "owned", False)
        self._worker.owned = True
        try:
            if self._observer is not None:
                try:
                    self._observer.record(
                        "task_scope.task",
                        status="cancelled"
                        if cancelled
                        else "error"
                        if error is not None
                        else "success",
                        elapsed=time.monotonic() - started,
                        queue_depth=pending,
                    )
                except BaseException as observer_error:
                    logging.getLogger(__name__).error(
                        "Task metrics reporting failed", exc_info=observer_error
                    )
            if error is not None and self._on_error is not None:
                try:
                    synchronous_result(self._on_error(handle, error))
                except BaseException as observer_error:
                    combined = BaseExceptionGroup(
                        "Task and error observer both failed", [error, observer_error]
                    )
                    logging.getLogger(__name__).error(
                        "Task error reporting failed", exc_info=combined
                    )
        finally:
            self._worker.owned = was_owned
            with self._changed:
                del self._tasks[future]
                if cancelled:
                    self._cancelled += 1
                elif error is None:
                    self._completed += 1
                else:
                    self._failed += 1
                if not self._tasks:
                    self._changed.notify_all()

    def cancel(self, handle: TaskHandle[Any] | None = None) -> int:
        if handle is not None and not isinstance(handle, TaskHandle):
            raise TypeError("handle must be a TaskHandle")
        with self._changed:
            futures = (
                tuple(self._tasks)
                if handle is None
                else ((handle._future,) if handle._future in self._tasks else ())
            )
        return sum(future.cancel() for future in futures)

    def _require_non_worker(self) -> None:
        if getattr(self._worker, "owned", False):
            raise RuntimeError("A scope worker cannot wait for its own task scope")

    def join(self, timeout: float | None = None) -> None:
        self._require_non_worker()
        budget = validate_nonnegative_timeout(timeout, name="task scope timeout")
        deadline = None if budget is None else time.monotonic() + budget
        with self._changed:
            while self._tasks:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError("Task scope still owns unfinished work")
                self._changed.wait(
                    None if remaining is None else min(remaining, threading.TIMEOUT_MAX)
                )

    def close(self, timeout: float | None = None) -> None:
        self._require_non_worker()
        timeout = validate_nonnegative_timeout(timeout, name="task scope close timeout")
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._changed:
            self._closed = True
            cancel = tuple(future for future, (_, mode) in self._tasks.items() if mode == "cancel")
        for future in cancel:
            future.cancel()
        self._pool.shutdown(wait=False)
        self.join(None if deadline is None else max(0.0, deadline - time.monotonic()))

    def snapshot(self) -> TaskScopeSnapshot:
        with self._changed:
            return TaskScopeSnapshot(
                len(self._tasks), self._completed, self._failed, self._cancelled, self._closed
            )

    def __enter__(self) -> TaskScope:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


class LatestTaskCoordinator(Generic[K]):
    """Keep only the newest result eligible for application for each key."""

    def __init__(self, *, max_workers: int = 4, max_pending: int = 128) -> None:
        self._scope = TaskScope(max_workers=max_workers, max_pending=max_pending)
        self._lock = threading.Condition()
        self._versions: dict[K, int] = {}
        self._latest: dict[K, TaskHandle[None]] = {}
        self._counts: dict[K, int] = {}
        self._apply_locks: dict[K, LockType] = {}
        self._closed = False
        self._completed = self._discarded = 0

    def submit(
        self,
        key: K,
        factory: Callable[[], T],
        on_result: Callable[[T], object],
        *,
        cancel_previous: bool = True,
        on_error: Callable[[BaseException], object] | None = None,
        name: str | None = None,
    ) -> TaskHandle[None]:
        for fn in (factory, on_result) if on_error is None else (factory, on_result, on_error):
            if not callable(fn) or is_async_callable(fn):
                raise TypeError("Latest task callbacks must be synchronous callables")
        if not isinstance(cancel_previous, bool):
            raise TypeError("cancel_previous must be a bool")

        def run() -> None:
            try:
                result = synchronous_result(factory())
                # Application is serialized for this key; new work may still be submitted.
                with apply_lock:
                    with self._lock:
                        if self._closed or self._versions.get(key) != version:
                            self._discarded += 1
                            return
                    synchronous_result(on_result(result))
                    with self._lock:
                        self._completed += 1
            except Exception as error:
                if on_error is None:
                    raise
                try:
                    synchronous_result(on_error(error))
                except Exception as observer_error:
                    raise ExceptionGroup(
                        "Latest task and error observer both failed", [error, observer_error]
                    ) from error

        with self._lock:
            if self._closed:
                raise RuntimeError("Cannot submit work to a closed coordinator")
            version = self._versions.get(key, 0) + 1
            previous = self._latest.get(key)
            apply_lock = self._apply_locks.get(key) or threading.Lock()
            handle = self._scope.start(run, name=name, shutdown="cancel")
            self._versions[key] = version
            self._apply_locks[key] = apply_lock
            self._latest[key] = handle
            self._counts[key] = self._counts.get(key, 0) + 1

        handle._future.add_done_callback(lambda _: self._finished(key, handle))
        if previous is not None and cancel_previous:
            previous.cancel()
        return handle

    def _finished(self, key: K, handle: TaskHandle[None]) -> None:
        with self._lock:
            if self._latest.get(key) is handle:
                del self._latest[key]
            remaining = self._counts[key] - 1
            if remaining:
                self._counts[key] = remaining
            else:
                del self._counts[key]
                del self._apply_locks[key]
                del self._versions[key]
                if not self._counts:
                    self._lock.notify_all()

    def cancel(self, key: K) -> bool:
        with self._lock:
            handle = self._latest.get(key)
            if handle is None:
                return False
            self._versions[key] += 1
        handle.cancel()
        return True

    def close(self, timeout: float | None = None) -> None:
        self._scope._require_non_worker()
        timeout = validate_nonnegative_timeout(timeout, name="coordinator close timeout")
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._lock:
            self._closed = True
        self._scope.close(timeout)
        with self._lock:
            while self._counts:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError("Latest task cleanup is still running")
                self._lock.wait(
                    None if remaining is None else min(remaining, threading.TIMEOUT_MAX)
                )

    def snapshot(self) -> LatestTaskSnapshot:
        with self._lock:
            active: dict[Hashable, int] = {key: self._versions[key] for key in self._latest}
            return LatestTaskSnapshot(MappingProxyType(active), self._completed, self._discarded)
