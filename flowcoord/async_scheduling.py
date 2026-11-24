"""Keyed coalescing and loop-owned async scheduling."""

from __future__ import annotations

import asyncio
import inspect
import math
import time
from collections.abc import Callable
from typing import Any, Literal

from ._async_lifecycle import (
    bind_running_loop,
    create_task_safely,
    finish_cleanup,
    request_task_cancellation,
)
from ._callbacks import close_awaitable
from ._compatibility import BaseExceptionGroup
from ._validation import validate_nonnegative_timeout, validate_positive_capacity
from .base import SchedulerStats, _periodic_parameters
from .observability import OperationStats, _require_operation_stats


class AsyncScheduledHandle:
    """Cancellation handle for one scheduled job; cancellation is cooperative."""

    def __init__(self, task: asyncio.Task[Any], loop: asyncio.AbstractEventLoop) -> None:
        self._task = task
        self._loop = loop

    @property
    def name(self) -> str:
        return self._task.get_name()

    @property
    def done(self) -> bool:
        return self._task.done()

    def cancel(self) -> bool:
        return request_task_cancellation(self._task, self._loop)

    async def join(self, timeout: float | None = None) -> Any:
        timeout = validate_nonnegative_timeout(timeout, name="scheduled task timeout")
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("AsyncScheduledHandle belongs to a different event loop")
        done, _ = await asyncio.wait((self._task,), timeout=timeout)
        if not done:
            raise TimeoutError("Scheduled task did not finish before the timeout")
        return self._task.result()


class AsyncScheduler:
    """Schedule one-shot and periodic async or synchronous callables on one loop.

    max_jobs limits registered jobs. Parallel ticks are limited per periodic
    job by max_parallel and do not consume registration slots.
    """

    def __init__(
        self,
        *,
        max_jobs: int = 4096,
        on_error: Callable[[BaseException], object] | None = None,
        observer: OperationStats | None = None,
    ) -> None:
        if on_error is not None and not callable(on_error):
            raise TypeError("on_error must be callable")
        _require_operation_stats(observer)
        self._capacity = validate_positive_capacity(max_jobs, name="max_jobs")
        self._job_count = 0
        self._on_error = on_error
        self._observer = observer
        self._loop: asyncio.AbstractEventLoop | None = None
        self._tasks: set[asyncio.Task[Any]] = set()
        self._closed = False
        self._closing: asyncio.Task[None] | None = None
        self._completed = self._failed = self._cancelled = 0

    def _bind_loop(self) -> asyncio.AbstractEventLoop:
        loop = bind_running_loop(self._loop, "AsyncScheduler")
        self._loop = loop
        return loop

    def _track(self, coroutine: Any, name: str) -> AsyncScheduledHandle:
        try:
            loop = self._bind_loop()
            if self._closed:
                raise RuntimeError("AsyncScheduler is closed")
            if self._job_count >= self._capacity:
                raise RuntimeError("AsyncScheduler capacity is full")
        except BaseException:
            coroutine.close()
            raise
        self._job_count += 1
        try:
            task = create_task_safely(coroutine, name=name, loop=loop)
        except BaseException:
            self._job_count -= 1
            raise
        self._tasks.add(task)
        task.add_done_callback(self._job_finished)
        return AsyncScheduledHandle(task, loop)

    def _job_finished(self, task: asyncio.Task[Any]) -> None:
        self._job_count -= 1
        self._finished(task)

    def _finished(self, task: asyncio.Task[Any]) -> None:
        self._tasks.discard(task)
        if task.cancelled():
            self._cancelled += 1
            return
        error = task.exception()
        if error is None:
            self._completed += 1
            return
        self._failed += 1
        if self._on_error is not None:
            try:
                result = self._on_error(error)
                if inspect.isawaitable(result):
                    close_awaitable(result)
                    raise TypeError("AsyncScheduler on_error must be synchronous")
                return
            except BaseException as observer_error:
                error = BaseExceptionGroup(
                    "Scheduled job and error observer both failed", [error, observer_error]
                )
        loop = self._loop
        if loop is not None:
            loop.call_exception_handler(
                {
                    "message": "Unhandled failure in FlowCoord scheduled job",
                    "exception": error,
                    "task": task,
                }
            )

    @staticmethod
    async def _invoke(
        callback: Callable[..., object], args: tuple[object, ...], kwargs: dict[str, object]
    ) -> object:
        result = callback(*args, **kwargs)
        return await result if inspect.isawaitable(result) else result

    def schedule_once(
        self,
        callback: Callable[..., object],
        *args: object,
        delay: float = 0.0,
        name: str | None = None,
        **kwargs: object,
    ) -> AsyncScheduledHandle:
        if not callable(callback):
            raise TypeError("callback must be callable")
        wait = validate_nonnegative_timeout(delay, name="schedule delay")

        async def run() -> object:
            if wait:
                await asyncio.sleep(wait)
            started = time.monotonic()
            try:
                result = await self._invoke(callback, args, kwargs)
            except BaseException as error:
                if self._observer is not None:
                    self._observer.record(
                        "scheduler.once",
                        status="cancelled"
                        if isinstance(error, asyncio.CancelledError)
                        else "error",
                        elapsed=time.monotonic() - started,
                    )
                raise
            if self._observer is not None:
                self._observer.record("scheduler.once", elapsed=time.monotonic() - started)
            return result

        return self._track(run(), name or "FlowCoordScheduledOnce")

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
    ) -> AsyncScheduledHandle:
        if not callable(callback):
            raise TypeError("callback must be callable")
        period, first, parallel_limit = _periodic_parameters(
            interval, initial_delay, max_parallel, overlap
        )
        loop = self._bind_loop()

        async def run_tick() -> None:
            started = time.monotonic()
            try:
                await self._invoke(callback, args, kwargs)
            except BaseException as error:
                if self._observer is not None:
                    self._observer.record(
                        "scheduler.periodic",
                        status="cancelled"
                        if isinstance(error, asyncio.CancelledError)
                        else "error",
                        elapsed=time.monotonic() - started,
                    )
                raise
            if self._observer is not None:
                self._observer.record("scheduler.periodic", elapsed=time.monotonic() - started)

        async def run() -> None:
            next_tick = loop.time() + first
            children: set[asyncio.Task[None]] = set()
            queue_one = False
            try:
                while True:
                    if self._closed:
                        return
                    await asyncio.sleep(max(0.0, next_tick - loop.time()))
                    if self._closed:
                        return
                    if overlap == "parallel":
                        if len(children) < parallel_limit:
                            child = create_task_safely(
                                run_tick(), name=f"{name or 'FlowCoordPeriodic'}:tick", loop=loop
                            )
                            children.add(child)
                            child.add_done_callback(children.discard)
                            self._tasks.add(child)
                            child.add_done_callback(self._finished)
                        elif self._observer is not None:
                            self._observer.record("scheduler.periodic", status="error", dropped=1)
                    else:
                        await run_tick()
                    now = loop.time()
                    next_tick += period
                    if next_tick <= now:
                        if overlap == "skip":
                            missed = math.floor((now - next_tick) / period) + 1
                            next_tick += missed * period
                        elif overlap == "queue" and not queue_one:
                            next_tick = now
                            queue_one = True
                        else:
                            next_tick = now + period
                            queue_one = False
            finally:
                if children:
                    for child in children:
                        child.cancel()
                    await asyncio.gather(*children, return_exceptions=True)

        return self._track(run(), name or "FlowCoordPeriodic")

    def cancel(self, handle: AsyncScheduledHandle) -> bool:
        if not isinstance(handle, AsyncScheduledHandle):
            raise TypeError("handle must be a AsyncScheduledHandle")
        if handle._task not in self._tasks:
            return False
        return handle.cancel()

    async def _close_tasks(self, timeout: float | None) -> None:
        tasks = tuple(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=timeout)
            if pending:
                raise TimeoutError(f"{len(pending)} scheduled task(s) remain after shutdown")

    async def aclose(self, timeout: float | None = None) -> None:
        self._bind_loop()
        current = asyncio.current_task()
        if current is self._closing or current in self._tasks:
            raise RuntimeError("A scheduled job cannot close its own scheduler")
        timeout = validate_nonnegative_timeout(timeout, name="scheduler close timeout")
        self._closed = True
        if self._closing is None or (
            self._closing.done()
            and (self._tasks or self._closing.cancelled() or self._closing.exception() is not None)
        ):
            self._closing = create_task_safely(
                self._close_tasks(timeout), name="FlowCoordSchedulerClose"
            )
        await finish_cleanup(self._closing)

    def snapshot(self) -> SchedulerStats:
        self._bind_loop()
        return SchedulerStats(
            len(self._tasks), self._completed, self._failed, self._cancelled, self._closed
        )

    async def __aenter__(self) -> AsyncScheduler:
        self._bind_loop()
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()
