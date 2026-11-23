"""Run asynchronous operations on a dedicated event-loop thread."""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
import time
from collections.abc import Awaitable, Callable
from typing import Any, Generic, TypeVar, cast

from ._callbacks import close_awaitable, is_awaitable
from ._validation import validate_nonnegative_timeout, validate_positive_capacity
from .errors import AsyncLoopPortalClosedError

T = TypeVar("T")
__all__ = ["AsyncLoopPortal"]


class _PortalSubmission(Generic[T]):
    __slots__ = (
        "portal",
        "loop",
        "operation",
        "args",
        "kwargs",
        "future",
        "task",
        "started",
    )

    def __init__(
        self,
        portal: AsyncLoopPortal,
        loop: asyncio.AbstractEventLoop,
        operation: Callable[..., Awaitable[T]] | Awaitable[T],
        args: tuple[object, ...],
        kwargs: dict[str, object],
        future: concurrent.futures.Future[T],
    ) -> None:
        self.portal = portal
        self.loop = loop
        self.operation = operation
        self.args = args
        self.kwargs = kwargs
        self.future = future
        self.task: asyncio.Task[T] | None = None
        self.started = False

    def close_input(self) -> None:
        if not callable(self.operation):
            close_awaitable(self.operation)

    def release(self) -> None:
        portal = self.portal
        with portal._lock:
            portal._futures.discard(self.future)

    def on_future_done(self, completed: concurrent.futures.Future[T]) -> None:
        if completed.cancelled():
            try:
                self.loop.call_soon_threadsafe(self.cancel_on_loop)
            except RuntimeError:
                pass

    def cancel_on_loop(self) -> None:
        task = self.task
        if task is not None and not task.done():
            task.cancel()

    async def _run(self) -> T:
        self.started = True
        operation = self.operation
        result = operation(*self.args, **self.kwargs) if callable(operation) else operation
        if not is_awaitable(result):
            raise TypeError("portal operation must return an awaitable")
        return await cast(Awaitable[T], result)

    def on_task_done(self, completed: asyncio.Task[T]) -> None:
        if not self.started:
            self.close_input()
        self.release()
        future = self.future
        if completed.cancelled():
            future.cancel()
            return
        error = completed.exception()
        try:
            if error is None:
                future.set_result(completed.result())
            else:
                future.set_exception(error)
        except concurrent.futures.InvalidStateError:
            # A caller may cancel the future while the task is completing.
            pass

    def schedule(self) -> None:
        future = self.future
        if future.cancelled():
            self.close_input()
            self.release()
            return
        coroutine = self._run()
        try:
            task = self.loop.create_task(coroutine, name="FlowCoordPortalCall")
        except BaseException as error:
            coroutine.close()
            self.close_input()
            self.release()
            try:
                future.set_exception(error)
            except concurrent.futures.InvalidStateError:
                pass
            return
        self.task = task
        if future.cancelled():
            task.cancel()
        task.add_done_callback(self.on_task_done)


class AsyncLoopPortal:
    """Own a background event loop and submit async operations from other threads."""

    def __init__(self, *, name: str = "FlowCoordLoopPortal", max_pending: int = 128) -> None:
        if not isinstance(name, str) or not name:
            raise ValueError("portal thread name must be a non-empty string")
        self._name = name
        self._capacity = validate_positive_capacity(max_pending, name="max_pending")
        self._lock = threading.Lock()
        self._close_lock = threading.Lock()
        self._ready = threading.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._startup_error: BaseException | None = None
        self._futures: set[concurrent.futures.Future[Any]] = set()
        self._closing = False
        self._stopping = False
        self._closed = False

    @property
    def running(self) -> bool:
        with self._lock:
            return (
                self._loop is not None
                and self._loop.is_running()
                and not self._closing
                and not self._stopping
            )

    def start(self) -> AsyncLoopPortal:
        """Start the loop thread and wait until its event loop is running."""
        with self._lock:
            if self._closed:
                raise AsyncLoopPortalClosedError("AsyncLoopPortal cannot be restarted after close")
            if self._closing or self._stopping:
                raise AsyncLoopPortalClosedError("AsyncLoopPortal is closing")
            if self._thread is None:
                self._ready.clear()
                self._startup_error = None
                thread = threading.Thread(target=self._run, name=self._name, daemon=True)
                self._thread = thread
                try:
                    thread.start()
                except BaseException as error:
                    self._thread = None
                    self._startup_error = error
                    self._ready.set()
                    raise
        self._ready.wait()
        with self._lock:
            if self._startup_error is not None:
                raise AsyncLoopPortalClosedError(
                    "AsyncLoopPortal event loop failed to start"
                ) from (self._startup_error)
            if self._closing or self._stopping or self._closed:
                raise AsyncLoopPortalClosedError("AsyncLoopPortal is closing")
        return self

    def _run(self) -> None:
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        except BaseException as error:
            with self._lock:
                self._startup_error = error
                self._ready.set()
            return

        def mark_started() -> None:
            with self._lock:
                self._loop = loop
                self._ready.set()

        try:
            loop.call_soon(mark_started)
            loop.run_forever()
        except BaseException as error:
            with self._lock:
                if not self._ready.is_set():
                    self._startup_error = error
                    self._ready.set()
            raise
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            finally:
                try:
                    loop.run_until_complete(loop.shutdown_default_executor())
                finally:
                    asyncio.set_event_loop(None)
                    loop.close()

    def submit(
        self,
        operation: Callable[..., Awaitable[T]] | Awaitable[T],
        *args: object,
        **kwargs: object,
    ) -> concurrent.futures.Future[T]:
        """Submit an awaitable or async callable; caller ContextVars propagate."""
        try:
            with self._lock:
                loop = self._loop
                if (
                    self._thread is None
                    or loop is None
                    or not loop.is_running()
                    or self._closing
                    or self._stopping
                ):
                    raise AsyncLoopPortalClosedError("AsyncLoopPortal is not accepting work")
                if len(self._futures) >= self._capacity:
                    raise RuntimeError("AsyncLoopPortal capacity is full")
                future: concurrent.futures.Future[T] = concurrent.futures.Future()
                submission = _PortalSubmission(self, loop, operation, args, kwargs, future)
                future.add_done_callback(submission.on_future_done)
                self._futures.add(future)
                try:
                    loop.call_soon_threadsafe(submission.schedule)
                except RuntimeError as error:
                    self._futures.discard(future)
                    raise AsyncLoopPortalClosedError(
                        "AsyncLoopPortal event loop is not accepting work"
                    ) from error
        except BaseException:
            if not callable(operation):
                close_awaitable(operation)
            raise
        return future

    def call(
        self,
        operation: Callable[..., Awaitable[T]] | Awaitable[T],
        *args: object,
        timeout: float | None = None,
        **kwargs: object,
    ) -> T:
        """Block for a result; calls from the portal loop thread are rejected."""
        if self._thread is threading.current_thread():
            if not callable(operation):
                close_awaitable(operation)
            raise RuntimeError("AsyncLoopPortal.call cannot block its own event-loop thread")
        timeout = validate_nonnegative_timeout(timeout, name="portal call timeout")
        future = self.submit(operation, *args, **kwargs)
        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError as error:
            if future.done() and not future.cancelled() and future.exception() is error:
                raise
            future.cancel()
            raise TimeoutError(
                "AsyncLoopPortal call exceeded its timeout; cancellation is cooperative"
            ) from error

    def cancel(self, future: concurrent.futures.Future[Any]) -> bool:
        if not isinstance(future, concurrent.futures.Future):
            raise TypeError("future must be a concurrent.futures.Future")
        with self._lock:
            if future not in self._futures or future.done():
                return False
        return future.cancel()

    def _restore_close_state(self) -> None:
        with self._lock:
            self._closing = False

    def _join_thread(
        self,
        thread: threading.Thread,
        end: float | None,
        *,
        timeout_message: str,
    ) -> None:
        remaining = None if end is None else max(0.0, end - time.monotonic())
        thread.join(remaining)
        if thread.is_alive():
            self._restore_close_state()
            raise TimeoutError(timeout_message)

    def close(self, timeout: float | None = None) -> None:
        """Cancel portal tasks and stop only after they have actually finished."""
        if self._thread is threading.current_thread():
            raise RuntimeError("AsyncLoopPortal.close cannot block its own event-loop thread")
        timeout = validate_nonnegative_timeout(timeout, name="portal close timeout")
        end = None if timeout is None else time.monotonic() + timeout
        with self._close_lock:
            with self._lock:
                if self._closed:
                    return
                thread = self._thread
                if thread is None:
                    self._closed = True
                    return
                self._closing = True
                starting = self._loop is None

            if starting:
                remaining = None if end is None else max(0.0, end - time.monotonic())
                if not self._ready.wait(remaining):
                    self._restore_close_state()
                    raise TimeoutError("AsyncLoopPortal event loop has not started")

            with self._lock:
                loop = self._loop
                startup_error = self._startup_error
                stopping = self._stopping

            if loop is None:
                self._join_thread(
                    thread,
                    end,
                    timeout_message="AsyncLoopPortal startup thread has not exited",
                )
                with self._lock:
                    self._closed = True
                    self._closing = False
                    self._thread = None
                if startup_error is not None:
                    raise AsyncLoopPortalClosedError(
                        "AsyncLoopPortal event loop failed to start"
                    ) from (startup_error)
                return

            if stopping or not loop.is_running():
                self._join_thread(
                    thread,
                    end,
                    timeout_message="AsyncLoopPortal thread is stopping but has not exited",
                )
                with self._lock:
                    self._closed = True
                    self._closing = False
                    self._loop = None
                    self._thread = None
                return

            async def cancel_owned_tasks() -> None:
                current = asyncio.current_task()
                tasks = tuple(task for task in asyncio.all_tasks(loop) if task is not current)
                for task in tasks:
                    task.cancel()
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

            while True:
                shutdown = asyncio.run_coroutine_threadsafe(cancel_owned_tasks(), loop)
                try:
                    remaining = None if end is None else max(0.0, end - time.monotonic())
                    shutdown.result(timeout=remaining)
                except concurrent.futures.CancelledError as error:
                    remaining = None if end is None else end - time.monotonic()
                    if remaining is None or remaining > 0:
                        continue
                    self._restore_close_state()
                    raise TimeoutError(
                        "AsyncLoopPortal shutdown was cancelled; loop ownership was retained"
                    ) from error
                except concurrent.futures.TimeoutError as error:
                    shutdown.cancel()
                    self._restore_close_state()
                    raise TimeoutError(
                        "AsyncLoopPortal tasks remain active; loop ownership was retained"
                    ) from error
                break
            with self._lock:
                self._stopping = True
            loop.call_soon_threadsafe(loop.stop)
            self._join_thread(
                thread,
                end,
                timeout_message="AsyncLoopPortal thread did not stop before the timeout",
            )
            with self._lock:
                self._closed = True
                self._closing = False
                self._loop = None
                self._thread = None

    def __enter__(self) -> AsyncLoopPortal:
        return self.start()

    def __exit__(self, *_args: object) -> None:
        self.close()
