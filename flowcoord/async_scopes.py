from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable

from ._async_lifecycle import bind_running_loop, create_task_safely, finish_cleanup_errors
from ._callbacks import synchronous_result
from ._lifecycle import raise_cleanup_errors
from .base import _BaseResourceScope, _resolve_cleanup, _ResourceEntry


class AsyncResourceScope(_BaseResourceScope):
    """Own sync or async resources and finish cleanup despite caller cancellation."""

    def __init__(self) -> None:
        super().__init__()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._cleanup: asyncio.Task[list[BaseException]] | None = None

    def _bind_loop(self) -> None:
        self._loop = bind_running_loop(self._loop, "AsyncResourceScope")

    @property
    def closed(self) -> bool:
        return self._closed

    def add(
        self,
        resource: object,
        *,
        close: str | Callable[[object], object] = "aclose",
        owned: bool = True,
    ) -> object:
        """Register a resource; cleanup may be sync or async and remains sequential."""
        if not isinstance(owned, bool):
            raise TypeError("owned must be a bool")
        cleanup = _resolve_cleanup(resource, close) if owned else None
        if self._loop is not None:
            self._bind_loop()
        return self._add(resource, cleanup)

    def defer(self, cleanup: Callable[[], object]) -> Callable[[], object]:
        """Register a zero-argument sync or async cleanup function."""
        if not callable(cleanup):
            raise TypeError("cleanup must be callable")
        if self._loop is not None:
            self._bind_loop()
        return self._defer(cleanup)

    def detach(self, resource: object) -> object:
        if self._loop is not None:
            self._bind_loop()
        return self._detach(resource)

    def pending_cleanup(self) -> int:
        return self._pending_cleanup()

    async def _run_cleanups(self) -> list[BaseException]:
        pending = self._take_pending()
        failed: list[_ResourceEntry] = []
        errors: list[BaseException] = []
        for entry in pending:
            if entry.cleanup is None:
                continue
            try:
                result = entry.cleanup()
                if inspect.isawaitable(result):
                    await result
                else:
                    synchronous_result(result)
            except BaseException as error:
                failed.append(entry)
                errors.append(error)
            finally:
                self._inflight -= 1
        self._retain_failures(failed)
        return errors

    async def aclose(self) -> None:
        self._bind_loop()
        if asyncio.current_task() is self._cleanup:
            raise RuntimeError("A resource cleanup cannot wait for its own scope")
        self._closed = True
        cleanup = self._cleanup
        if cleanup is None or (cleanup.done() and self._entries):
            cleanup = create_task_safely(self._run_cleanups(), name="FlowCoordResourceCleanup")
            self._cleanup = cleanup
        errors = await finish_cleanup_errors(cleanup, message="Resource scope cleanup failed")
        raise_cleanup_errors(errors, message="Resource scope cleanup failed")

    async def __aenter__(self) -> AsyncResourceScope:
        self._bind_loop()
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()
