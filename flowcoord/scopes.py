from __future__ import annotations

import threading
from collections.abc import Callable

from ._callbacks import is_async_callable, synchronous_result
from ._lifecycle import raise_cleanup_errors
from .base import _BaseResourceScope, _resolve_cleanup, _ResourceEntry


class ResourceScope(_BaseResourceScope):
    """Own sync resources and deferred cleanup in reverse registration order."""

    def __init__(self) -> None:
        super().__init__()
        self._lock = threading.RLock()
        self._close_lock = threading.RLock()
        self._closing_thread: int | None = None

    @property
    def closed(self) -> bool:
        with self._lock:
            return self._closed

    def add(
        self,
        resource: object,
        *,
        close: str | Callable[[object], object] = "close",
        owned: bool = True,
    ) -> object:
        """Register a resource; borrowed resources are tracked but never closed."""
        if not isinstance(owned, bool):
            raise TypeError("owned must be a bool")
        cleanup = _resolve_cleanup(resource, close) if owned else None
        if cleanup is not None and is_async_callable(cleanup):
            raise TypeError("ResourceScope requires synchronous cleanup")
        with self._lock:
            return self._add(resource, cleanup)

    def defer(self, cleanup: Callable[[], object]) -> Callable[[], object]:
        """Register a zero-argument cleanup function."""
        if not callable(cleanup) or is_async_callable(cleanup):
            raise TypeError("ResourceScope.defer requires a synchronous callable")
        with self._lock:
            return self._defer(cleanup)

    def detach(self, resource: object) -> object:
        """Remove ownership and return the resource without running its cleanup."""
        with self._lock:
            return self._detach(resource)

    def pending_cleanup(self) -> int:
        with self._lock:
            return self._pending_cleanup()

    def close(self) -> None:
        """Attempt all cleanups; failed entries remain available for a later retry."""
        with self._close_lock:
            with self._lock:
                current_thread = threading.get_ident()
                if self._closing_thread == current_thread:
                    return
                self._closing_thread = current_thread
                self._closed = True
                pending = self._take_pending()
            failed: list[_ResourceEntry] = []
            errors: list[BaseException] = []
            try:
                for entry in pending:
                    if entry.cleanup is None:
                        continue
                    try:
                        synchronous_result(entry.cleanup())
                    except BaseException as error:
                        failed.append(entry)
                        errors.append(error)
                    finally:
                        with self._lock:
                            self._inflight -= 1
                with self._lock:
                    self._retain_failures(failed)
                raise_cleanup_errors(errors, message="Resource scope cleanup failed")
            finally:
                with self._lock:
                    self._closing_thread = None

    def __enter__(self) -> ResourceScope:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()
