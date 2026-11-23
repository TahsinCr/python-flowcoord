"""Thread-to-async bridge behavior and ownership tests."""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import threading
import unittest
from unittest.mock import patch

from flowcoord.async_portal import AsyncLoopPortal
from flowcoord.errors import AsyncLoopPortalClosedError


class ThreadBridgeTests(unittest.TestCase):
    def test_cancelled_future_keeps_capacity_until_portal_task_finishes(self):
        started, cancelled, finished = (threading.Event() for _ in range(3))
        release = asyncio.Event()

        async def operation():
            task = asyncio.current_task()
            assert task is not None
            task.add_done_callback(lambda _: finished.set())
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                await release.wait()
                raise ValueError("late task failure") from None

        with AsyncLoopPortal(max_pending=1) as portal:
            future = portal.submit(operation)
            try:
                self.assertTrue(started.wait(3))
                self.assertTrue(future.cancel())
                self.assertTrue(cancelled.wait(3))
                rejected = asyncio.sleep(0)
                with self.assertRaisesRegex(RuntimeError, "capacity is full"):
                    portal.submit(rejected)
                self.assertIsNone(rejected.cr_frame)
            finally:
                loop = portal._loop
                assert loop is not None
                loop.call_soon_threadsafe(release.set)
            self.assertTrue(finished.wait(3))
            self.assertEqual(portal.call(asyncio.sleep, 0, result=42, timeout=3), 42)

    def test_loop_portal_recovers_after_thread_start_failure(self):
        portal = AsyncLoopPortal()

        with patch("threading.Thread.start", side_effect=RuntimeError("thread start failed")):
            with self.assertRaisesRegex(RuntimeError, "thread start failed"):
                portal.start()

        self.assertIs(portal.start(), portal)
        portal.close()

    def test_loop_portal_releases_thread_after_loop_factory_failure(self):
        portal = AsyncLoopPortal(name="flowcoord-startup-failure")

        with patch("asyncio.new_event_loop", side_effect=RuntimeError("loop creation failed")):
            with self.assertRaises(AsyncLoopPortalClosedError) as start_error:
                portal.start()
            self.assertIsInstance(start_error.exception.__cause__, RuntimeError)

            thread = portal._thread
            assert thread is not None
            with self.assertRaises(AsyncLoopPortalClosedError) as close_error:
                portal.close()

        self.assertIsInstance(close_error.exception.__cause__, RuntimeError)
        self.assertFalse(thread.is_alive())
        self.assertIsNone(portal._thread)
        self.assertIsNone(portal._loop)
        portal.close()

    def test_loop_portal_call_context_and_owned_task_shutdown(self):
        value = contextvars.ContextVar("portal-test", default="missing")
        portal = AsyncLoopPortal()
        portal.start()
        value.set("caller-context")

        async def read_context():
            return value.get()

        self.assertEqual(portal.call(read_context, timeout=1), "caller-context")

        async def blocking_call_from_loop():
            return portal.call(asyncio.sleep(0))

        with self.assertRaises(RuntimeError):
            portal.submit(blocking_call_from_loop).result(1)
        started = threading.Event()
        cancelled = threading.Event()

        async def background():
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        portal.submit(background)
        self.assertTrue(started.wait(1))
        portal.close(timeout=1)
        self.assertTrue(cancelled.wait(1))
        self.assertFalse(portal.running)
        with self.assertRaises(RuntimeError):
            portal.start()

    def test_loop_portal_preserves_operation_timeout_errors(self):
        operation_error = TimeoutError("operation timed out")

        async def operation():
            raise operation_error

        with AsyncLoopPortal() as portal:
            for timeout in (None, 1.0):
                with self.subTest(timeout=timeout):
                    with self.assertRaises(TimeoutError) as raised:
                        portal.call(operation, timeout=timeout)
                    self.assertIs(raised.exception, operation_error)

    def test_loop_portal_rejects_work_after_stop_is_queued(self):
        portal = AsyncLoopPortal().start()
        loop = portal._loop
        self.assertIsNotNone(loop)
        assert loop is not None
        original_call_soon_threadsafe = loop.call_soon_threadsafe
        loop_blocked = threading.Event()
        release_loop = threading.Event()

        def block_loop():
            loop_blocked.set()
            release_loop.wait()

        def intercept_stop(callback, *args, **kwargs):
            if (
                getattr(callback, "__self__", None) is loop
                and getattr(callback, "__name__", None) == "stop"
            ):
                original_call_soon_threadsafe(block_loop)
            return original_call_soon_threadsafe(callback, *args, **kwargs)

        portal_thread = portal._thread
        self.assertIsNotNone(portal_thread)
        assert portal_thread is not None
        original_join = portal_thread.join

        def return_before_thread_exits(timeout=None):
            self.assertTrue(loop_blocked.wait(1))

        loop.call_soon_threadsafe = intercept_stop
        portal_thread.join = return_before_thread_exits
        try:
            with self.assertRaises(TimeoutError):
                portal.close(timeout=2)
            self.assertFalse(portal.running)
            with self.assertRaises(AsyncLoopPortalClosedError):
                portal.start()
            coroutine = asyncio.sleep(0)
            with self.assertRaises(AsyncLoopPortalClosedError):
                portal.submit(coroutine)
            self.assertIsNone(coroutine.cr_frame)
        finally:
            release_loop.set()
            loop.call_soon_threadsafe = original_call_soon_threadsafe
            portal_thread.join = original_join
            portal.close(timeout=1)

    def test_loop_portal_serializes_concurrent_start_and_close(self):
        portal = AsyncLoopPortal()
        barrier = threading.Barrier(3)
        errors = []

        def start_portal():
            barrier.wait()
            try:
                portal.start()
            except BaseException as error:
                errors.append(error)

        starters = [threading.Thread(target=start_portal) for _ in range(2)]
        for thread in starters:
            thread.start()
        barrier.wait()
        for thread in starters:
            thread.join(1)
        self.assertFalse(any(thread.is_alive() for thread in starters))
        self.assertEqual(errors, [])
        self.assertTrue(portal.running)

        closers = [threading.Thread(target=portal.close) for _ in range(2)]
        for thread in closers:
            thread.start()
        for thread in closers:
            thread.join(1)
        self.assertFalse(any(thread.is_alive() for thread in closers))
        self.assertFalse(portal.running)

    def test_loop_portal_closes_an_awaitable_cancelled_before_start(self):
        portal = AsyncLoopPortal()
        portal.start()
        loop_blocked = threading.Event()
        release_loop = threading.Event()

        async def operation():
            return 1

        async def hold_loop():
            loop_blocked.set()
            release_loop.wait()

        coroutine = operation()
        blocker = portal.submit(hold_loop())
        try:
            self.assertTrue(loop_blocked.wait(1))
            future = portal.submit(coroutine)
            self.assertTrue(future.cancel())
        finally:
            release_loop.set()
            portal.call(asyncio.sleep, 0)
            blocker.result(timeout=1)
            portal.close(timeout=1)
        self.assertIsNone(coroutine.cr_frame)

    def test_loop_portal_shutdown_waits_for_default_executor_threads(self):
        portal = AsyncLoopPortal().start()
        worker_started = threading.Event()
        release_worker = threading.Event()

        def blocking_work():
            worker_started.set()
            release_worker.wait(5)

        async def operation():
            await asyncio.to_thread(blocking_work)

        portal.submit(operation())
        try:
            self.assertTrue(worker_started.wait(1))
            with self.assertRaises(TimeoutError):
                portal.close(timeout=1)
            self.assertFalse(portal.running)
        finally:
            release_worker.set()
            portal.close(timeout=2)


class AsyncPortalTests(unittest.IsolatedAsyncioTestCase):
    async def test_portal_cancellation_api(self):
        with AsyncLoopPortal() as portal:
            future = portal.submit(asyncio.sleep, 60)
            self.assertTrue(portal.cancel(future))
            with self.assertRaises(concurrent.futures.CancelledError):
                future.result(1)
            self.assertFalse(portal.cancel(future))
            with self.assertRaises(TypeError):
                portal.cancel(object())

    async def test_portal_timeout_invalid_submission_and_post_close_cleanup(self):
        portal = AsyncLoopPortal()
        rejected_coroutine = asyncio.sleep(0)
        with self.assertRaises(AsyncLoopPortalClosedError):
            portal.submit(rejected_coroutine)
        self.assertIsNone(rejected_coroutine.cr_frame)

        portal.start()
        started = threading.Event()
        cancelled = threading.Event()

        async def slow():
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

        with self.assertRaisesRegex(TimeoutError, "AsyncLoopPortal call exceeded its timeout"):
            portal.call(slow, timeout=0.01)
        self.assertTrue(started.wait(1))
        self.assertTrue(cancelled.wait(1))
        with self.assertRaises((TypeError, ValueError)):
            portal.call(lambda: asyncio.sleep(0), timeout=True)
        with self.assertRaises(TypeError):
            portal.submit(lambda: 42).result(1)
        portal.close(timeout=1)
        closed_coroutine = asyncio.sleep(0)
        with self.assertRaises(AsyncLoopPortalClosedError):
            portal.submit(closed_coroutine)
        self.assertIsNone(closed_coroutine.cr_frame)

    async def test_portal_retains_loop_after_close_timeout_and_can_retry(self):
        portal = AsyncLoopPortal().start()
        started = threading.Event()
        cancellation_seen = threading.Event()

        async def slow_shutdown():
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancellation_seen.set()
                await asyncio.sleep(0.02)

        portal.submit(slow_shutdown)
        self.assertTrue(started.wait(1))
        with self.assertRaises(TimeoutError):
            portal.close(timeout=0)
        self.assertTrue(portal.running)
        portal.close(timeout=1)
        self.assertTrue(cancellation_seen.wait(1))
        self.assertFalse(portal.running)
