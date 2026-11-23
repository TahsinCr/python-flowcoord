"""Edge cases for registry, policy, and work-pool primitives."""

from __future__ import annotations

import threading
import unittest

from flowcoord.executors import BoundedWorkPool
from flowcoord.observability import OperationStats


class WorkPoolTests(unittest.TestCase):
    def test_bounded_pool_rejection_cancellation_and_worker_shutdown_guard(self):
        with self.assertRaises(ValueError):
            BoundedWorkPool(max_workers=0, max_pending=0)
        with self.assertRaises(TypeError):
            BoundedWorkPool(max_workers=1, max_pending=True)
        pool = BoundedWorkPool(max_workers=1, max_pending=1)
        with self.assertRaises(TypeError):
            pool.submit(object())

        async def async_callable():
            return 1

        with self.assertRaises(TypeError):
            pool.submit(async_callable)
        blocker = threading.Event()
        started = threading.Event()

        def wait():
            started.set()
            blocker.wait(1)

        running = pool.submit(wait)
        self.assertTrue(started.wait(1))
        pending = pool.submit(lambda: 2)
        self.assertTrue(pending.cancel())
        blocker.set()
        running.result(1)
        self.assertTrue(pending.cancelled())
        pool.shutdown()

    def test_bounded_pool_rejection_observer_runs_outside_lock_and_keeps_error(self):

        pool_ref = {}
        inspected = threading.Event()
        observer_threads = []
        observer = OperationStats()
        original_record = observer.record

        def record(operation, **metrics):
            if operation != "work_pool.submit":
                return original_record(operation, **metrics)

            def inspect_pool():
                pool_ref["pool"].stats()
                inspected.set()

            thread = threading.Thread(target=inspect_pool, daemon=True)
            observer_threads.append(thread)
            thread.start()
            if not inspected.wait(timeout=0.5):
                raise ValueError("reentrant stats call was blocked")
            raise LookupError("observer failed")

        observer.record = record  # type: ignore[method-assign]
        pool = BoundedWorkPool(max_workers=1, max_pending=0, observer=observer)
        pool_ref["pool"] = pool
        release, started = threading.Event(), threading.Event()

        def blocking_work():
            started.set()
            release.wait(timeout=2)

        try:
            pool.submit(blocking_work)
            self.assertTrue(started.wait(timeout=1))
            with self.assertRaisesRegex(RuntimeError, "capacity is full") as caught:
                pool.submit(lambda: None)
            self.assertIsInstance(caught.exception.__cause__, LookupError)
            self.assertTrue(inspected.is_set())
            for thread in observer_threads:
                thread.join(timeout=1)
                self.assertFalse(thread.is_alive())
        finally:
            release.set()
            pool.shutdown()

    def test_work_pool_cleans_awaitables_and_rejects_worker_self_wait(self):
        pool = BoundedWorkPool(max_workers=1, max_pending=0)

        async def result():
            return 1

        coroutine = result()
        future = pool.submit(lambda: coroutine)
        with self.assertRaises(TypeError):
            future.result(1)
        self.assertIsNone(coroutine.cr_frame)

        self_wait = pool.submit(pool.shutdown)
        with self.assertRaises(RuntimeError):
            self_wait.result(1)
        self.assertFalse(pool.stats().closed)
        pool.shutdown()
        pool.shutdown()
        self.assertTrue(pool.stats().closed)
        with self.assertRaises(RuntimeError):
            pool.submit(lambda: 1)

    def test_work_pool_done_callback_rejects_waiting_for_its_own_shutdown(self):
        pool = BoundedWorkPool(max_workers=1, max_pending=0)
        started = threading.Event()
        release = threading.Event()
        callback_done = threading.Event()
        errors = []

        def work():
            started.set()
            release.wait(1)
            return 1

        future = pool.submit(work)
        self.assertTrue(started.wait(1))

        def shutdown_from_callback(_future):
            try:
                pool.shutdown(wait=True)
            except RuntimeError as error:
                errors.append(error)
            finally:
                callback_done.set()

        future.add_done_callback(shutdown_from_callback)
        release.set()
        try:
            self.assertEqual(future.result(timeout=1), 1)
            self.assertTrue(callback_done.wait(1))
            self.assertEqual(len(errors), 1)
            self.assertEqual(
                str(errors[0]), "A BoundedWorkPool worker cannot wait for its own shutdown"
            )
            self.assertFalse(pool.stats().closed)
        finally:
            pool.shutdown(wait=True)
