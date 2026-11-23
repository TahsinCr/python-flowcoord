"""Thread ownership, wakeups, shutdown, and split imports."""

from __future__ import annotations

import subprocess
import sys
import threading
import time
import unittest
from concurrent.futures import CancelledError, ThreadPoolExecutor
from pathlib import Path

from flowcoord import (
    KeyedConcurrencyLimiter,
    KeyedRateLimiter,
    LatestTaskCoordinator,
    OperationStats,
    Scheduler,
    SingleFlight,
    TaskScope,
)


class ThreadCoordinationTests(unittest.TestCase):
    def wait_until(self, predicate):
        end = time.monotonic() + 2
        while not predicate():
            if time.monotonic() >= end:
                self.fail("worker did not reach the required state")
            time.sleep(0.001)

    def test_sync_public_imports_do_not_load_async_modules(self):
        source = """
import sys
assert 'asyncio' not in sys.modules
from flowcoord import Scheduler, TaskScope, RetryPolicy, ResourceScope, SingleFlight
from flowcoord.concurrency import KeyedConcurrencyLimiter, KeyedRateLimiter
assert 'asyncio' not in sys.modules
assert not any(name.startswith(('flowcoord.async_', 'flowcoord._async_')) for name in sys.modules)
import flowcoord
assert 'AsyncScheduler' in dir(flowcoord)
assert flowcoord.AsyncScheduler.__module__ == 'flowcoord.async_scheduling'
assert 'asyncio' in sys.modules
"""
        subprocess.run(
            [sys.executable, "-c", source],
            cwd=Path(__file__).resolve().parents[1],
            check=True,
            timeout=10,
        )

    def test_permits_preserve_fifo_without_leaking_on_timeout(self):
        limiter = KeyedConcurrencyLimiter[str](1)
        acquired = []

        def enter(id_):
            with limiter.acquire("key", timeout=2):
                acquired.append(id_)

        with ThreadPoolExecutor(2) as pool:
            with limiter.acquire("key"):
                first = pool.submit(enter, 1)
                self.wait_until(lambda: limiter.stats().waiters == 1)
                second = pool.submit(enter, 2)
                self.wait_until(lambda: limiter.stats().waiters == 2)
                self.assertFalse(limiter.acquire_nowait("key"))
                with self.assertRaises(TimeoutError), limiter.acquire("key", timeout=0.01):
                    self.fail("timed-out waiter acquired a permit")
                self.assertEqual(limiter.stats().waiters, 2)
            first.result(2)
            second.result(2)
        self.assertEqual(acquired, [1, 2])
        self.assertEqual(limiter.stats().keys, 0)

    def test_close_wakes_blocked_permit_and_token_waiters(self):
        concurrency = KeyedConcurrencyLimiter[str](1)
        rate = KeyedRateLimiter[str](rate=0.1, burst=1)
        self.assertTrue(rate.try_acquire("key"))

        def take_permit():
            with concurrency.acquire("key", timeout=2):
                self.fail("closed limiter admitted a waiter")

        with ThreadPoolExecutor(2) as pool, concurrency.acquire("key"):
            permit = pool.submit(take_permit)
            token = pool.submit(rate.acquire, "key", 2)
            self.wait_until(lambda: concurrency.stats().waiters == rate.stats().waiters == 1)
            concurrency.close()
            rate.close()
            for future in permit, token:
                with self.assertRaisesRegex(RuntimeError, "closed"):
                    future.result(2)
        self.assertEqual(concurrency.stats().active, 0)
        self.assertEqual(rate.stats().waiters, 0)

    def test_rate_timeout_keeps_other_keys_independent(self):
        rate = KeyedRateLimiter[str](1, period=10, burst=1, max_keys=2)
        rate.acquire("a", timeout=0)
        with self.assertRaises(TimeoutError):
            rate.acquire("a", timeout=0.01)
        self.assertEqual(rate.stats().waiters, 0)
        self.assertTrue(rate.try_acquire("b"))
        with self.assertRaisesRegex(RuntimeError, "capacity"):
            rate.try_acquire("c")
        rate.close()

    def test_single_flight_shares_result_and_retries_close_after_timeout(self):
        flight = SingleFlight[str]()
        start = threading.Event()
        release = threading.Event()
        output = object()

        def produce():
            start.set()
            self.assertTrue(release.wait(2))
            return output

        with ThreadPoolExecutor(2) as pool:
            leader = pool.submit(flight.run, "a", produce)
            try:
                self.assertTrue(start.wait(2))
                follower = pool.submit(flight.run, "a", lambda: self.fail("ran a second call"))
                self.wait_until(lambda: flight.stats().shared_waiters == 1)
                with self.assertRaises(TimeoutError):
                    flight.close(timeout=0)
            finally:
                release.set()
            self.assertIs(leader.result(2), output)
            self.assertIs(follower.result(2), output)
        flight.close(timeout=2)
        self.assertEqual(flight.stats().active, 0)

    def test_single_flight_rejects_a_recursive_wait(self):
        flight = SingleFlight[str]()
        with self.assertRaisesRegex(RuntimeError, "itself"):
            flight.run("key", lambda: flight.run("key", lambda: None))
        self.assertEqual(flight.run("key", lambda: 7), 7)
        flight.close()

    def test_task_scope_cancels_queued_calls_leaving_drain_work_owned(self):
        metrics = OperationStats()
        scope = TaskScope(max_workers=1, max_pending=2, observer=metrics)
        started = threading.Event()
        release = threading.Event()

        def running():
            started.set()
            self.assertTrue(release.wait(2))
            return 1

        first = scope.start(running, shutdown="drain")
        try:
            self.assertTrue(started.wait(2))
            cancelled = scope.start(lambda: self.fail("cancelled job ran"))
            drained = scope.start(lambda: 2, shutdown="drain")
            with self.assertRaises(TimeoutError):
                scope.close(timeout=0)
            self.assertEqual(scope.snapshot().active, 2)
            with self.assertRaises(CancelledError):
                cancelled.join()
        finally:
            release.set()
            scope.close(timeout=2)
        self.assertEqual((first.join(), drained.join()), (1, 2))
        self.assertEqual(scope.snapshot().cancelled, 1)
        self.assertEqual(scope.snapshot().completed, 2)

    def test_latest_thread_result_discards_superseded_work(self):
        callbacks = []
        started = threading.Event()
        release = threading.Event()

        def old():
            started.set()
            self.assertTrue(release.wait(2))
            return 1

        coordinator = LatestTaskCoordinator[str](max_workers=2)
        try:
            stale = coordinator.submit("key", old, callbacks.append)
            self.assertTrue(started.wait(2))
            fresh = coordinator.submit("key", lambda: 2, callbacks.append)
            fresh.join(2)
        finally:
            release.set()
            coordinator.close(timeout=2)
        stale.join(2)
        self.assertEqual(callbacks, [2])
        self.assertEqual(coordinator.snapshot().discarded_results, 1)
        self.assertFalse(coordinator.snapshot().active)

    def test_scheduler_bounds_jobs_and_can_cancel_a_future_timer(self):
        with Scheduler(max_jobs=1) as scheduler:
            job = scheduler.schedule_once(self.fail, "cancelled job ran", delay=60)
            with self.assertRaisesRegex(RuntimeError, "capacity"):
                scheduler.schedule_once(lambda: None)
            self.assertTrue(job.cancel())
            with self.assertRaises(CancelledError):
                job.join(timeout=1)
            self.assertEqual(scheduler.schedule_once(abs, -42).join(timeout=2), 42)

    def test_periodic_cancel_keeps_running_callback_owned_until_it_returns(self):
        started = threading.Event()
        release = threading.Event()

        def tick():
            started.set()
            self.assertTrue(release.wait(2))

        scheduler = Scheduler(max_workers=1)
        job = scheduler.periodic(tick, interval=0.01, initial_delay=0)
        try:
            self.assertTrue(started.wait(2))
            self.assertTrue(job.cancel())
            self.assertFalse(job.done)
            with self.assertRaises(TimeoutError):
                scheduler.close(timeout=0)
        finally:
            release.set()
            scheduler.close(timeout=2)
        with self.assertRaises(CancelledError):
            job.join(timeout=2)
        self.assertEqual(scheduler.snapshot().active, 0)

    def test_scheduler_reports_error_and_rejects_own_worker_wait(self):
        errors = []
        with Scheduler(on_error=errors.append) as scheduler:
            job = scheduler.schedule_once(scheduler.close)
            with self.assertRaisesRegex(RuntimeError, "own scheduler"):
                job.join(timeout=2)
            self.wait_until(lambda: len(errors) == 1)
            self.assertEqual(scheduler.snapshot().failed, 1)

    def test_latest_result_callbacks_for_one_key_do_not_overlap(self):
        first_started = threading.Event()
        release = threading.Event()
        second_ready = threading.Event()
        results = []

        def apply(value):
            if value == 1:
                first_started.set()
                self.assertTrue(release.wait(2))
            results.append(value)

        coordinator = LatestTaskCoordinator[str](max_workers=2)
        try:
            first = coordinator.submit("key", lambda: 1, apply)
            self.assertTrue(first_started.wait(2))

            def second():
                second_ready.set()
                return 2

            latest = coordinator.submit("key", second, apply, cancel_previous=False)
            self.assertTrue(second_ready.wait(2))
            self.assertEqual(results, [])
            release.set()
            first.join(2)
            latest.join(2)
        finally:
            release.set()
            coordinator.close(timeout=2)
        self.assertEqual(results, [1, 2])
