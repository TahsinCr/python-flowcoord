"""Synchronous execution primitive behavior."""

from __future__ import annotations

import threading
import unittest
from unittest.mock import patch

from _support import ManualClock

from flowcoord._compatibility import ExceptionGroup
from flowcoord.async_scheduling import AsyncScheduler
from flowcoord.async_tasks import AsyncTaskScope
from flowcoord.concurrency import KeyedConcurrencyLimiter, KeyedRateLimiter
from flowcoord.executors import BoundedWorkPool
from flowcoord.observability import OperationStats
from flowcoord.resilience import CircuitBreaker, CooldownRegistry, RetryPolicy
from flowcoord.scheduling import KeyedCoalescer
from flowcoord.scopes import ResourceScope
from flowcoord.timing import Deadline


class PrimitiveTests(unittest.TestCase):
    def test_deadline_is_monotonic_and_child_cannot_extend_parent(self):
        clock = ManualClock(10.0)
        deadline = Deadline.after(5, clock=clock)
        child = deadline.child(2)
        clock.set(11.0)
        self.assertEqual(deadline.remaining, 4.0)
        self.assertEqual(child.remaining, 1.0)
        self.assertEqual(child.child(20)._expires_at, 12.0)
        clock.set(13.0)
        self.assertTrue(child.expired)
        self.assertIsNone(Deadline.after(None, clock=clock).remaining)
        for invalid in (-1, float("inf"), True, 10**1000):
            with self.assertRaises((TypeError, ValueError)):
                Deadline.after(invalid)

    def test_operation_stats_bound_cardinality_and_freeze_snapshots(self):
        stats = OperationStats(max_operations=2)
        stats.record("bus.publish", elapsed=0.25, queue_depth=3, dropped=1)
        stats.record("task.run", status="error", retries=2, timeouts=1)
        stats.record("unbounded-user-key-123", status="cancelled")
        snapshot = stats.snapshot()
        self.assertEqual(set(snapshot.operations), {"bus.publish", "other"})
        self.assertEqual(snapshot.operations["bus.publish"].max_queue_depth, 3)
        self.assertEqual(snapshot.operations["other"].cancelled, 1)
        with self.assertRaises(TypeError):
            snapshot.operations["new"] = snapshot.operations["other"]

    def test_operation_stats_python_fallback_accepts_unbounded_counters(self):
        maximum = (1 << 63) - 1
        oversized = 1 << 100
        with patch("flowcoord.observability.CORE", None):
            stats = OperationStats()
            stats.record(
                "overflow",
                dropped=maximum,
                retries=maximum,
                rejected=maximum,
                timeouts=maximum,
                queue_depth=oversized,
            )
            stats.record(
                "overflow", dropped=5, retries=oversized, rejected=7, timeouts=9, queue_depth=9
            )
            stats.record("overflow")

            class Counter(int):
                normalized = []

                def __int__(self):
                    value = int.__int__(self)
                    self.normalized.append(value)
                    return value

            stats.record(
                "subclass",
                dropped=Counter(5),
                retries=Counter(oversized),
                queue_depth=Counter(7),
            )

        metrics = stats.snapshot().operations["overflow"]
        self.assertEqual(metrics.dropped, maximum + 5)
        self.assertEqual(metrics.retries, maximum + oversized)
        self.assertEqual(metrics.rejected, maximum + 7)
        self.assertEqual(metrics.timeouts, maximum + 9)
        self.assertEqual(metrics.queue_depth, 9)
        self.assertEqual(metrics.max_queue_depth, oversized)
        metrics = stats.snapshot().operations["subclass"]
        self.assertEqual(metrics.dropped, 5)
        self.assertEqual(metrics.retries, oversized)
        self.assertEqual(metrics.queue_depth, 7)
        self.assertEqual(Counter.normalized, [5, oversized, 7])

    def test_resource_scope_order_borrowing_detach_and_retry(self):
        calls = []

        class Resource:
            def __init__(self, name, fail=False):
                self.name, self.fail = name, fail

            def close(self):
                calls.append(self.name)
                if self.fail:
                    raise ValueError(self.name)

        scope = ResourceScope()
        borrowed = Resource("borrowed")
        failed = Resource("failed", fail=True)
        owned = Resource("owned")
        scope.add(borrowed, owned=False)
        scope.add(failed)
        scope.add(owned)
        detached = Resource("detached")
        scope.add(detached)
        self.assertIs(scope.detach(detached), detached)
        with self.assertRaises(ExceptionGroup) as raised:
            scope.close()
        self.assertIn("Resource scope cleanup failed", str(raised.exception))
        self.assertEqual(calls, ["owned", "failed"])
        self.assertEqual(scope.pending_cleanup(), 1)
        failed.fail = False
        scope.close()
        self.assertEqual(calls, ["owned", "failed", "failed"])
        self.assertEqual(scope.pending_cleanup(), 0)

    def test_resource_scope_ignores_reentrant_close_from_cleanup(self):
        scope = ResourceScope()
        scope.defer(scope.close)

        scope.close()

        self.assertTrue(scope.closed)
        self.assertEqual(scope.pending_cleanup(), 0)

    def test_bounded_work_pool_separates_running_and_pending(self):
        started = threading.Event()
        release = threading.Event()

        def blocking():
            started.set()
            release.wait(2)
            return 1

        pool = BoundedWorkPool(max_workers=1, max_pending=1)
        first = pool.submit(blocking)
        self.assertTrue(started.wait(1))
        second = pool.submit(lambda: 2)
        self.assertEqual(pool.stats().pending, 1)
        with self.assertRaises(RuntimeError):
            pool.submit(lambda: 3)
        release.set()
        self.assertEqual(first.result(1), 1)
        self.assertEqual(second.result(1), 2)
        pool.shutdown()
        self.assertTrue(pool.stats().closed)

    def test_bounded_work_pool_uses_configured_thread_name_prefix(self):
        pool = BoundedWorkPool(
            max_workers=1,
            max_pending=0,
            thread_name_prefix="MavDispatcher",
        )
        try:
            worker_name = pool.submit(lambda: threading.current_thread().name).result(1)
        finally:
            pool.shutdown()

        self.assertEqual(worker_name, "MavDispatcher_0")

    def test_sync_retry_is_explicit_and_circuit_breaker_recovers(self):
        calls = []
        retry = RetryPolicy(max_attempts=3)

        def operation():
            calls.append(1)
            if len(calls) < 3:
                raise OSError("temporary")
            return "ok"

        self.assertEqual(
            retry.run(operation, should_retry=lambda error: isinstance(error, OSError)), "ok"
        )
        self.assertEqual(len(calls), 3)
        single_attempt = []
        with self.assertRaises(OSError):
            retry.run(lambda: (single_attempt.append(1), (_ for _ in ()).throw(OSError()))[1])
        self.assertEqual(len(single_attempt), 1)
        bounded = RetryPolicy(max_attempts=1_000_000, initial_delay=0.1, max_delay=1)
        self.assertEqual(bounded._delay(1_000_000), 1)

    def test_retry_closes_awaitable_returned_by_sync_operation(self):
        async def operation():
            return "wrong mode"

        pending = operation()
        with self.assertRaisesRegex(TypeError, "synchronous operation"):
            RetryPolicy(max_attempts=3).run(lambda: pending)

        self.assertIsNone(pending.cr_frame)

    def test_retry_observer_failure_keeps_the_operation_error(self):
        operation_error = OSError("operation failed")
        observer_error = RuntimeError("observer failed")
        attempts = []

        def operation():
            attempts.append(None)
            raise operation_error

        def observer(_observation):
            raise observer_error

        retry = RetryPolicy(max_attempts=3, observer=observer)
        with self.assertRaises(ExceptionGroup) as caught:
            retry.run(operation, should_retry=lambda _error: True)

        self.assertEqual(caught.exception.exceptions, (operation_error, observer_error))
        self.assertEqual(len(attempts), 1)

    def test_sync_retry_delay_is_capped_to_its_deadline(self):
        clock = ManualClock(10)
        deadline = Deadline.after(1, clock=clock)
        attempts = []
        retry = RetryPolicy(max_attempts=3, initial_delay=2)

        def operation():
            attempts.append(None)
            raise OSError("temporary")

        with patch("flowcoord.resilience.time.sleep", side_effect=clock.advance) as sleep:
            with self.assertRaisesRegex(TimeoutError, "before another attempt"):
                retry.run(operation, should_retry=lambda _error: True, deadline=deadline)

        self.assertEqual(len(attempts), 1)
        sleep.assert_called_once_with(1)

    def test_circuit_breaker_rejects_async_callable_on_sync_path(self):
        breaker = CircuitBreaker(failure_threshold=1)

        async def operation():
            return 1

        with self.assertRaisesRegex(TypeError, "synchronous operation"):
            breaker.call(operation)
        self.assertEqual(breaker.state, "closed")

    def test_sync_extension_callbacks_reject_awaitables_cleanly(self):
        async def async_result():
            return True

        retry = RetryPolicy(max_attempts=2)
        with self.assertRaises(TypeError):
            retry.run(
                lambda: (_ for _ in ()).throw(OSError("temporary")),
                should_retry=lambda _error: async_result(),
            )
        with self.assertRaises(TypeError):
            RetryPolicy(observer=lambda _observation: async_result()).run(lambda: 1)

        async def async_work():
            return 1

        pool = BoundedWorkPool(max_workers=1, max_pending=0)
        with self.assertRaises(TypeError):
            pool.submit(async_work)
        with self.assertRaises(TypeError):
            pool.submit(lambda: async_result()).result(1)
        pool.close()

        clock = ManualClock()
        breaker = CircuitBreaker(
            failure_threshold=1,
            recovery_timeout=5,
            clock=clock,
        )
        with self.assertRaises(ValueError):
            breaker.call(lambda: (_ for _ in ()).throw(ValueError("failed")))
        self.assertEqual(breaker.state, "open")
        with self.assertRaises(RuntimeError):
            breaker.call(lambda: None)
        clock.set(5.0)
        self.assertEqual(breaker.state, "half_open")
        self.assertEqual(breaker.call(lambda: "recovered"), "recovered")
        self.assertEqual(breaker.state, "closed")

    def test_cooldown_registry_uses_monotonic_deadlines_and_can_clear(self):
        clock = ManualClock(1.0)
        cooldowns = CooldownRegistry(clock=clock)
        cooldowns.block("key", 4)
        self.assertEqual(cooldowns.remaining("key"), 4)
        cooldowns.block("key", 2)
        self.assertEqual(cooldowns.remaining("key"), 4)
        cooldowns.block("key", 2, extend=False)
        self.assertEqual(cooldowns.remaining("key"), 2)
        clock.advance(2)
        self.assertFalse(cooldowns.is_blocked("key"))
        self.assertEqual(cooldowns.prune(), 0)

    def test_cooldown_prunes_other_expired_keys_only_when_capacity_requires_it(self):
        clock = ManualClock(0.0)
        cooldowns = CooldownRegistry(max_entries=2, clock=clock)
        cooldowns.block("expired", 1)
        cooldowns.block("active", 10)
        clock.advance(1)

        self.assertEqual(cooldowns.block("active", 2), 9)
        self.assertEqual(cooldowns.block("new", 2), 2)
        self.assertFalse(cooldowns.is_blocked("expired"))
        self.assertTrue(cooldowns.is_blocked("active"))
        self.assertTrue(cooldowns.is_blocked("new"))

    def test_operation_observers_are_validated_at_construction(self):
        invalid_observers = (
            lambda: AsyncTaskScope(observer=object()),
            lambda: KeyedConcurrencyLimiter(1, observer=object()),
            lambda: KeyedRateLimiter(1, observer=object()),
            lambda: BoundedWorkPool(max_workers=1, max_pending=0, observer=object()),
            lambda: KeyedCoalescer(
                interval=1,
                key=lambda value: value,
                merge=lambda first, _second: first,
                callback=lambda _value: None,
                observer=object(),
            ),
            lambda: AsyncScheduler(observer=object()),
        )
        for create in invalid_observers:
            with self.subTest(create=create), self.assertRaises(TypeError):
                create()

    def test_operation_stats_reject_invalid_public_record_values(self):
        observer = OperationStats()
        invalid_records = (
            ("", {}, ValueError),
            ("publish", {"status": "unknown"}, ValueError),
            ("publish", {"elapsed": True}, TypeError),
            ("publish", {"elapsed": float("nan")}, ValueError),
            ("publish", {"elapsed": -1}, ValueError),
            ("publish", {"queue_depth": True}, ValueError),
            ("publish", {"queue_depth": -1}, ValueError),
            ("publish", {"dropped": 1.5}, ValueError),
            ("publish", {"retries": True}, ValueError),
            ("publish", {"rejected": -1}, ValueError),
            ("publish", {"timeouts": 1.0}, ValueError),
            ("publish", {"shutdown_seconds": float("inf")}, ValueError),
        )

        for name, values, error_type in invalid_records:
            with self.subTest(name=name, values=values):
                with self.assertRaises(error_type):
                    observer.record(name, **values)
