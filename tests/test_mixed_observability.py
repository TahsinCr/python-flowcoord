"""Both execution modes can share one metrics recorder."""

import asyncio
import threading
import unittest

from flowcoord import AsyncTaskScope, OperationStats, TaskScope


class MixedObservabilityTests(unittest.IsolatedAsyncioTestCase):
    async def test_thread_and_async_work_share_one_operation_snapshot(self):
        metrics = OperationStats()
        ready = threading.Event()

        def record_in_thread():
            if not ready.wait(2):
                raise TimeoutError("async recording did not start")
            for _ in range(200):
                metrics.record("shared")

        async def record_on_loop():
            ready.set()
            for index in range(200):
                metrics.record("shared")
                if index % 20 == 0:
                    await asyncio.sleep(0)

        thread_scope = TaskScope(max_workers=1, observer=metrics)
        thread_task = thread_scope.start(record_in_thread, shutdown="drain")
        try:
            async with AsyncTaskScope(observer=metrics) as async_scope:
                async_task = async_scope.start(record_on_loop, shutdown="drain")
                await async_task.join(2)
            await asyncio.to_thread(thread_scope.close, 2)
        finally:
            ready.set()
            await asyncio.to_thread(thread_scope.close, 2)
        thread_task.join(0)
        snapshot = metrics.snapshot().operations
        self.assertEqual(snapshot["shared"].calls, 400)
        self.assertEqual(snapshot["task_scope.task"].successful, 2)

    async def test_latest_async_job_cancelled_before_start_releases_ownership(self):
        from flowcoord import AsyncLatestTaskCoordinator

        coordinator = AsyncLatestTaskCoordinator[str]()

        async def run():
            self.fail("pre-start cancelled task ran")

        pending = coordinator.submit("key", run, lambda value: None)
        pending.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await pending
        await coordinator.aclose(timeout=1)
        self.assertFalse(coordinator.snapshot().active)

    async def test_async_single_flight_rejects_recursion(self):
        from flowcoord import AsyncSingleFlight

        flight = AsyncSingleFlight[str]()

        async def recursive():
            return await flight.run("key", recursive)

        try:
            with self.assertRaisesRegex(RuntimeError, "itself"):
                await asyncio.wait_for(flight.run("key", recursive), timeout=1)
        finally:
            await flight.aclose()

    async def test_latest_result_callbacks_for_one_key_do_not_overlap(self):
        from flowcoord import AsyncLatestTaskCoordinator

        coordinator = AsyncLatestTaskCoordinator[str]()
        first_started = asyncio.Event()
        release = asyncio.Event()
        second_ready = asyncio.Event()
        results = []

        async def apply(value):
            if value == 1:
                first_started.set()
                await release.wait()
            results.append(value)

        async def first():
            return 1

        async def second():
            second_ready.set()
            return 2

        try:
            previous = coordinator.submit("key", first, apply)
            await asyncio.wait_for(first_started.wait(), 1)
            latest = coordinator.submit("key", second, apply, cancel_previous=False)
            await asyncio.wait_for(second_ready.wait(), 1)
            self.assertEqual(results, [])
            release.set()
            await asyncio.gather(previous, latest)
        finally:
            release.set()
            await coordinator.aclose(timeout=1)
        self.assertEqual(results, [1, 2])
