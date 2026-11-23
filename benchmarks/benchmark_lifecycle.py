"""Benchmark work submission, scopes, and single-flight coordination."""

from __future__ import annotations

import asyncio

from flowcoord.async_concurrency import AsyncSingleFlight
from flowcoord.async_scopes import AsyncResourceScope
from flowcoord.async_tasks import AsyncTaskScope
from flowcoord.executors import BoundedWorkPool
from flowcoord.scopes import ResourceScope

from ._runner import bench_async, bench_sync, create_runner


def _identity(value: int) -> int:
    return value


async def _async_identity(value: int) -> int:
    return value


def main() -> None:
    runner = create_runner(__doc__ or "FlowCoord lifecycle benchmarks")
    runner.parse_args()

    def resource_scope() -> int:
        with ResourceScope() as scope:
            scope.defer(lambda: None)
            return scope.pending_cleanup()

    bench_sync(runner, "resource-scope/defer-close", resource_scope)

    async def async_resource_scope() -> int:
        async with AsyncResourceScope() as scope:
            scope.defer(lambda: None)
            return scope.pending_cleanup()

    bench_async(runner, "async-resource-scope/defer-close", async_resource_scope)

    with BoundedWorkPool(max_workers=1, max_pending=1) as pool:
        bench_sync(
            runner,
            "bounded-work-pool/submit-result",
            lambda: pool.submit(_identity, 1).result(),
        )

    async def task_scope_roundtrip() -> int:
        async with AsyncTaskScope() as scope:
            handle = scope.start(lambda: _async_identity(1), shutdown="drain")
            return await handle.join()

    bench_async(runner, "async-task-scope/start-join-close", task_scope_roundtrip)

    async def singleflight_burst() -> tuple[int, ...]:
        singleflight = AsyncSingleFlight[str]()

        async def factory() -> int:
            await asyncio.sleep(0)
            return 1

        try:
            return tuple(
                await asyncio.gather(*(singleflight.run("shared", factory) for _ in range(8)))
            )
        finally:
            await singleflight.aclose()

    bench_async(
        runner,
        "single-flight/same-key-eight-waiters",
        singleflight_burst,
        metadata={"waiters": 8},
    )


if __name__ == "__main__":
    main()
