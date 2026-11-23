"""Benchmark loop portal roundtrips."""

from __future__ import annotations

from flowcoord.async_portal import AsyncLoopPortal

from ._runner import bench_async, bench_sync, create_runner


async def _noop() -> None:
    return None


def main() -> None:
    runner = create_runner(__doc__ or "Thread-to-async bridge benchmarks")
    runner.parse_args()

    bench_async(runner, "async-direct-await", _noop)

    with AsyncLoopPortal() as portal:
        bench_sync(
            runner,
            "loop-portal-roundtrip",
            lambda: portal.call(_noop, timeout=10),
        )


if __name__ == "__main__":
    main()
