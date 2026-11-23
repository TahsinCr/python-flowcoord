"""Benchmark operation metric recording."""

from __future__ import annotations

from flowcoord.observability import OperationStats

from ._runner import bench_sync, create_runner


def main() -> None:
    runner = create_runner(__doc__ or "FlowCoord component benchmarks")
    runner.parse_args()

    metrics = OperationStats(max_operations=16)
    bench_sync(runner, "operation-stats/record", lambda: metrics.record("publish"))


if __name__ == "__main__":
    main()
