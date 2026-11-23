"""Measure uncontended thread coordination and work-submission round trips."""

from flowcoord import KeyedConcurrencyLimiter, KeyedRateLimiter, Scheduler, SingleFlight, TaskScope

from ._runner import bench_sync, create_runner


def _value() -> int:
    return 42


def main() -> None:
    runner = create_runner(__doc__ or "Thread coordination benchmarks")
    runner.parse_args()
    limiter = KeyedConcurrencyLimiter[str](1)

    def use_permit() -> None:
        if not limiter.acquire_nowait("key"):
            raise RuntimeError("permit benchmark unexpectedly blocked")
        limiter.release("key")

    bench_sync(runner, "thread/permit-acquire-release", use_permit)
    rate = KeyedRateLimiter[str](rate=1, max_keys=1)
    bench_sync(runner, "thread/rate-try-acquire", lambda: rate.try_acquire("key"))
    flight = SingleFlight[str]()
    bench_sync(runner, "thread/single-flight", lambda: flight.run("key", _value))
    with TaskScope(max_workers=1) as scope:
        bench_sync(runner, "thread/task-start-join", lambda: scope.start(_value).join())
    with Scheduler(max_workers=1) as scheduler:
        bench_sync(
            runner, "thread/scheduler-once-join", lambda: scheduler.schedule_once(_value).join()
        )


if __name__ == "__main__":
    main()
