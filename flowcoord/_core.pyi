from typing import Any

class OperationStatsCore:
    def __init__(self, max_operations: int = ...) -> None: ...
    def record(
        self,
        name: str,
        status: str,
        elapsed: float,
        queue_depth: int | None,
        dropped: int,
        retries: int,
        rejected: int,
        timeouts: int,
        shutdown_seconds: float,
    ) -> None: ...
    def snapshot(self) -> Any: ...
    def reset(self) -> None: ...

def configure_observability(
    operation_metrics: type[Any],
    operation_stats_snapshot: type[Any],
    mapping_proxy: type[Any],
) -> None: ...
