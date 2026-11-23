"""Thread-based and asyncio coordination with an independent sync import path."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

from .base import (
    CircuitBreakerStats,
    CooldownStats,
    KeyedConcurrencyKeyStats,
    KeyedConcurrencyStats,
    KeyedRateStats,
    LatestTaskSnapshot,
    RetryObservation,
    SchedulerStats,
    SingleFlightStats,
    TaskScopeSnapshot,
)
from .concurrency import KeyedConcurrencyLimiter, KeyedRateLimiter, SingleFlight
from .errors import AsyncLoopPortalClosedError, CircuitOpenError
from .executors import BoundedWorkPool, WorkPoolStats
from .observability import OperationMetrics, OperationStats, OperationStatsSnapshot
from .resilience import CircuitBreaker, CooldownRegistry, RetryPolicy
from .scheduling import CoalescerStats, KeyedCoalescer, ScheduledHandle, Scheduler
from .scopes import ResourceScope
from .tasks import LatestTaskCoordinator, TaskHandle, TaskScope
from .timing import Deadline

if TYPE_CHECKING:
    from .async_concurrency import (
        AsyncKeyedConcurrencyLimiter,
        AsyncKeyedRateLimiter,
        AsyncSingleFlight,
    )
    from .async_portal import AsyncLoopPortal
    from .async_resilience import AsyncCircuitBreaker, AsyncCooldownRegistry, AsyncRetryPolicy
    from .async_scheduling import AsyncScheduledHandle, AsyncScheduler
    from .async_scopes import AsyncResourceScope
    from .async_tasks import AsyncLatestTaskCoordinator, AsyncTaskHandle, AsyncTaskScope

_ASYNC_EXPORTS = {
    "AsyncKeyedConcurrencyLimiter": ".async_concurrency",
    "AsyncKeyedRateLimiter": ".async_concurrency",
    "AsyncSingleFlight": ".async_concurrency",
    "AsyncLoopPortal": ".async_portal",
    "AsyncCircuitBreaker": ".async_resilience",
    "AsyncCooldownRegistry": ".async_resilience",
    "AsyncRetryPolicy": ".async_resilience",
    "AsyncScheduledHandle": ".async_scheduling",
    "AsyncScheduler": ".async_scheduling",
    "AsyncResourceScope": ".async_scopes",
    "AsyncLatestTaskCoordinator": ".async_tasks",
    "AsyncTaskHandle": ".async_tasks",
    "AsyncTaskScope": ".async_tasks",
}

__all__ = [
    "AsyncCircuitBreaker",
    "AsyncCooldownRegistry",
    "AsyncKeyedConcurrencyLimiter",
    "AsyncKeyedRateLimiter",
    "AsyncLatestTaskCoordinator",
    "AsyncLoopPortal",
    "AsyncLoopPortalClosedError",
    "AsyncResourceScope",
    "AsyncRetryPolicy",
    "AsyncScheduledHandle",
    "AsyncScheduler",
    "AsyncSingleFlight",
    "AsyncTaskHandle",
    "AsyncTaskScope",
    "BoundedWorkPool",
    "CircuitBreaker",
    "CircuitBreakerStats",
    "CircuitOpenError",
    "CoalescerStats",
    "CooldownRegistry",
    "CooldownStats",
    "Deadline",
    "KeyedCoalescer",
    "KeyedConcurrencyKeyStats",
    "KeyedConcurrencyLimiter",
    "KeyedConcurrencyStats",
    "KeyedRateLimiter",
    "KeyedRateStats",
    "LatestTaskCoordinator",
    "LatestTaskSnapshot",
    "OperationMetrics",
    "OperationStats",
    "OperationStatsSnapshot",
    "ResourceScope",
    "RetryObservation",
    "RetryPolicy",
    "ScheduledHandle",
    "Scheduler",
    "SchedulerStats",
    "SingleFlight",
    "SingleFlightStats",
    "TaskHandle",
    "TaskScope",
    "TaskScopeSnapshot",
    "WorkPoolStats",
]


def __getattr__(name: str) -> Any:
    module = _ASYNC_EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module, __name__), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
