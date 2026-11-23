"""Errors raised by FlowCoord components."""


class AsyncLoopPortalClosedError(RuntimeError):
    """Raised when submitting work to a stopped LoopPortal."""


class CircuitOpenError(RuntimeError):
    """Raised when a circuit breaker rejects a call while open or saturated."""
