"""Small shared test utilities."""

from collections.abc import Callable
from typing import NoReturn


def _reject_task_creation(rejected: list[object]) -> Callable[..., NoReturn]:
    def reject_task(coroutine: object, **_: object) -> NoReturn:
        rejected.append(coroutine)
        raise RuntimeError("task creation rejected")

    return reject_task


class ManualClock:
    """A controllable monotonic clock for deterministic timing tests."""

    def __init__(self, value: float = 0.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value

    def set(self, value: float) -> None:
        self.value = value

    def advance(self, seconds: float) -> None:
        self.value += seconds


class _AsyncValueMixin:
    @staticmethod
    async def _value(value):
        return value
