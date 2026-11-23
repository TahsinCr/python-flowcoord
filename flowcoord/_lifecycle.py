from __future__ import annotations

from ._compatibility import BaseExceptionGroup, ExceptionGroup


def raise_cleanup_errors(
    errors: list[BaseException],
    *,
    message: str,
    exclude_repeated_cause: bool = False,
) -> None:
    """Raise all cleanup failures without hiding cancellation-like exceptions."""
    if not errors:
        return
    for index, error in enumerate(errors):
        if not isinstance(error, Exception):
            remaining = (
                [candidate for candidate in errors if candidate is not error]
                if exclude_repeated_cause
                else errors[:index] + errors[index + 1 :]
            )
            if remaining:
                raise error from BaseExceptionGroup(message, remaining)
            raise error
    raise ExceptionGroup(message, [error for error in errors if isinstance(error, Exception)])
