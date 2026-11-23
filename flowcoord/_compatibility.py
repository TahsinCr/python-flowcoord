"""Expose standard-library features across supported Python versions."""

from __future__ import annotations

import sys

if sys.version_info >= (3, 11):
    from builtins import BaseExceptionGroup, ExceptionGroup
else:  # pragma: no cover - exercised by the Python 3.10 environment
    from exceptiongroup import BaseExceptionGroup, ExceptionGroup

__all__ = ["BaseExceptionGroup", "ExceptionGroup"]
