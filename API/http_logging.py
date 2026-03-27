"""Compatibility wrapper for HTTP logging helpers."""

from __future__ import annotations

try:
    from ..utils import extract_error_code, read_float_env, read_int_env
except ImportError:
    from utils import extract_error_code, read_float_env, read_int_env

__all__ = ["extract_error_code", "read_float_env", "read_int_env"]
