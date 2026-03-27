"""Utility helpers for the shared logging package."""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any

try:
    from .models import LOG_LEVELS, ROBOT_EVENT_TYPES, ROBOT_STATUSES
except ImportError:
    from models import LOG_LEVELS, ROBOT_EVENT_TYPES, ROBOT_STATUSES


def utc_now() -> datetime:
    """Return the current UTC datetime."""
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    """Return the current UTC datetime as an ISO-8601 string."""
    return utc_now().isoformat().replace("+00:00", "Z")


def coerce_timestamp(value: datetime | str | None) -> datetime | None:
    """Convert strings or datetimes into UTC-aware datetimes."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    raise TypeError(f"Unsupported timestamp value: {type(value)!r}")


def generate_correlation_id() -> str:
    """Generate a correlation identifier."""
    return str(uuid.uuid4())


def generate_request_id() -> str:
    """Generate a request identifier."""
    return str(uuid.uuid4())


def normalize_log_level(level: str) -> str:
    """Validate and normalize a log level."""
    normalized = (level or "info").strip().lower()
    if normalized not in LOG_LEVELS:
        raise ValueError(f"Unsupported log level: {level}")
    return normalized


def normalize_status(status: str) -> str:
    """Validate and normalize a run status."""
    normalized = (status or "").strip().lower()
    if normalized not in ROBOT_STATUSES:
        raise ValueError(f"Unsupported robot status: {status}")
    return normalized


def normalize_event_type(event_type: str) -> str:
    """Validate and normalize a run event type."""
    normalized = (event_type or "").strip().lower()
    if normalized not in ROBOT_EVENT_TYPES:
        raise ValueError(f"Unsupported robot event type: {event_type}")
    return normalized


def truncate_text(value: str | None, max_length: int | None) -> str | None:
    """Truncate a string value when a max length is configured."""
    if value is None or max_length is None or max_length <= 0:
        return value
    if len(value) <= max_length:
        return value
    if max_length <= 3:
        return value[:max_length]
    return value[: max_length - 3] + "..."


def serialize_payload(value: Any, max_length: int | None = None) -> str | None:
    """Serialize arbitrary data to JSON for storage."""
    if value is None:
        return None
    if isinstance(value, str):
        return truncate_text(value, max_length)
    serialized = json.dumps(value, default=str, ensure_ascii=True)
    return truncate_text(serialized, max_length)


def extract_response_body(response: Any, max_length: int | None = None) -> str | None:
    """Best-effort extraction of a response body for logging."""
    if response is None:
        return None
    text = getattr(response, "text", None)
    if text:
        return truncate_text(str(text), max_length)
    try:
        payload = response.json()
    except Exception:
        return None
    return serialize_payload(payload, max_length=max_length)


def extract_error_code(response: Any) -> str:
    """
    Resolve a transaction code from an HTTP response.

    Returns ``errors[0].kode`` when available, otherwise the HTTP status code.
    """
    try:
        error_payload = response.json()
        errors = error_payload.get("errors")
        if isinstance(errors, list) and errors:
            first_error = errors[0]
            if isinstance(first_error, dict):
                kode = first_error.get("kode")
                if kode:
                    return str(kode)
    except (ValueError, AttributeError, TypeError):
        pass

    status_code = getattr(response, "status_code", "")
    return str(status_code) if status_code != "" else "UNKNOWN"


def read_int_env(name: str, default: int, min_value: int | None = None) -> int:
    """Read an integer value from an environment variable with safe fallback."""
    value = os.getenv(name)
    try:
        parsed = int(value) if value is not None else default
    except (TypeError, ValueError):
        parsed = default
    if min_value is not None and parsed < min_value:
        return min_value
    return parsed


def read_float_env(name: str, default: float) -> float:
    """Read a float value from an environment variable with safe fallback."""
    value = os.getenv(name)
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default
