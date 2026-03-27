"""Public exports for the shared logging package."""

from __future__ import annotations

try:
    from .config import LoggingConfig, SchemaMapping
    from .db import InMemoryAdapter, SQLServerAdapter, StorageAdapter
    from .logger import LoggerService, RunHandle, build_logger_service
    from .models import (
        HttpLogEntry,
        LogContext,
        ROBOT_DEFINITIONS,
        ROBOT_EVENT_TYPES,
        ROBOT_STATUSES,
        RobotDefinition,
        RunEventLog,
        RunLog,
        TransactionLog,
    )
except ImportError:
    from config import LoggingConfig, SchemaMapping
    from db import InMemoryAdapter, SQLServerAdapter, StorageAdapter
    from logger import LoggerService, RunHandle, build_logger_service
    from models import (
        HttpLogEntry,
        LogContext,
        ROBOT_DEFINITIONS,
        ROBOT_EVENT_TYPES,
        ROBOT_STATUSES,
        RobotDefinition,
        RunEventLog,
        RunLog,
        TransactionLog,
    )

__all__ = [
    "HttpLogEntry",
    "InMemoryAdapter",
    "LogContext",
    "LoggerService",
    "LoggingConfig",
    "ROBOT_DEFINITIONS",
    "ROBOT_EVENT_TYPES",
    "ROBOT_STATUSES",
    "RobotDefinition",
    "RunEventLog",
    "RunHandle",
    "RunLog",
    "SQLServerAdapter",
    "SchemaMapping",
    "StorageAdapter",
    "TransactionLog",
    "build_logger_service",
]
