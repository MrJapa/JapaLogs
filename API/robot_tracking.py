"""Compatibility wrapper for durable robot run tracking."""

from __future__ import annotations

from typing import Any, Optional

try:
    from ..logger import LoggerService, RunHandle, default_triggered_by
    from ..models import ROBOT_DEFINITIONS, ROBOT_EVENT_TYPES, ROBOT_STATUSES, RobotDefinition
except ImportError:
    from logger import LoggerService, RunHandle, default_triggered_by
    from models import ROBOT_DEFINITIONS, ROBOT_EVENT_TYPES, ROBOT_STATUSES, RobotDefinition


RobotRunHandle = RunHandle


class RobotRunTracker:
    """Compatibility facade that delegates to the shared logger service."""

    def __init__(self, logger_service: LoggerService | None = None) -> None:
        self.logger = logger_service or LoggerService()

    def ensure_schema(self) -> None:
        self.logger.storage.ensure_schema()

    def create_run(
        self,
        *,
        robot_id: str,
        trigger_source: str,
        triggered_by: Optional[str] = None,
        input_params: Optional[dict[str, Any]] = None,
        retry_of_run_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ) -> RobotRunHandle:
        return self.logger.create_run(
            robot_id=robot_id,
            trigger_source=trigger_source,
            triggered_by=triggered_by,
            input_params=input_params,
            retry_of_run_id=retry_of_run_id,
            correlation_id=correlation_id,
        )

    def bind_job(self, run_id: str, job_id: Optional[str]) -> None:
        self.logger.bind_job(run_id, job_id)

    def mark_started(self, run_id: str, *, message: Optional[str] = None) -> None:
        self.logger.mark_started(run_id, message=message)

    def heartbeat(self, run_id: str, *, message: Optional[str] = None, payload: Optional[dict[str, Any]] = None) -> None:
        self.logger.heartbeat(run_id, message=message, payload=payload)

    def progress(
        self,
        run_id: str,
        *,
        current: Optional[int] = None,
        total: Optional[int] = None,
        unit: Optional[str] = None,
        message: Optional[str] = None,
        records_processed: Optional[int] = None,
        records_succeeded: Optional[int] = None,
        records_failed: Optional[int] = None,
    ) -> None:
        self.logger.progress(
            run_id,
            current=current,
            total=total,
            unit=unit,
            message=message,
            records_processed=records_processed,
            records_succeeded=records_succeeded,
            records_failed=records_failed,
        )

    def mark_persisting(self, run_id: str, message: str = "Persisting records") -> None:
        self.logger.mark_persisting(run_id, message=message)

    def mark_completed(
        self,
        run_id: str,
        *,
        message: Optional[str] = None,
        correlation_id: Optional[str] = None,
        latest_transaction_id: Optional[int] = None,
        records_processed: Optional[int] = None,
        records_succeeded: Optional[int] = None,
        records_failed: Optional[int] = None,
    ) -> None:
        self.logger.mark_completed(
            run_id,
            message=message,
            correlation_id=correlation_id,
            latest_transaction_id=latest_transaction_id,
            records_processed=records_processed,
            records_succeeded=records_succeeded,
            records_failed=records_failed,
        )

    def mark_failed(
        self,
        run_id: str,
        *,
        error_message: str,
        failure_category: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ) -> None:
        self.logger.mark_failed(
            run_id,
            error_message=error_message,
            failure_category=failure_category,
            correlation_id=correlation_id,
        )

    def mark_cancel_requested(self, run_id: str, *, message: str = "Cancellation requested by user") -> None:
        self.logger.mark_cancel_requested(run_id, message=message)

    def mark_cancelled(self, run_id: str, *, message: str = "Cancelled by user") -> None:
        self.logger.mark_cancelled(run_id, message=message)

    def attach_transaction(self, run_id: str, *, correlation_id: Optional[str] = None, latest_transaction_id: Optional[int] = None) -> None:
        self.logger.attach_transaction(
            run_id,
            correlation_id=correlation_id,
            latest_transaction_id=latest_transaction_id,
        )

    def update_run(self, run_id: str, **fields: Any) -> None:
        self.logger.update_run(run_id, **fields)


__all__ = [
    "ROBOT_DEFINITIONS",
    "ROBOT_EVENT_TYPES",
    "ROBOT_STATUSES",
    "RobotDefinition",
    "RobotRunHandle",
    "RobotRunTracker",
    "default_triggered_by",
]
