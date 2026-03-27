"""High-level logging service and durable run handle APIs."""

from __future__ import annotations

import getpass
import os
import socket
from dataclasses import replace
from typing import Any

try:
    from .config import LoggingConfig
    from .db import SQLServerAdapter, StorageAdapter
    from .models import (
        ROBOT_DEFINITIONS,
        HttpLogEntry,
        LogContext,
        RobotDefinition,
        RunEventLog,
        RunLog,
        RunState,
        TransactionLog,
    )
    from .utils import (
        coerce_timestamp,
        extract_error_code,
        extract_response_body,
        generate_correlation_id,
        generate_request_id,
        normalize_event_type,
        normalize_log_level,
        normalize_status,
        serialize_payload,
        utc_now,
    )
except ImportError:
    from config import LoggingConfig
    from db import SQLServerAdapter, StorageAdapter
    from models import (
        ROBOT_DEFINITIONS,
        HttpLogEntry,
        LogContext,
        RobotDefinition,
        RunEventLog,
        RunLog,
        RunState,
        TransactionLog,
    )
    from utils import (
        coerce_timestamp,
        extract_error_code,
        extract_response_body,
        generate_correlation_id,
        generate_request_id,
        normalize_event_type,
        normalize_log_level,
        normalize_status,
        serialize_payload,
        utc_now,
    )


def default_triggered_by(trigger_source: str) -> str:
    """Best-effort actor identifier for backend-triggered runs."""
    return getpass.getuser() if trigger_source == "cli" else "api"


def build_logger_service(
    *,
    config: LoggingConfig | None = None,
    storage: StorageAdapter | None = None,
) -> "LoggerService":
    """Create a logger service using either the supplied or default adapter."""
    return LoggerService(config=config, storage=storage)


class LoggerService:
    """Main entry point for transaction, HTTP, and run logging."""

    def __init__(
        self,
        *,
        config: LoggingConfig | None = None,
        storage: StorageAdapter | None = None,
    ) -> None:
        self.config = config or LoggingConfig()
        self.storage = storage or SQLServerAdapter(config=self.config)
        self._run_states: dict[str, RunState] = {}

    def new_context(
        self,
        *,
        correlation_id: str | None = None,
        request_id: str | None = None,
        source_system: str | None = None,
        script_name: str | None = None,
        actor: str | None = None,
        environment: str | None = None,
        robot_id: str | None = None,
        run_id: str | None = None,
    ) -> LogContext:
        """Create a shared logging context for one flow."""
        return LogContext(
            correlation_id=correlation_id or generate_correlation_id(),
            request_id=request_id or generate_request_id(),
            source_system=source_system or self.config.default_source_system,
            script_name=script_name or self.config.default_script_name,
            actor=actor,
            environment=environment or self.config.default_environment,
            robot_id=robot_id,
            run_id=run_id,
        )

    def log_transaction(
        self,
        type: str,
        correlation_id: str,
        *,
        code: str | int | None,
        message: str | None = None,
        payload: Any = None,
        context: LogContext | None = None,
        timestamp: Any = None,
        level: str = "info",
    ) -> int | str | None:
        """Persist a normalized transaction entry."""
        normalized_level = normalize_log_level(level)
        entry = TransactionLog(
            type=type,
            correlation_id=correlation_id,
            code=str(code if code is not None else "UNKNOWN"),
            timestamp=coerce_timestamp(timestamp) or utc_now(),
            message=message,
            source_system=context.source_system if context else self.config.default_source_system,
            script_name=context.script_name if context else self.config.default_script_name,
            payload_json=serialize_payload(payload, max_length=self.config.max_payload_length),
            request_id=context.request_id if context else None,
            run_id=context.run_id if context else None,
            level=normalized_level,
        )
        return self.storage.log_transaction(entry)

    def log_request_success(
        self,
        transaction_type: str,
        correlation_id: str,
        *,
        response: Any = None,
        status_code: int | str | None = None,
        method: str | None = None,
        url: str | None = None,
        request_headers: dict[str, Any] | None = None,
        request_params: dict[str, Any] | None = None,
        request_body: Any = None,
        response_body: Any = None,
        duration_ms: int | None = None,
        context: LogContext | None = None,
        message: str | None = None,
    ) -> int | str | None:
        """Persist a successful HTTP-oriented transaction log."""
        entry = self._build_http_entry(
            transaction_type=transaction_type,
            correlation_id=correlation_id,
            success=True,
            response=response,
            status_code=status_code,
            method=method,
            url=url,
            request_headers=request_headers,
            request_params=request_params,
            request_body=request_body,
            response_body=response_body,
            duration_ms=duration_ms,
            context=context,
            message=message,
        )
        return self.log_transaction(
            transaction_type,
            correlation_id,
            code=entry.derived_error_code or entry.status_code or "200",
            message=entry.message,
            payload=entry.__dict__,
            context=context,
        )

    def log_request_failure(
        self,
        transaction_type: str,
        correlation_id: str,
        *,
        response: Any = None,
        code: str | int | None = None,
        method: str | None = None,
        url: str | None = None,
        request_headers: dict[str, Any] | None = None,
        request_params: dict[str, Any] | None = None,
        request_body: Any = None,
        response_body: Any = None,
        duration_ms: int | None = None,
        context: LogContext | None = None,
        message: str | None = None,
    ) -> int | str | None:
        """Persist a failed HTTP-oriented transaction log."""
        entry = self._build_http_entry(
            transaction_type=transaction_type,
            correlation_id=correlation_id,
            success=False,
            response=response,
            status_code=code,
            method=method,
            url=url,
            request_headers=request_headers,
            request_params=request_params,
            request_body=request_body,
            response_body=response_body,
            duration_ms=duration_ms,
            context=context,
            message=message,
        )
        resolved_code = str(code) if code is not None else (entry.derived_error_code or entry.status_code or "UNKNOWN")
        return self.log_transaction(
            transaction_type,
            correlation_id,
            code=resolved_code,
            message=entry.message,
            payload=entry.__dict__,
            context=context,
            level="error",
        )

    def log_request_exception(
        self,
        transaction_type: str,
        correlation_id: str,
        exc: Exception,
        *,
        response: Any = None,
        default_code: str = "400",
        method: str | None = None,
        url: str | None = None,
        request_headers: dict[str, Any] | None = None,
        request_params: dict[str, Any] | None = None,
        request_body: Any = None,
        duration_ms: int | None = None,
        context: LogContext | None = None,
        message: str | None = None,
    ) -> int | str | None:
        """Persist an exception boundary transaction log."""
        response_obj = response or getattr(exc, "response", None)
        resolved_code = extract_error_code(response_obj)
        if resolved_code == "UNKNOWN":
            resolved_code = default_code
        entry = self._build_http_entry(
            transaction_type=transaction_type,
            correlation_id=correlation_id,
            success=False,
            response=response_obj,
            status_code=resolved_code,
            method=method,
            url=url,
            request_headers=request_headers,
            request_params=request_params,
            request_body=request_body,
            response_body=extract_response_body(response_obj, self.config.max_payload_length),
            duration_ms=duration_ms,
            context=context,
            message=message or str(exc),
        )
        payload = dict(entry.__dict__)
        payload["exception"] = str(exc)
        return self.log_transaction(
            transaction_type,
            correlation_id,
            code=resolved_code,
            message=entry.message,
            payload=payload,
            context=context,
            level="error",
        )

    def create_run(
        self,
        *,
        robot_id: str,
        trigger_source: str,
        triggered_by: str | None = None,
        input_params: dict[str, Any] | None = None,
        retry_of_run_id: str | None = None,
        correlation_id: str | None = None,
        context: LogContext | None = None,
    ) -> "RunHandle":
        """Create a queued run and return a handle for later lifecycle updates."""
        now = utc_now()
        effective_correlation_id = correlation_id or (context.correlation_id if context else generate_correlation_id())
        definition = self._resolve_robot_definition(robot_id, context, trigger_source)
        run = RunLog(
            run_id=generate_correlation_id(),
            robot_id=robot_id,
            status="queued",
            trigger_source=(trigger_source or "manual").strip().lower(),
            triggered_by=triggered_by or default_triggered_by((trigger_source or "manual").strip().lower()),
            input_params_json=serialize_payload(input_params, max_length=self.config.max_payload_length),
            retry_of_run_id=retry_of_run_id,
            correlation_id=effective_correlation_id,
            queued_at=now,
            last_heartbeat_at=now,
            host_name=socket.gethostname(),
            process_id=os.getpid(),
            created_at=now,
            updated_at=now,
            robot_definition=definition,
        )
        self.storage.create_run(run)
        state = RunState(run=run)
        self._run_states[run.run_id] = state
        self.add_run_event(
            run.run_id,
            "queued",
            message="Run queued",
            payload={
                "input_params": input_params,
                "trigger_source": run.trigger_source,
            },
            timestamp=now,
        )
        return RunHandle(service=self, run_id=run.run_id)

    def bind_job(self, run_id: str, job_id: str | None) -> None:
        """Attach an API job id to a durable run."""
        self.update_run(run_id, job_id=job_id)

    def mark_started(self, run_id: str, *, message: str | None = None) -> None:
        """Mark a run as running."""
        now = utc_now()
        self.update_run(run_id, status="running", started_at=now, last_heartbeat_at=now)
        self.add_run_event(run_id, "started", message=message or "Run started", timestamp=now)

    def heartbeat(self, run_id: str, *, message: str | None = None, payload: dict[str, Any] | None = None) -> None:
        """Persist a heartbeat for an active run."""
        now = utc_now()
        self.update_run(run_id, last_heartbeat_at=now)
        self.add_run_event(run_id, "heartbeat", message=message or "Heartbeat", payload=payload, timestamp=now)

    def progress(
        self,
        run_id: str,
        *,
        current: int | None = None,
        total: int | None = None,
        unit: str | None = None,
        message: str | None = None,
        records_processed: int | None = None,
        records_succeeded: int | None = None,
        records_failed: int | None = None,
    ) -> None:
        """Persist progress counters and emit a progress event."""
        now = utc_now()
        percent = None
        if total and total > 0 and current is not None:
            percent = round((current / total) * 100, 2)
        processed = records_processed if records_processed is not None else current
        self.update_run(
            run_id,
            progress_current=current,
            progress_total=total,
            progress_percent=percent,
            progress_unit=unit,
            progress_message=message,
            last_heartbeat_at=now,
            records_processed=processed,
            records_succeeded=records_succeeded,
            records_failed=records_failed,
        )
        self.add_run_event(
            run_id,
            "progress",
            message=message or "Progress updated",
            payload={
                "current": current,
                "total": total,
                "percent": percent,
                "unit": unit,
                "records_processed": processed,
                "records_succeeded": records_succeeded,
                "records_failed": records_failed,
            },
            timestamp=now,
        )

    def mark_persisting(self, run_id: str, message: str = "Persisting records") -> None:
        """Emit a persistence stage event for a run."""
        now = utc_now()
        self.update_run(run_id, progress_message=message, last_heartbeat_at=now)
        self.add_run_event(run_id, "persisting", message=message, timestamp=now)

    def mark_completed(
        self,
        run_id: str,
        *,
        message: str | None = None,
        correlation_id: str | None = None,
        latest_transaction_id: int | str | None = None,
        records_processed: int | None = None,
        records_succeeded: int | None = None,
        records_failed: int | None = None,
    ) -> None:
        """Mark a run as completed."""
        now = utc_now()
        state = self._run_states.get(run_id)
        started_at = state.run.started_at if state else None
        duration_ms = self._duration_ms(started_at, now)
        updates: dict[str, Any] = {
            "status": "succeeded",
            "finished_at": now,
            "duration_ms": duration_ms,
            "last_heartbeat_at": now,
            "progress_message": message or "Completed",
        }
        if records_processed is not None:
            updates["records_processed"] = records_processed
            updates["progress_percent"] = 100
        if records_succeeded is not None:
            updates["records_succeeded"] = records_succeeded
        if records_failed is not None:
            updates["records_failed"] = records_failed
        if correlation_id is not None:
            updates["correlation_id"] = correlation_id
        if latest_transaction_id is not None:
            updates["latest_transaction_id"] = latest_transaction_id
        self.update_run(run_id, **updates)
        self.add_run_event(
            run_id,
            "completed",
            message=message or "Run completed",
            payload={
                "duration_ms": duration_ms,
                "correlation_id": correlation_id,
                "latest_transaction_id": latest_transaction_id,
            },
            timestamp=now,
        )

    def mark_failed(
        self,
        run_id: str,
        *,
        error_message: str,
        failure_category: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        """Mark a run as failed."""
        now = utc_now()
        state = self._run_states.get(run_id)
        started_at = state.run.started_at if state else None
        duration_ms = self._duration_ms(started_at, now)
        self.update_run(
            run_id,
            status="failed",
            finished_at=now,
            duration_ms=duration_ms,
            last_heartbeat_at=now,
            error_message=error_message,
            failure_category=failure_category,
            correlation_id=correlation_id,
        )
        self.add_run_event(
            run_id,
            "failed",
            message=error_message,
            payload={"failure_category": failure_category, "duration_ms": duration_ms},
            timestamp=now,
        )

    def mark_cancel_requested(self, run_id: str, *, message: str = "Cancellation requested by user") -> None:
        """Mark a run as waiting for cancellation."""
        now = utc_now()
        self.update_run(run_id, status="cancelling", progress_message=message, last_heartbeat_at=now)
        self.add_run_event(run_id, "cancel_requested", message=message, timestamp=now)

    def mark_cancelled(self, run_id: str, *, message: str = "Cancelled by user") -> None:
        """Mark a run as cancelled."""
        now = utc_now()
        state = self._run_states.get(run_id)
        started_at = state.run.started_at if state else None
        duration_ms = self._duration_ms(started_at, now)
        self.update_run(
            run_id,
            status="cancelled",
            finished_at=now,
            duration_ms=duration_ms,
            last_heartbeat_at=now,
            error_message=message,
        )
        self.add_run_event(
            run_id,
            "cancelled",
            message=message,
            payload={"duration_ms": duration_ms},
            timestamp=now,
        )

    def attach_transaction(
        self,
        run_id: str,
        *,
        correlation_id: str | None = None,
        latest_transaction_id: int | str | None = None,
    ) -> None:
        """Attach transaction metadata to a durable run."""
        self.update_run(
            run_id,
            correlation_id=correlation_id,
            latest_transaction_id=latest_transaction_id,
        )

    def update_run(self, run_id: str, **fields: Any) -> None:
        """Apply a partial run update through the active storage adapter."""
        if not fields:
            return
        normalized_fields = dict(fields)
        if "status" in normalized_fields and normalized_fields["status"] is not None:
            normalized_fields["status"] = normalize_status(normalized_fields["status"])
        normalized_fields["updated_at"] = utc_now()
        self.storage.update_run(run_id, normalized_fields)
        state = self._run_states.get(run_id)
        if state is not None:
            state.run = replace(state.run, **{k: v for k, v in normalized_fields.items() if hasattr(state.run, k)})

    def add_run_event(
        self,
        run_id: str,
        event_type: str,
        *,
        message: str | None = None,
        payload: Any = None,
        timestamp: Any = None,
    ) -> None:
        """Append an event to a durable run."""
        entry = RunEventLog(
            run_id=run_id,
            timestamp=coerce_timestamp(timestamp) or utc_now(),
            event_type=normalize_event_type(event_type),
            message=message,
            payload_json=serialize_payload(payload, max_length=self.config.max_payload_length),
        )
        self.storage.add_event(entry)
        state = self._run_states.get(run_id)
        if state is not None:
            state.events.append(entry)

    def _build_http_entry(
        self,
        *,
        transaction_type: str,
        correlation_id: str,
        success: bool,
        response: Any,
        status_code: int | str | None,
        method: str | None,
        url: str | None,
        request_headers: dict[str, Any] | None,
        request_params: dict[str, Any] | None,
        request_body: Any,
        response_body: Any,
        duration_ms: int | None,
        context: LogContext | None,
        message: str | None,
    ) -> HttpLogEntry:
        derived_error_code = None
        if response is not None:
            derived_error_code = extract_error_code(response)
        resolved_status = status_code if status_code is not None else getattr(response, "status_code", None)
        resolved_response_body = response_body
        if resolved_response_body is None and self.config.capture_response_body:
            resolved_response_body = extract_response_body(response, self.config.max_payload_length)
        resolved_request_body = request_body
        if not self.config.capture_request_body:
            resolved_request_body = None
        return HttpLogEntry(
            method=method,
            url=url,
            status_code=resolved_status,
            derived_error_code=derived_error_code,
            request_headers=request_headers,
            request_params=request_params,
            request_body=resolved_request_body,
            response_body=resolved_response_body,
            duration_ms=duration_ms,
            success=success,
            correlation_id=correlation_id,
            request_id=context.request_id if context else None,
            source_system=context.source_system if context else self.config.default_source_system,
            script_name=context.script_name if context else self.config.default_script_name,
            transaction_type=transaction_type,
            message=message,
        )

    def _resolve_robot_definition(
        self,
        robot_id: str,
        context: LogContext | None,
        trigger_source: str,
    ) -> RobotDefinition:
        definition = ROBOT_DEFINITIONS.get(robot_id)
        if definition:
            return definition
        source_system = (
            context.source_system if context and context.source_system else self.config.default_source_system or "unknown"
        )
        return RobotDefinition(
            robot_id=robot_id,
            display_name=robot_id.replace("_", " ").title(),
            source_system=source_system,
            run_scope=(trigger_source or "custom").strip().lower(),
            description=f"Run definition for {robot_id}.",
        )

    @staticmethod
    def _duration_ms(started_at: Any, finished_at: Any) -> int | None:
        start = coerce_timestamp(started_at)
        end = coerce_timestamp(finished_at)
        if start is None or end is None:
            return None
        return int((end - start).total_seconds() * 1000)


class RunHandle:
    """Convenience handle for scripts updating one run."""

    def __init__(self, *, service: LoggerService, run_id: str) -> None:
        self.service = service
        self.run_id = run_id

    @property
    def _state(self) -> RunState | None:
        return self.service._run_states.get(self.run_id)

    @property
    def robot_id(self) -> str | None:
        return self._state.run.robot_id if self._state else None

    def bind_job(self, job_id: str | None) -> None:
        self.service.bind_job(self.run_id, job_id)

    def queued(self) -> None:
        self.service.heartbeat(self.run_id, message="Run is queued")

    def started(self, *, message: str | None = None) -> None:
        self.service.mark_started(self.run_id, message=message)

    def heartbeat(self, *, message: str | None = None, payload: dict[str, Any] | None = None) -> None:
        self.service.heartbeat(self.run_id, message=message, payload=payload)

    def progress(
        self,
        current: int | None = None,
        total: int | None = None,
        unit: str | None = None,
        message: str | None = None,
        records_processed: int | None = None,
        records_succeeded: int | None = None,
        records_failed: int | None = None,
    ) -> None:
        self.service.progress(
            self.run_id,
            current=current,
            total=total,
            unit=unit,
            message=message,
            records_processed=records_processed,
            records_succeeded=records_succeeded,
            records_failed=records_failed,
        )

    def persisting(self, message: str = "Persisting records") -> None:
        self.service.mark_persisting(self.run_id, message=message)

    def completed(
        self,
        *,
        message: str | None = None,
        correlation_id: str | None = None,
        latest_transaction_id: int | str | None = None,
        records_processed: int | None = None,
        records_succeeded: int | None = None,
        records_failed: int | None = None,
    ) -> None:
        self.service.mark_completed(
            self.run_id,
            message=message,
            correlation_id=correlation_id,
            latest_transaction_id=latest_transaction_id,
            records_processed=records_processed,
            records_succeeded=records_succeeded,
            records_failed=records_failed,
        )

    def failed(
        self,
        *,
        error_message: str,
        failure_category: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        self.service.mark_failed(
            self.run_id,
            error_message=error_message,
            failure_category=failure_category,
            correlation_id=correlation_id,
        )

    def cancel_requested(self, message: str = "Cancellation requested by user") -> None:
        self.service.mark_cancel_requested(self.run_id, message=message)

    def cancelled(self, message: str = "Cancelled by user") -> None:
        self.service.mark_cancelled(self.run_id, message=message)

    def attach_transaction(
        self,
        *,
        correlation_id: str | None = None,
        latest_transaction_id: int | str | None = None,
    ) -> None:
        self.service.attach_transaction(
            self.run_id,
            correlation_id=correlation_id,
            latest_transaction_id=latest_transaction_id,
        )
