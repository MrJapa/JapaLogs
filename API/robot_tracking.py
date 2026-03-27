"""Shared robot run tracking for durable backend observability."""

from __future__ import annotations

import getpass
import json
import os
import socket
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from Database.db import SQL


ROBOT_STATUSES = {
    "idle",
    "queued",
    "running",
    "succeeded",
    "failed",
    "cancelled",
    "stale",
    "cancelling",
}

ROBOT_EVENT_TYPES = {
    "queued",
    "started",
    "progress",
    "heartbeat",
    "persisting",
    "completed",
    "failed",
    "cancel_requested",
    "cancelled",
}


@dataclass(frozen=True)
class RobotDefinition:
    """Static metadata for a dashboard-monitored robot."""

    robot_id: str
    display_name: str
    source_system: str
    run_scope: str
    description: str


ROBOT_DEFINITIONS: dict[str, RobotDefinition] = {
    "timeview_customers_all": RobotDefinition(
        robot_id="timeview_customers_all",
        display_name="Timeview Customers - Full Sync",
        source_system="Timeview",
        run_scope="all",
        description="Synchronize all Timeview customers into SQL.",
    ),
    "timeview_customers_updates": RobotDefinition(
        robot_id="timeview_customers_updates",
        display_name="Timeview Customers - Updates",
        source_system="Timeview",
        run_scope="updates",
        description="Synchronize recently updated Timeview customers into SQL.",
    ),
    "timeview_customer_single": RobotDefinition(
        robot_id="timeview_customer_single",
        display_name="Timeview Customer - Single CVR",
        source_system="Timeview",
        run_scope="single",
        description="Synchronize one Timeview customer by CVR.",
    ),
    "cvr_all": RobotDefinition(
        robot_id="cvr_all",
        display_name="CVR - Full Sync",
        source_system="CVR",
        run_scope="all",
        description="Fetch CVR records for all customers.",
    ),
    "cvr_fiscal": RobotDefinition(
        robot_id="cvr_fiscal",
        display_name="CVR - Fiscal Year End",
        source_system="CVR",
        run_scope="fiscal",
        description="Fetch CVR records filtered by fiscal year end.",
    ),
    "cvr_single": RobotDefinition(
        robot_id="cvr_single",
        display_name="CVR - Single CVR",
        source_system="CVR",
        run_scope="single",
        description="Fetch CVR records for one CVR.",
    ),
    "dupla_token": RobotDefinition(
        robot_id="dupla_token",
        display_name="DUPLA - Token",
        source_system="DUPLA",
        run_scope="token",
        description="Request DUPLA access and refresh tokens.",
    ),
    "skattekonto_all": RobotDefinition(
        robot_id="skattekonto_all",
        display_name="Skattekonto - Full Sync",
        source_system="DUPLA",
        run_scope="all",
        description="Fetch Skattekonto fordringer for all customers.",
    ),
    "skattekonto_single": RobotDefinition(
        robot_id="skattekonto_single",
        display_name="Skattekonto - Single CVR",
        source_system="DUPLA",
        run_scope="single",
        description="Fetch Skattekonto fordringer for one CVR.",
    ),
    "kontrol_all": RobotDefinition(
        robot_id="kontrol_all",
        display_name="Kontroloplysninger - Full Sync",
        source_system="DUPLA",
        run_scope="all",
        description="Fetch kontroloplysninger for all customers.",
    ),
    "kontrol_single": RobotDefinition(
        robot_id="kontrol_single",
        display_name="Kontroloplysninger - Single CVR",
        source_system="DUPLA",
        run_scope="single",
        description="Fetch kontroloplysninger for one CVR.",
    ),
    "selskabsselvangivelse_all": RobotDefinition(
        robot_id="selskabsselvangivelse_all",
        display_name="Selskabsselvangivelse - Full Sync",
        source_system="DUPLA",
        run_scope="all",
        description="Fetch selskabsselvangivelser for all customers.",
    ),
    "selskabsselvangivelse_single": RobotDefinition(
        robot_id="selskabsselvangivelse_single",
        display_name="Selskabsselvangivelse - Single CVR",
        source_system="DUPLA",
        run_scope="single",
        description="Fetch selskabsselvangivelse for one CVR.",
    ),
}


_SCHEMA_LOCK = threading.Lock()
_SCHEMA_READY = False


def utc_now() -> datetime:
    """Return the current UTC datetime."""
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    """Return the current UTC datetime as an ISO-8601 string."""
    return utc_now().isoformat().replace("+00:00", "Z")


def _serialize(value: Any) -> Optional[str]:
    """Serialize a value to JSON for storage when present."""
    if value is None:
        return None
    return json.dumps(value, default=str, ensure_ascii=True)


def _normalize_status(status: str) -> str:
    """Validate and normalize a robot status value."""
    status_value = (status or "").strip().lower()
    if status_value not in ROBOT_STATUSES:
        raise ValueError(f"Unsupported robot status: {status}")
    return status_value


def _normalize_event_type(event_type: str) -> str:
    """Validate and normalize a robot event type."""
    event_value = (event_type or "").strip().lower()
    if event_value not in ROBOT_EVENT_TYPES:
        raise ValueError(f"Unsupported robot event type: {event_type}")
    return event_value


def _coerce_timestamp(value: datetime | str | None) -> Optional[datetime]:
    """Convert strings or datetimes into UTC-aware datetimes."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        cleaned = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(cleaned)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    raise TypeError(f"Unsupported timestamp value: {type(value)!r}")


class RobotRunTracker:
    """Persist durable robot metadata and lifecycle events in SQL Server."""

    def ensure_schema(self) -> None:
        """Create dashboard tracking tables the first time they are needed."""
        global _SCHEMA_READY
        if _SCHEMA_READY:
            return

        with _SCHEMA_LOCK:
            if _SCHEMA_READY:
                return

            ddl = """
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'api' AND t.name = 'Robot')
BEGIN
    CREATE TABLE [api].[Robot](
        [Id] NVARCHAR(100) NOT NULL PRIMARY KEY,
        [DisplayName] NVARCHAR(255) NOT NULL,
        [SourceSystem] NVARCHAR(100) NOT NULL,
        [RunScope] NVARCHAR(50) NOT NULL,
        [Description] NVARCHAR(1000) NULL,
        [IsActive] BIT NOT NULL CONSTRAINT [DF_api_Robot_IsActive] DEFAULT (1),
        [CreatedAt] DATETIME2(0) NOT NULL CONSTRAINT [DF_api_Robot_CreatedAt] DEFAULT SYSUTCDATETIME(),
        [UpdatedAt] DATETIME2(0) NOT NULL CONSTRAINT [DF_api_Robot_UpdatedAt] DEFAULT SYSUTCDATETIME()
    );
END;

IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'api' AND t.name = 'RobotRun')
BEGIN
    CREATE TABLE [api].[RobotRun](
        [Id] NVARCHAR(36) NOT NULL PRIMARY KEY,
        [Robot_Id] NVARCHAR(100) NOT NULL,
        [JobId] NVARCHAR(36) NULL,
        [RetryOfRun_Id] NVARCHAR(36) NULL,
        [Status] NVARCHAR(20) NOT NULL,
        [QueuedAt] DATETIME2(0) NULL,
        [StartedAt] DATETIME2(0) NULL,
        [FinishedAt] DATETIME2(0) NULL,
        [DurationMs] BIGINT NULL,
        [LastHeartbeatAt] DATETIME2(0) NULL,
        [ProgressCurrent] INT NULL,
        [ProgressTotal] INT NULL,
        [ProgressPercent] DECIMAL(5,2) NULL,
        [ProgressUnit] NVARCHAR(50) NULL,
        [ProgressMessage] NVARCHAR(1000) NULL,
        [RecordsProcessed] INT NULL,
        [RecordsSucceeded] INT NULL,
        [RecordsFailed] INT NULL,
        [ErrorMessage] NVARCHAR(MAX) NULL,
        [FailureCategory] NVARCHAR(100) NULL,
        [TriggerSource] NVARCHAR(20) NOT NULL,
        [TriggeredBy] NVARCHAR(255) NULL,
        [InputParamsJson] NVARCHAR(MAX) NULL,
        [CorrelationId] NVARCHAR(100) NULL,
        [LatestTransactionId] INT NULL,
        [HostName] NVARCHAR(255) NULL,
        [ProcessId] INT NULL,
        [CreatedAt] DATETIME2(0) NOT NULL CONSTRAINT [DF_api_RobotRun_CreatedAt] DEFAULT SYSUTCDATETIME(),
        [UpdatedAt] DATETIME2(0) NOT NULL CONSTRAINT [DF_api_RobotRun_UpdatedAt] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [FK_api_RobotRun_Robot] FOREIGN KEY([Robot_Id]) REFERENCES [api].[Robot]([Id]),
        CONSTRAINT [FK_api_RobotRun_Retry] FOREIGN KEY([RetryOfRun_Id]) REFERENCES [api].[RobotRun]([Id]),
        CONSTRAINT [FK_api_RobotRun_Transaktion] FOREIGN KEY([LatestTransactionId]) REFERENCES [api].[Transaktion]([Id])
    );
    CREATE INDEX [IX_api_RobotRun_Robot_Status] ON [api].[RobotRun]([Robot_Id], [Status], [QueuedAt] DESC);
    CREATE INDEX [IX_api_RobotRun_JobId] ON [api].[RobotRun]([JobId]);
END;

IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'api' AND t.name = 'RobotRunEvent')
BEGIN
    CREATE TABLE [api].[RobotRunEvent](
        [Id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        [Run_Id] NVARCHAR(36) NOT NULL,
        [Timestamp] DATETIME2(0) NOT NULL,
        [EventType] NVARCHAR(30) NOT NULL,
        [Message] NVARCHAR(1000) NULL,
        [PayloadJson] NVARCHAR(MAX) NULL,
        CONSTRAINT [FK_api_RobotRunEvent_Run] FOREIGN KEY([Run_Id]) REFERENCES [api].[RobotRun]([Id])
    );
    CREATE INDEX [IX_api_RobotRunEvent_Run_Timestamp] ON [api].[RobotRunEvent]([Run_Id], [Timestamp] DESC);
END;
"""

            with SQL() as db:
                db.execute(ddl, commit=True)

            self._sync_robot_definitions()
            _SCHEMA_READY = True

    def _sync_robot_definitions(self) -> None:
        """Upsert static robot metadata into the dashboard tables."""
        sql = """
IF EXISTS (SELECT 1 FROM [api].[Robot] WHERE [Id] = ?)
BEGIN
    UPDATE [api].[Robot]
    SET [DisplayName] = ?, [SourceSystem] = ?, [RunScope] = ?, [Description] = ?, [IsActive] = 1, [UpdatedAt] = SYSUTCDATETIME()
    WHERE [Id] = ?;
END
ELSE
BEGIN
    INSERT INTO [api].[Robot]([Id], [DisplayName], [SourceSystem], [RunScope], [Description], [IsActive], [CreatedAt], [UpdatedAt])
    VALUES (?, ?, ?, ?, ?, 1, SYSUTCDATETIME(), SYSUTCDATETIME());
END;
"""
        with SQL() as db:
            for robot in ROBOT_DEFINITIONS.values():
                params = (
                    robot.robot_id,
                    robot.display_name,
                    robot.source_system,
                    robot.run_scope,
                    robot.description,
                    robot.robot_id,
                    robot.robot_id,
                    robot.display_name,
                    robot.source_system,
                    robot.run_scope,
                    robot.description,
                )
                db.execute(sql, params, commit=False)
            db.connection.commit()

    def create_run(
        self,
        *,
        robot_id: str,
        trigger_source: str,
        triggered_by: Optional[str] = None,
        input_params: Optional[dict[str, Any]] = None,
        retry_of_run_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ) -> "RobotRunHandle":
        """Create a queued robot run and return a handle for subsequent updates."""
        self.ensure_schema()
        if robot_id not in ROBOT_DEFINITIONS:
            raise ValueError(f"Unknown robot id: {robot_id}")

        now = utc_now()
        run_id = str(uuid.uuid4())
        trigger_value = (trigger_source or "manual").strip().lower()
        params_json = _serialize(input_params)
        host_name = socket.gethostname()
        process_id = os.getpid()

        insert_sql = """
INSERT INTO [api].[RobotRun](
    [Id], [Robot_Id], [RetryOfRun_Id], [Status], [QueuedAt], [LastHeartbeatAt],
    [TriggerSource], [TriggeredBy], [InputParamsJson], [CorrelationId],
    [HostName], [ProcessId], [CreatedAt], [UpdatedAt]
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""
        with SQL() as db:
            db.execute(
                insert_sql,
                (
                    run_id,
                    robot_id,
                    retry_of_run_id,
                    "queued",
                    now,
                    now,
                    trigger_value,
                    triggered_by,
                    params_json,
                    correlation_id,
                    host_name,
                    process_id,
                    now,
                    now,
                ),
                commit=True,
            )

        self._insert_event(run_id, "queued", "Run queued", {"input_params": input_params, "trigger_source": trigger_value})

        return RobotRunHandle(
            tracker=self,
            run_id=run_id,
            robot_id=robot_id,
            trigger_source=trigger_value,
            triggered_by=triggered_by,
            input_params=input_params or {},
            retry_of_run_id=retry_of_run_id,
        )

    def bind_job(self, run_id: str, job_id: Optional[str]) -> None:
        """Attach an in-memory API job id to a durable robot run."""
        self.update_run(run_id, job_id=job_id)

    def mark_started(self, run_id: str, *, message: Optional[str] = None) -> None:
        """Mark a run as actively executing."""
        now = utc_now()
        self.update_run(run_id, status="running", started_at=now, last_heartbeat_at=now)
        self._insert_event(run_id, "started", message or "Run started", None, now)

    def heartbeat(self, run_id: str, *, message: Optional[str] = None, payload: Optional[dict[str, Any]] = None) -> None:
        """Persist a heartbeat for an active run."""
        now = utc_now()
        self.update_run(run_id, last_heartbeat_at=now)
        self._insert_event(run_id, "heartbeat", message or "Heartbeat", payload, now)

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
        self._insert_event(
            run_id,
            "progress",
            message or "Progress updated",
            {
                "current": current,
                "total": total,
                "percent": percent,
                "unit": unit,
                "records_processed": processed,
                "records_succeeded": records_succeeded,
                "records_failed": records_failed,
            },
            now,
        )

    def mark_persisting(self, run_id: str, message: str = "Persisting records") -> None:
        """Emit a persistence lifecycle event for long-running jobs."""
        now = utc_now()
        self.update_run(run_id, progress_message=message, last_heartbeat_at=now)
        self._insert_event(run_id, "persisting", message, None, now)

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
        """Mark a run as successfully completed."""
        finished_at = utc_now()
        started_row = self._fetch_run_row(run_id)
        started_at = started_row[0] if started_row else None
        duration_ms = self._duration_ms(started_at, finished_at)
        updates: dict[str, Any] = {
            "status": "succeeded",
            "finished_at": finished_at,
            "duration_ms": duration_ms,
            "last_heartbeat_at": finished_at,
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
        self._insert_event(
            run_id,
            "completed",
            message or "Run completed",
            {
                "duration_ms": duration_ms,
                "correlation_id": correlation_id,
                "latest_transaction_id": latest_transaction_id,
            },
            finished_at,
        )

    def mark_failed(
        self,
        run_id: str,
        *,
        error_message: str,
        failure_category: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ) -> None:
        """Mark a run as failed and store the failure details."""
        finished_at = utc_now()
        started_row = self._fetch_run_row(run_id)
        started_at = started_row[0] if started_row else None
        duration_ms = self._duration_ms(started_at, finished_at)
        self.update_run(
            run_id,
            status="failed",
            finished_at=finished_at,
            duration_ms=duration_ms,
            last_heartbeat_at=finished_at,
            error_message=error_message,
            failure_category=failure_category,
            correlation_id=correlation_id,
        )
        self._insert_event(
            run_id,
            "failed",
            error_message,
            {"failure_category": failure_category, "duration_ms": duration_ms},
            finished_at,
        )

    def mark_cancel_requested(self, run_id: str, *, message: str = "Cancellation requested by user") -> None:
        """Persist a cancellation request for a running or queued run."""
        now = utc_now()
        self.update_run(run_id, status="cancelling", progress_message=message, last_heartbeat_at=now)
        self._insert_event(run_id, "cancel_requested", message, None, now)

    def mark_cancelled(self, run_id: str, *, message: str = "Cancelled by user") -> None:
        """Mark a run as cancelled."""
        finished_at = utc_now()
        started_row = self._fetch_run_row(run_id)
        started_at = started_row[0] if started_row else None
        duration_ms = self._duration_ms(started_at, finished_at)
        self.update_run(
            run_id,
            status="cancelled",
            finished_at=finished_at,
            duration_ms=duration_ms,
            last_heartbeat_at=finished_at,
            error_message=message,
        )
        self._insert_event(run_id, "cancelled", message, {"duration_ms": duration_ms}, finished_at)

    def attach_transaction(self, run_id: str, *, correlation_id: Optional[str] = None, latest_transaction_id: Optional[int] = None) -> None:
        """Attach low-level transaction metadata to a durable run."""
        self.update_run(
            run_id,
            correlation_id=correlation_id,
            latest_transaction_id=latest_transaction_id,
        )

    def update_run(self, run_id: str, **fields: Any) -> None:
        """Apply partial updates to a robot run row."""
        if not fields:
            return

        column_map = {
            "job_id": "JobId",
            "status": "Status",
            "started_at": "StartedAt",
            "finished_at": "FinishedAt",
            "duration_ms": "DurationMs",
            "last_heartbeat_at": "LastHeartbeatAt",
            "progress_current": "ProgressCurrent",
            "progress_total": "ProgressTotal",
            "progress_percent": "ProgressPercent",
            "progress_unit": "ProgressUnit",
            "progress_message": "ProgressMessage",
            "records_processed": "RecordsProcessed",
            "records_succeeded": "RecordsSucceeded",
            "records_failed": "RecordsFailed",
            "error_message": "ErrorMessage",
            "failure_category": "FailureCategory",
            "correlation_id": "CorrelationId",
            "latest_transaction_id": "LatestTransactionId",
            "input_params": "InputParamsJson",
            "retry_of_run_id": "RetryOfRun_Id",
        }

        assignments = []
        params: list[Any] = []

        for key, value in fields.items():
            if key not in column_map:
                continue
            if key == "status" and value is not None:
                value = _normalize_status(value)
            if key == "input_params":
                value = _serialize(value)
            if key.endswith("_at") and value is not None:
                value = _coerce_timestamp(value)
            assignments.append(f"[{column_map[key]}] = ?")
            params.append(value)

        if not assignments:
            return

        assignments.append("[UpdatedAt] = ?")
        params.append(utc_now())
        params.append(run_id)

        sql = f"UPDATE [api].[RobotRun] SET {', '.join(assignments)} WHERE [Id] = ?;"
        with SQL() as db:
            db.execute(sql, tuple(params), commit=True)

    def _insert_event(
        self,
        run_id: str,
        event_type: str,
        message: Optional[str],
        payload: Optional[dict[str, Any]],
        timestamp: Optional[datetime] = None,
    ) -> None:
        """Insert an append-only run event."""
        sql = """
INSERT INTO [api].[RobotRunEvent]([Run_Id], [Timestamp], [EventType], [Message], [PayloadJson])
VALUES (?, ?, ?, ?, ?);
"""
        event_time = _coerce_timestamp(timestamp) or utc_now()
        with SQL() as db:
            db.execute(
                sql,
                (
                    run_id,
                    event_time,
                    _normalize_event_type(event_type),
                    message,
                    _serialize(payload),
                ),
                commit=True,
            )

    def _fetch_run_row(self, run_id: str):
        """Fetch a subset of run timing columns for duration calculations."""
        sql = "SELECT [StartedAt] FROM [api].[RobotRun] WHERE [Id] = ?;"
        with SQL() as db:
            return db.fetch_one(sql, (run_id,))

    @staticmethod
    def _duration_ms(started_at: datetime | None, finished_at: datetime) -> Optional[int]:
        """Compute duration in milliseconds when a start time exists."""
        start_ts = _coerce_timestamp(started_at)
        end_ts = _coerce_timestamp(finished_at)
        if start_ts is None or end_ts is None:
            return None
        return int((end_ts - start_ts).total_seconds() * 1000)


@dataclass
class RobotRunHandle:
    """Convenience handle for orchestrators updating a single run."""

    tracker: RobotRunTracker
    run_id: str
    robot_id: str
    trigger_source: str
    triggered_by: Optional[str]
    input_params: dict[str, Any]
    retry_of_run_id: Optional[str] = None

    def bind_job(self, job_id: Optional[str]) -> None:
        """Attach an API job id to the run."""
        self.tracker.bind_job(self.run_id, job_id)

    def queued(self) -> None:
        """Emit a heartbeat while the job remains queued."""
        self.tracker.heartbeat(self.run_id, message="Run is queued")

    def started(self, *, message: Optional[str] = None) -> None:
        """Mark the run as started."""
        self.tracker.mark_started(self.run_id, message=message)

    def heartbeat(self, *, message: Optional[str] = None, payload: Optional[dict[str, Any]] = None) -> None:
        """Record a heartbeat for the run."""
        self.tracker.heartbeat(self.run_id, message=message, payload=payload)

    def progress(
        self,
        current: Optional[int] = None,
        total: Optional[int] = None,
        unit: Optional[str] = None,
        message: Optional[str] = None,
        records_processed: Optional[int] = None,
        records_succeeded: Optional[int] = None,
        records_failed: Optional[int] = None,
    ) -> None:
        """Update durable progress counters for the run."""
        self.tracker.progress(
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
        """Record a persistence stage event."""
        self.tracker.mark_persisting(self.run_id, message=message)

    def completed(
        self,
        *,
        message: Optional[str] = None,
        correlation_id: Optional[str] = None,
        latest_transaction_id: Optional[int] = None,
        records_processed: Optional[int] = None,
        records_succeeded: Optional[int] = None,
        records_failed: Optional[int] = None,
    ) -> None:
        """Mark the run as completed."""
        self.tracker.mark_completed(
            self.run_id,
            message=message,
            correlation_id=correlation_id,
            latest_transaction_id=latest_transaction_id,
            records_processed=records_processed,
            records_succeeded=records_succeeded,
            records_failed=records_failed,
        )

    def failed(self, *, error_message: str, failure_category: Optional[str] = None, correlation_id: Optional[str] = None) -> None:
        """Mark the run as failed."""
        self.tracker.mark_failed(
            self.run_id,
            error_message=error_message,
            failure_category=failure_category,
            correlation_id=correlation_id,
        )

    def cancel_requested(self, message: str = "Cancellation requested by user") -> None:
        """Mark the run as waiting for cancellation."""
        self.tracker.mark_cancel_requested(self.run_id, message=message)

    def cancelled(self, message: str = "Cancelled by user") -> None:
        """Mark the run as cancelled."""
        self.tracker.mark_cancelled(self.run_id, message=message)

    def attach_transaction(self, *, correlation_id: Optional[str] = None, latest_transaction_id: Optional[int] = None) -> None:
        """Attach related transaction metadata to the run."""
        self.tracker.attach_transaction(
            self.run_id,
            correlation_id=correlation_id,
            latest_transaction_id=latest_transaction_id,
        )


def default_triggered_by(trigger_source: str) -> str:
    """Best-effort actor identifier for backend-triggered runs."""
    if trigger_source == "cli":
        return getpass.getuser()
    return "api"
