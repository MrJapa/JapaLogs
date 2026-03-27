"""Configuration models for the shared logging package."""

from __future__ import annotations

from dataclasses import dataclass, field


def _default_transaction_columns() -> dict[str, str]:
    return {
        "id": "Id",
        "type": "Type",
        "correlation_id": "Transaktion",
        "timestamp": "Timestamp",
        "code": "Code",
    }


def _default_robot_columns() -> dict[str, str]:
    return {
        "id": "Id",
        "display_name": "DisplayName",
        "source_system": "SourceSystem",
        "run_scope": "RunScope",
        "description": "Description",
        "is_active": "IsActive",
        "created_at": "CreatedAt",
        "updated_at": "UpdatedAt",
    }


def _default_run_columns() -> dict[str, str]:
    return {
        "id": "Id",
        "robot_id": "Robot_Id",
        "job_id": "JobId",
        "retry_of_run_id": "RetryOfRun_Id",
        "status": "Status",
        "queued_at": "QueuedAt",
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
        "trigger_source": "TriggerSource",
        "triggered_by": "TriggeredBy",
        "input_params_json": "InputParamsJson",
        "correlation_id": "CorrelationId",
        "latest_transaction_id": "LatestTransactionId",
        "host_name": "HostName",
        "process_id": "ProcessId",
        "created_at": "CreatedAt",
        "updated_at": "UpdatedAt",
    }


def _default_event_columns() -> dict[str, str]:
    return {
        "id": "Id",
        "run_id": "Run_Id",
        "timestamp": "Timestamp",
        "event_type": "EventType",
        "message": "Message",
        "payload_json": "PayloadJson",
    }


@dataclass(frozen=True)
class SchemaMapping:
    """Configurable database objects and column mappings for persisted logs."""

    transaction_table: str = "api.Transaktion"
    transaction_columns: dict[str, str] = field(default_factory=_default_transaction_columns)
    robot_table: str = "api.Robot"
    robot_columns: dict[str, str] = field(default_factory=_default_robot_columns)
    run_table: str = "api.RobotRun"
    run_columns: dict[str, str] = field(default_factory=_default_run_columns)
    event_table: str = "api.RobotRunEvent"
    event_columns: dict[str, str] = field(default_factory=_default_event_columns)

    def column(self, section: str, field_name: str) -> str | None:
        """Return the mapped column name for a logical field."""
        mapping = {
            "transaction": self.transaction_columns,
            "robot": self.robot_columns,
            "run": self.run_columns,
            "event": self.event_columns,
        }[section]
        return mapping.get(field_name)


@dataclass(frozen=True)
class LoggingConfig:
    """Runtime configuration for the shared logging service."""

    storage_adapter: str = "sqlserver"
    schema_mapping: SchemaMapping = field(default_factory=SchemaMapping)
    auto_create_schema: bool = True
    capture_request_body: bool = True
    capture_response_body: bool = True
    max_payload_length: int = 4000
    default_source_system: str | None = None
    default_script_name: str | None = None
    default_environment: str | None = None
