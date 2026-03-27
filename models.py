"""Typed models and constants for shared logging workflows."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


LOG_LEVELS = {"debug", "info", "warning", "error", "critical"}

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


@dataclass(frozen=True)
class LogContext:
    """Shared metadata that ties related logs together."""

    correlation_id: str
    request_id: str | None = None
    source_system: str | None = None
    script_name: str | None = None
    actor: str | None = None
    environment: str | None = None
    robot_id: str | None = None
    run_id: str | None = None


@dataclass(frozen=True)
class TransactionLog:
    """Normalized transaction log entry."""

    type: str
    correlation_id: str
    code: str
    timestamp: datetime
    message: str | None = None
    source_system: str | None = None
    script_name: str | None = None
    payload_json: str | None = None
    request_id: str | None = None
    run_id: str | None = None
    level: str = "info"


@dataclass(frozen=True)
class HttpLogEntry:
    """Structured summary of one HTTP request outcome."""

    method: str | None
    url: str | None
    status_code: int | str | None
    derived_error_code: str | None
    request_headers: dict[str, Any] | None = None
    request_params: dict[str, Any] | None = None
    request_body: Any = None
    response_body: Any = None
    duration_ms: int | None = None
    success: bool = True
    correlation_id: str | None = None
    request_id: str | None = None
    source_system: str | None = None
    script_name: str | None = None
    transaction_type: str | None = None
    message: str | None = None


@dataclass(frozen=True)
class RunLog:
    """Normalized durable run state."""

    run_id: str
    robot_id: str
    status: str
    trigger_source: str
    triggered_by: str | None = None
    input_params_json: str | None = None
    retry_of_run_id: str | None = None
    correlation_id: str | None = None
    queued_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_ms: int | None = None
    last_heartbeat_at: datetime | None = None
    progress_current: int | None = None
    progress_total: int | None = None
    progress_percent: float | None = None
    progress_unit: str | None = None
    progress_message: str | None = None
    records_processed: int | None = None
    records_succeeded: int | None = None
    records_failed: int | None = None
    error_message: str | None = None
    failure_category: str | None = None
    latest_transaction_id: int | str | None = None
    host_name: str | None = None
    process_id: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    job_id: str | None = None
    robot_definition: RobotDefinition | None = None


@dataclass(frozen=True)
class RunEventLog:
    """Structured append-only event for a durable run."""

    run_id: str
    timestamp: datetime
    event_type: str
    message: str | None = None
    payload_json: str | None = None


@dataclass
class RunState:
    """Mutable in-process view of a run used by `RunHandle`."""

    run: RunLog
    events: list[RunEventLog] = field(default_factory=list)
