"""Storage adapters for the shared logging package."""

from __future__ import annotations

import re
import threading
from abc import ABC, abstractmethod
from dataclasses import asdict
from typing import Any, Callable

try:
    from .config import LoggingConfig
    from .models import RobotDefinition, RunEventLog, RunLog, TransactionLog
except ImportError:
    from config import LoggingConfig
    from models import RobotDefinition, RunEventLog, RunLog, TransactionLog


def default_sql_factory():
    """Lazily import the repo SQL helper to avoid hard dependency at import time."""
    from Database.db import SQL

    return SQL()


def _quote_identifier(identifier: str) -> str:
    return f"[{identifier}]"


def _quote_table(table_name: str) -> str:
    return ".".join(_quote_identifier(part) for part in table_name.split("."))


def _split_table(table_name: str) -> tuple[str, str]:
    parts = table_name.split(".")
    if len(parts) == 1:
        return "dbo", parts[0]
    return parts[0], parts[-1]


def _safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", value)


class StorageAdapter(ABC):
    """Storage contract for persisted logging backends."""

    @abstractmethod
    def log_transaction(self, entry: TransactionLog) -> int | str | None:
        raise NotImplementedError

    @abstractmethod
    def create_run(self, run: RunLog) -> str:
        raise NotImplementedError

    @abstractmethod
    def update_run(self, run_id: str, fields: dict[str, Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def add_event(self, event: RunEventLog) -> None:
        raise NotImplementedError

    @abstractmethod
    def ensure_schema(self) -> None:
        raise NotImplementedError


class SQLServerAdapter(StorageAdapter):
    """SQL Server-backed log storage using the existing repo SQL helper."""

    def __init__(
        self,
        *,
        config: LoggingConfig | None = None,
        sql_factory: Callable[[], Any] | None = None,
    ) -> None:
        self.config = config or LoggingConfig()
        self.sql_factory = sql_factory or default_sql_factory
        self._schema_ready = False
        self._schema_lock = threading.Lock()

    def log_transaction(self, entry: TransactionLog) -> int | str | None:
        mapping = self.config.schema_mapping
        values = self._pick_columns(
            mapping.transaction_columns,
            {
                "type": entry.type,
                "correlation_id": entry.correlation_id,
                "timestamp": entry.timestamp,
                "code": entry.code,
                "message": entry.message,
                "source_system": entry.source_system,
                "script_name": entry.script_name,
                "payload_json": entry.payload_json,
                "request_id": entry.request_id,
                "run_id": entry.run_id,
                "level": entry.level,
            },
        )
        id_column = mapping.transaction_columns.get("id")
        return self._insert_row(mapping.transaction_table, values, returning_column=id_column)

    def create_run(self, run: RunLog) -> str:
        if self.config.auto_create_schema:
            self.ensure_schema()
        self._upsert_robot_definition(run.robot_definition)
        mapping = self.config.schema_mapping
        values = self._pick_columns(
            mapping.run_columns,
            {
                "id": run.run_id,
                "robot_id": run.robot_id,
                "job_id": run.job_id,
                "retry_of_run_id": run.retry_of_run_id,
                "status": run.status,
                "queued_at": run.queued_at,
                "started_at": run.started_at,
                "finished_at": run.finished_at,
                "duration_ms": run.duration_ms,
                "last_heartbeat_at": run.last_heartbeat_at,
                "progress_current": run.progress_current,
                "progress_total": run.progress_total,
                "progress_percent": run.progress_percent,
                "progress_unit": run.progress_unit,
                "progress_message": run.progress_message,
                "records_processed": run.records_processed,
                "records_succeeded": run.records_succeeded,
                "records_failed": run.records_failed,
                "error_message": run.error_message,
                "failure_category": run.failure_category,
                "trigger_source": run.trigger_source,
                "triggered_by": run.triggered_by,
                "input_params_json": run.input_params_json,
                "correlation_id": run.correlation_id,
                "latest_transaction_id": run.latest_transaction_id,
                "host_name": run.host_name,
                "process_id": run.process_id,
                "created_at": run.created_at,
                "updated_at": run.updated_at,
            },
        )
        self._insert_row(mapping.run_table, values)
        return run.run_id

    def update_run(self, run_id: str, fields: dict[str, Any]) -> None:
        mapping = self.config.schema_mapping
        values = self._pick_columns(mapping.run_columns, fields)
        if not values:
            return
        run_id_column = mapping.run_columns["id"]
        assignments = ", ".join(f"{_quote_identifier(column)} = ?" for column in values)
        params = list(values.values()) + [run_id]
        sql = (
            f"UPDATE {_quote_table(mapping.run_table)} "
            f"SET {assignments} "
            f"WHERE {_quote_identifier(run_id_column)} = ?;"
        )
        with self.sql_factory() as db:
            db.execute(sql, tuple(params), commit=True)

    def add_event(self, event: RunEventLog) -> None:
        mapping = self.config.schema_mapping
        values = self._pick_columns(
            mapping.event_columns,
            {
                "run_id": event.run_id,
                "timestamp": event.timestamp,
                "event_type": event.event_type,
                "message": event.message,
                "payload_json": event.payload_json,
            },
        )
        self._insert_row(mapping.event_table, values)

    def ensure_schema(self) -> None:
        if self._schema_ready:
            return
        with self._schema_lock:
            if self._schema_ready:
                return
            ddl = "\n".join(
                [
                    self._robot_table_ddl(),
                    self._run_table_ddl(),
                    self._event_table_ddl(),
                    self._run_indexes_ddl(),
                    self._event_indexes_ddl(),
                ]
            )
            with self.sql_factory() as db:
                db.execute(ddl, commit=True)
            self._schema_ready = True

    def _pick_columns(self, column_map: dict[str, str], data: dict[str, Any]) -> dict[str, Any]:
        picked: dict[str, Any] = {}
        for logical_name, column_name in column_map.items():
            if logical_name not in data:
                continue
            value = data[logical_name]
            if value is None:
                continue
            picked[column_name] = value
        return picked

    def _insert_row(
        self,
        table_name: str,
        values: dict[str, Any],
        *,
        returning_column: str | None = None,
    ) -> int | str | None:
        if not values:
            return None
        columns = list(values.keys())
        placeholders = ", ".join("?" for _ in columns)
        column_list = ", ".join(_quote_identifier(column) for column in columns)
        output_sql = ""
        if returning_column:
            output_sql = f" OUTPUT INSERTED.{_quote_identifier(returning_column)}"
        sql = (
            f"INSERT INTO {_quote_table(table_name)} ({column_list})"
            f"{output_sql} VALUES ({placeholders});"
        )
        with self.sql_factory() as db:
            cursor = db.connection.cursor()
            try:
                cursor.execute(sql, tuple(values[column] for column in columns))
                returned = cursor.fetchone()[0] if returning_column else None
                db.connection.commit()
                return returned
            except Exception:
                db.connection.rollback()
                raise
            finally:
                cursor.close()

    def _upsert_robot_definition(self, definition: RobotDefinition | None) -> None:
        if definition is None:
            return
        mapping = self.config.schema_mapping
        cols = mapping.robot_columns
        sql = f"""
IF EXISTS (SELECT 1 FROM {_quote_table(mapping.robot_table)} WHERE {_quote_identifier(cols['id'])} = ?)
BEGIN
    UPDATE {_quote_table(mapping.robot_table)}
    SET
        {_quote_identifier(cols['display_name'])} = ?,
        {_quote_identifier(cols['source_system'])} = ?,
        {_quote_identifier(cols['run_scope'])} = ?,
        {_quote_identifier(cols['description'])} = ?,
        {_quote_identifier(cols['is_active'])} = 1,
        {_quote_identifier(cols['updated_at'])} = SYSUTCDATETIME()
    WHERE {_quote_identifier(cols['id'])} = ?;
END
ELSE
BEGIN
    INSERT INTO {_quote_table(mapping.robot_table)} (
        {_quote_identifier(cols['id'])},
        {_quote_identifier(cols['display_name'])},
        {_quote_identifier(cols['source_system'])},
        {_quote_identifier(cols['run_scope'])},
        {_quote_identifier(cols['description'])},
        {_quote_identifier(cols['is_active'])},
        {_quote_identifier(cols['created_at'])},
        {_quote_identifier(cols['updated_at'])}
    )
    VALUES (?, ?, ?, ?, ?, 1, SYSUTCDATETIME(), SYSUTCDATETIME());
END;
"""
        params = (
            definition.robot_id,
            definition.display_name,
            definition.source_system,
            definition.run_scope,
            definition.description,
            definition.robot_id,
            definition.robot_id,
            definition.display_name,
            definition.source_system,
            definition.run_scope,
            definition.description,
        )
        with self.sql_factory() as db:
            db.execute(sql, params, commit=True)

    def _table_exists_clause(self, table_name: str) -> str:
        schema_name, bare_table = _split_table(table_name)
        return (
            "IF NOT EXISTS ("
            "SELECT 1 FROM sys.tables t "
            "JOIN sys.schemas s ON s.schema_id = t.schema_id "
            f"WHERE s.name = '{schema_name}' AND t.name = '{bare_table}')"
        )

    def _robot_table_ddl(self) -> str:
        mapping = self.config.schema_mapping
        cols = mapping.robot_columns
        table = _quote_table(mapping.robot_table)
        return f"""
{self._table_exists_clause(mapping.robot_table)}
BEGIN
    CREATE TABLE {table}(
        {_quote_identifier(cols['id'])} NVARCHAR(100) NOT NULL PRIMARY KEY,
        {_quote_identifier(cols['display_name'])} NVARCHAR(255) NOT NULL,
        {_quote_identifier(cols['source_system'])} NVARCHAR(100) NOT NULL,
        {_quote_identifier(cols['run_scope'])} NVARCHAR(50) NOT NULL,
        {_quote_identifier(cols['description'])} NVARCHAR(1000) NULL,
        {_quote_identifier(cols['is_active'])} BIT NOT NULL DEFAULT (1),
        {_quote_identifier(cols['created_at'])} DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),
        {_quote_identifier(cols['updated_at'])} DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;
"""

    def _run_table_ddl(self) -> str:
        mapping = self.config.schema_mapping
        cols = mapping.run_columns
        robot_cols = mapping.robot_columns
        table = _quote_table(mapping.run_table)
        robot_table = _quote_table(mapping.robot_table)
        return f"""
{self._table_exists_clause(mapping.run_table)}
BEGIN
    CREATE TABLE {table}(
        {_quote_identifier(cols['id'])} NVARCHAR(36) NOT NULL PRIMARY KEY,
        {_quote_identifier(cols['robot_id'])} NVARCHAR(100) NOT NULL REFERENCES {robot_table}({_quote_identifier(robot_cols['id'])}),
        {_quote_identifier(cols['job_id'])} NVARCHAR(36) NULL,
        {_quote_identifier(cols['retry_of_run_id'])} NVARCHAR(36) NULL,
        {_quote_identifier(cols['status'])} NVARCHAR(20) NOT NULL,
        {_quote_identifier(cols['queued_at'])} DATETIME2(0) NULL,
        {_quote_identifier(cols['started_at'])} DATETIME2(0) NULL,
        {_quote_identifier(cols['finished_at'])} DATETIME2(0) NULL,
        {_quote_identifier(cols['duration_ms'])} BIGINT NULL,
        {_quote_identifier(cols['last_heartbeat_at'])} DATETIME2(0) NULL,
        {_quote_identifier(cols['progress_current'])} INT NULL,
        {_quote_identifier(cols['progress_total'])} INT NULL,
        {_quote_identifier(cols['progress_percent'])} DECIMAL(5,2) NULL,
        {_quote_identifier(cols['progress_unit'])} NVARCHAR(50) NULL,
        {_quote_identifier(cols['progress_message'])} NVARCHAR(1000) NULL,
        {_quote_identifier(cols['records_processed'])} INT NULL,
        {_quote_identifier(cols['records_succeeded'])} INT NULL,
        {_quote_identifier(cols['records_failed'])} INT NULL,
        {_quote_identifier(cols['error_message'])} NVARCHAR(MAX) NULL,
        {_quote_identifier(cols['failure_category'])} NVARCHAR(100) NULL,
        {_quote_identifier(cols['trigger_source'])} NVARCHAR(20) NOT NULL,
        {_quote_identifier(cols['triggered_by'])} NVARCHAR(255) NULL,
        {_quote_identifier(cols['input_params_json'])} NVARCHAR(MAX) NULL,
        {_quote_identifier(cols['correlation_id'])} NVARCHAR(100) NULL,
        {_quote_identifier(cols['latest_transaction_id'])} INT NULL,
        {_quote_identifier(cols['host_name'])} NVARCHAR(255) NULL,
        {_quote_identifier(cols['process_id'])} INT NULL,
        {_quote_identifier(cols['created_at'])} DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),
        {_quote_identifier(cols['updated_at'])} DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),
        FOREIGN KEY({_quote_identifier(cols['retry_of_run_id'])}) REFERENCES {table}({_quote_identifier(cols['id'])})
    );
END;
"""

    def _event_table_ddl(self) -> str:
        mapping = self.config.schema_mapping
        cols = mapping.event_columns
        run_cols = mapping.run_columns
        table = _quote_table(mapping.event_table)
        run_table = _quote_table(mapping.run_table)
        return f"""
{self._table_exists_clause(mapping.event_table)}
BEGIN
    CREATE TABLE {table}(
        {_quote_identifier(cols['id'])} BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        {_quote_identifier(cols['run_id'])} NVARCHAR(36) NOT NULL REFERENCES {run_table}({_quote_identifier(run_cols['id'])}),
        {_quote_identifier(cols['timestamp'])} DATETIME2(0) NOT NULL,
        {_quote_identifier(cols['event_type'])} NVARCHAR(30) NOT NULL,
        {_quote_identifier(cols['message'])} NVARCHAR(1000) NULL,
        {_quote_identifier(cols['payload_json'])} NVARCHAR(MAX) NULL
    );
END;
"""

    def _run_indexes_ddl(self) -> str:
        mapping = self.config.schema_mapping
        cols = mapping.run_columns
        index_one = _safe_name(f"IX_{mapping.run_table}_robot_status")
        index_two = _safe_name(f"IX_{mapping.run_table}_job_id")
        table = _quote_table(mapping.run_table)
        run_schema, run_table = _split_table(mapping.run_table)
        return f"""
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = '{index_one}')
BEGIN
    CREATE INDEX {_quote_identifier(index_one)} ON {table}(
        {_quote_identifier(cols['robot_id'])},
        {_quote_identifier(cols['status'])},
        {_quote_identifier(cols['queued_at'])} DESC
    );
END;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = '{index_two}')
BEGIN
    CREATE INDEX {_quote_identifier(index_two)} ON {table}({_quote_identifier(cols['job_id'])});
END;
"""

    def _event_indexes_ddl(self) -> str:
        mapping = self.config.schema_mapping
        cols = mapping.event_columns
        index_name = _safe_name(f"IX_{mapping.event_table}_run_timestamp")
        table = _quote_table(mapping.event_table)
        return f"""
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = '{index_name}')
BEGIN
    CREATE INDEX {_quote_identifier(index_name)} ON {table}(
        {_quote_identifier(cols['run_id'])},
        {_quote_identifier(cols['timestamp'])} DESC
    );
END;
"""


class InMemoryAdapter(StorageAdapter):
    """Simple in-memory adapter for tests and local verification."""

    def __init__(self) -> None:
        self.transactions: list[dict[str, Any]] = []
        self.runs: dict[str, dict[str, Any]] = {}
        self.events: list[dict[str, Any]] = []
        self.robots: dict[str, dict[str, Any]] = {}
        self._transaction_id = 0

    def log_transaction(self, entry: TransactionLog) -> int | str | None:
        self._transaction_id += 1
        payload = asdict(entry)
        payload["id"] = self._transaction_id
        self.transactions.append(payload)
        return self._transaction_id

    def create_run(self, run: RunLog) -> str:
        payload = asdict(run)
        definition = payload.pop("robot_definition", None)
        if definition:
            self.robots[definition["robot_id"]] = definition
        self.runs[run.run_id] = payload
        return run.run_id

    def update_run(self, run_id: str, fields: dict[str, Any]) -> None:
        if run_id not in self.runs:
            self.runs[run_id] = {"run_id": run_id}
        self.runs[run_id].update(fields)

    def add_event(self, event: RunEventLog) -> None:
        self.events.append(asdict(event))

    def ensure_schema(self) -> None:
        return None
