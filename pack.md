# Plan For A Reusable Python Logging Package

## Goal

Create a Python package that centralizes logging for API integrations, database persistence, and robot/job execution, so the same logging model can be reused across multiple scripts without duplicating logic.

The package should preserve the strengths of the current implementation:

- Simple transaction logging like `SQL.log(...)` in `Database/db.py`
- HTTP-aware success and failure logging like in `API/CVR/cvr.py` and `API/DUPLA/selskabsskatteoplysninger.py`
- Durable run tracking like `API/robot_tracking.py`
- Compatibility with different database schemas and table layouts
- Easy adoption from standalone scripts, services, and batch jobs

## What The Current Code Already Does Well

The current codebase already has three useful logging layers:

1. Transaction logging
   `Database/db.py` writes rows to `api.Transaktion` with `Type`, `Transaktion`, `Timestamp`, and `Code`.

2. API request/result logging
   `API/CVR/cvr.py`, `API/DUPLA/selskabsskatteoplysninger.py`, and related modules log:
   - request success
   - request failure
   - request exceptions
   - correlation ids / transaction ids
   - response-derived error codes

3. Robot execution tracking
   `API/robot_tracking.py` tracks:
   - run creation
   - status transitions
   - heartbeats
   - progress
   - completion/failure
   - event history
   - relation to low-level transaction rows

The main weakness is not capability. It is that the logic is spread across multiple files and tightly coupled to one SQL schema and one helper class.

## Package Objectives

The package should:

- expose one consistent logging API for all scripts
- support multiple storage backends and multiple database schemas
- separate logging concerns from API/business logic
- support both lightweight transaction logging and advanced run tracking
- make correlation ids and request ids first-class concepts
- standardize status codes, event types, and error handling
- reduce repeated `try/except/log/re-raise` code in API modules
- allow scripts to opt into only the logging features they need

## Recommended Package Scope

The package should cover four logging domains:

1. Transaction logging
   A lightweight log entry for one request, one persistence action, or one integration step.

2. HTTP/API logging
   Helpers for request start, request success, request failure, exception capture, and error code extraction.

3. Run/job logging
   Long-running process tracking for batch jobs, robots, CLI commands, and scheduled tasks.

4. Persistence logging hooks
   Optional helpers for upserts, batch writes, and database save operations.

## Proposed Package Name

Use a clear internal name such as:

- `bdh_logging`
- `integration_logging`
- `robot_logging`

`bdh_logging` is probably the best fit if this is an internal shared package for this codebase.

## Proposed Package Structure

```text
bdh_logging/
  __init__.py
  config.py
  context.py
  enums.py
  models.py
  exceptions.py
  interfaces.py
  service.py
  factory.py
  correlation.py
  http.py
  runs.py
  transactions.py
  persistence.py
  adapters/
    __init__.py
    base.py
    sql_server.py
    sqlite.py
    memory.py
  schemas/
    __init__.py
    transaction_schema.py
    run_schema.py
    event_schema.py
    mappings.py
  integrations/
    __init__.py
    requests.py
    pyodbc.py
  testing/
    fakes.py
    fixtures.py
```

## Core Design Principle

The package should be split into:

- a domain layer
  Defines what a log event means

- a service layer
  Defines how application code interacts with logging

- an adapter layer
  Defines where logs are stored and how table/column differences are handled

This is the key requirement for handling different database structures.

## Main Abstractions

### 1. `LoggerService`

This should be the main entry point used by scripts.

Responsibilities:

- create transaction logs
- log HTTP requests and failures
- create and update run records
- write structured events
- attach correlation ids to related records

Example shape:

```python
logger = LoggerService(storage=storage_adapter, config=config)
```

### 2. `StorageAdapter`

This should be an interface or abstract base class.

Responsibilities:

- insert transaction rows
- update run rows
- insert event rows
- optionally create schema objects
- translate generic logging fields into a concrete table structure

Example methods:

```python
class StorageAdapter(Protocol):
    def log_transaction(self, entry: TransactionLog) -> int | str: ...
    def create_run(self, run: RunLog) -> str: ...
    def update_run(self, run_id: str, fields: dict) -> None: ...
    def add_event(self, event: RunEventLog) -> None: ...
    def ensure_schema(self) -> None: ...
```

### 3. `SchemaMapping`

This is the most important piece for supporting different database structures.

Instead of hardcoding:

- table names
- schema names
- primary key names
- column names

the package should allow these to be configured.

Example:

```python
SchemaMapping(
    transaction_table="api.Transaktion",
    transaction_columns={
        "id": "Id",
        "type": "Type",
        "correlation_id": "Transaktion",
        "timestamp": "Timestamp",
        "code": "Code",
    },
    run_table="api.RobotRun",
    event_table="api.RobotRunEvent",
)
```

This lets the same package write to:

- the current SQL Server schema
- a future normalized schema
- a simplified single-table log model
- a customer-specific database layout

### 4. `LogContext`

This should hold shared metadata for a flow.

Fields:

- correlation_id
- request_id
- source_system
- script_name
- actor
- environment
- robot_id
- run_id

This allows one script to create a context once and reuse it across many logging calls.

## Data Models To Standardize

The package should define typed models, likely dataclasses or Pydantic models.

### `TransactionLog`

Fields:

- type
- correlation_id
- code
- timestamp
- message
- source_system
- script_name
- payload_json

### `HttpLogEntry`

Fields:

- method
- url
- status_code
- derived_error_code
- request_headers
- request_params
- request_body
- response_body
- duration_ms
- success
- correlation_id

### `RunLog`

Fields:

- run_id
- robot_id
- status
- queued_at
- started_at
- finished_at
- duration_ms
- trigger_source
- triggered_by
- input_params_json
- correlation_id
- latest_transaction_id
- records_processed
- records_succeeded
- records_failed
- progress_current
- progress_total
- progress_percent
- host_name
- process_id

### `RunEventLog`

Fields:

- run_id
- timestamp
- event_type
- message
- payload_json

## Support For Different Database Structures

This should be a first-class design requirement, not an afterthought.

### Approach

The package should support three levels of flexibility:

1. Fixed built-in schema profiles
   Ready-made mappings for:
   - current `api.Transaktion`
   - current `api.RobotRun`
   - current `api.RobotRunEvent`

2. Configurable table and column mappings
   Allow the caller to remap table names and columns without changing package code.

3. Fully custom adapters
   Allow a new adapter to be written when a database uses a very different model.

### Schema Variants The Package Should Handle

The package should be able to support:

- one transaction table only
- transaction table plus separate run table
- transaction table plus run table plus event table
- a single generic log table with `event_type` and JSON payload
- legacy schemas with different column names
- databases where ids are integers, GUIDs, or strings

### Configuration Example

```python
config = LoggingConfig(
    schema_mapping=SchemaMapping(
        transaction_table="audit.ApiTransactions",
        transaction_columns={
            "id": "TransactionId",
            "type": "Category",
            "correlation_id": "CorrelationKey",
            "timestamp": "CreatedAt",
            "code": "StatusCode",
        },
        run_table="audit.JobRuns",
        event_table="audit.JobRunEvents",
    )
)
```

## Recommended Modules

### `transactions.py`

Purpose:

- replace direct `SQL.log(...)` usage
- provide a generic transaction logger
- handle default timestamps
- normalize codes to strings
- attach optional metadata

Recommended API:

```python
transaction_id = logger.log_transaction(
    type="CVR_Request",
    correlation_id=transaction_id,
    code="200",
    message="CVR request completed",
)
```

### `http.py`

Purpose:

- standardize request success/failure logs
- extract error codes from responses
- optionally wrap `requests` calls

Recommended features:

- `extract_error_code(response)` compatible helper
- `log_request_success(...)`
- `log_request_failure(...)`
- `log_request_exception(...)`
- optional context manager or decorator around HTTP calls

Example:

```python
with logger.http_request(
    method="GET",
    url=url,
    context=context,
) as http_log:
    response = requests.get(...)
    http_log.capture_response(response)
```

### `runs.py`

Purpose:

- move the durable execution tracking out of script-specific code
- generalize `RobotRunTracker` into a reusable run tracker

Recommended API:

```python
run = logger.create_run(
    robot_id="cvr_all",
    trigger_source="cli",
    input_params={"year": 2025},
)

run.started()
run.progress(current=10, total=100, message="Fetched first batch")
run.completed(records_processed=100, latest_transaction_id=123)
```

### `persistence.py`

Purpose:

- log save/upsert operations without tightly coupling logging
- optionally track table name, row count, primary key values, and failures

Recommended features:

- `log_upsert_start(...)`
- `log_upsert_success(...)`
- `log_upsert_failure(...)`
- helpers for bulk operations

This module should not own database upsert behavior. It should only log persistence behavior.


## Recommended Storage Adapters

### 1. SQL Server adapter

This should be the first production adapter because the current code uses `pyodbc` and SQL Server.

Responsibilities:

- integrate with the existing connection pattern
- support identity return values
- support `ensure_schema()`
- support configurable table mappings

### 2. In-memory adapter

This is important for tests.

Responsibilities:

- store logs in Python lists/dicts
- make unit testing fast and deterministic
- allow testing without SQL Server

### 3. SQLite adapter

Optional but valuable for local development.

Responsibilities:

- support a simple local file database
- make package behavior easy to test outside production SQL Server

## Logging Levels And Event Types

Even if logs are persisted to database tables, the package should define standard event semantics.

Recommended log levels:

- `debug`
- `info`
- `warning`
- `error`
- `critical`

Recommended transaction/event categories:

- `request`
- `request_success`
- `request_failure`
- `request_exception`
- `run_queued`
- `run_started`
- `run_progress`
- `run_persisting`
- `run_completed`
- `run_failed`
- `db_upsert`
- `db_upsert_failed`

These should be stored as enums or constants so naming stays consistent across all scripts.

## Correlation Strategy

The package should define a clear correlation model.

### Minimum rule

Every operation should be able to carry:

- a correlation id
- an optional request id
- an optional run id

### Recommended behavior

- generate correlation ids automatically when not provided
- preserve externally provided ids like `X-Transaktions-ID`
- allow a run to attach the latest low-level transaction id
- allow one correlation id to connect HTTP calls, save operations, and final run outcome

This is especially important for your current API flows.

## Error Handling Strategy

The package should make failures easier to log correctly.

### Recommended pattern

- log request/operation failure once, near the failure boundary
- preserve the original exception and re-raise it
- avoid duplicating the same error log in multiple layers unless that duplication is intentional

### Package helpers should support

- exception-to-code mapping
- safe response parsing
- fallback code values like `"400"` or `"UNKNOWN"`
- truncated payload logging for large response bodies

## Configuration Model

Use a config object rather than scattered environment-variable reads inside business logic.

### `LoggingConfig` should include

- adapter type
- database connection provider
- schema mapping
- whether to auto-create schema
- payload truncation limits
- default source system
- default script name
- log body capture on/off
- retry-safe behavior

Example:

```python
config = LoggingConfig(
    auto_create_schema=False,
    capture_response_body=True,
    max_payload_length=4000,
    default_source_system="CVR",
)
```

## Integration Pattern For Existing Scripts

### Current problem

Each script currently:

- creates its own ids
- logs success/failure manually
- decides error codes locally
- writes database rows directly

### Recommended target

Each script should instead:

1. create a logger once
2. create a context once
3. log via the package API
4. keep business logic focused on fetching and saving data

Example for CVR:

```python
logger = build_logger_from_sql(sql_connection_factory)
context = logger.new_context(source_system="CVR", script_name="API.CVR.cvr")

run = logger.create_run(robot_id="cvr_all", trigger_source="cli", input_params={"year": year})
run.started()

try:
    payload, correlation_id = logger.http_post(...)
    transaction_id = logger.log_transaction("CVR_Request", correlation_id=correlation_id, code="200")
    save_result = save_cvr_response(payload, transaction_id)
    run.completed(records_processed=save_result.records_processed, latest_transaction_id=transaction_id)
except Exception as exc:
    run.failed(error_message=str(exc), failure_category="http")
    raise
```

## Backward Compatibility Plan

The package should be introduced in phases.

### Phase 1

Build package with SQL Server adapter and compatibility wrappers.

Deliverables:

- `LoggerService`
- SQL Server adapter
- transaction logging
- HTTP logging helpers
- run logging
- compatibility wrapper for `SQL.log(...)`

## Testing Strategy

The package should be tested at three levels.

### Unit tests

Test:

- error code extraction
- correlation id generation
- status normalization
- payload truncation
- schema mapping resolution

### Adapter tests

Test:

- transaction insert
- run create/update
- event insert
- identity return behavior
- nullable column behavior

### Integration tests

Test:

- CVR request success path
- CVR request failure path
- DUPLA request exception path
- run completion with attached transaction id
- run failure with persisted error details

## Risks To Plan Around

### 1. Duplicate logging

Some failures are currently logged in more than one place. The package should define exactly which layer owns each log.

### 2. Schema drift

If table names or columns change, hardcoded SQL will become brittle. That is why schema mapping should be configurable.

### 3. Over-coupling to SQL Server

The first adapter can target SQL Server, but the service layer should not depend directly on `pyodbc`.

### 4. Logging too much payload data

Large API responses should be truncated or summarized.

### 5. Breaking existing scripts during migration

This is why compatibility wrappers and phased rollout matter.

## Minimal Viable Version

The first useful version of the package should include:

- transaction logging
- HTTP error-code extraction
- request success/failure logging helpers
- run creation and completion/failure tracking
- SQL Server adapter
- configurable table/column mapping
- in-memory adapter for tests

That is enough to replace the current repeated logging logic in the main API modules.

## Recommendation

Build the package around a generic `LoggerService` plus pluggable storage adapters and schema mappings.

That gives you:

- one logging API across all scripts
- support for different database structures
- cleaner API modules
- easier testing
- a safe migration path from the current `SQL.log(...)` and `RobotRunTracker` patterns

If this package is built in that shape, it can become the shared logging foundation for all your Python integrations instead of just another helper module tied to one database table.
