import unittest

from config import LoggingConfig
from db import SQLServerAdapter
from models import RobotDefinition, RunEventLog, RunLog, TransactionLog
from utils import utc_now


class FakeCursor:
    def __init__(self, recorder, returned_value):
        self.recorder = recorder
        self.returned_value = returned_value

    def execute(self, sql, params=()):
        self.recorder.append(("cursor.execute", sql, params))

    def fetchone(self):
        return (self.returned_value,)

    def close(self):
        self.recorder.append(("cursor.close",))


class FakeConnection:
    def __init__(self, recorder, returned_value):
        self.recorder = recorder
        self.returned_value = returned_value

    def cursor(self):
        return FakeCursor(self.recorder, self.returned_value)

    def commit(self):
        self.recorder.append(("connection.commit",))

    def rollback(self):
        self.recorder.append(("connection.rollback",))


class FakeSQL:
    def __init__(self, recorder, returned_value=123):
        self.recorder = recorder
        self.connection = FakeConnection(recorder, returned_value)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None, commit=False):
        self.recorder.append(("execute", sql, params, commit))


class SQLServerAdapterTests(unittest.TestCase):
    def test_log_transaction_returns_inserted_id(self):
        recorder = []
        adapter = SQLServerAdapter(
            config=LoggingConfig(auto_create_schema=False),
            sql_factory=lambda: FakeSQL(recorder, returned_value=321),
        )

        transaction_id = adapter.log_transaction(
            TransactionLog(
                type="CVR_Request",
                correlation_id="corr-1",
                code="200",
                timestamp=utc_now(),
            )
        )

        self.assertEqual(transaction_id, 321)
        self.assertTrue(any("INSERT INTO [api].[Transaktion]" in sql for kind, sql, *_ in recorder if kind == "cursor.execute"))

    def test_create_run_ensures_schema_and_upserts_robot_definition(self):
        recorder = []
        adapter = SQLServerAdapter(sql_factory=lambda: FakeSQL(recorder, returned_value=1))
        run = RunLog(
            run_id="run-1",
            robot_id="custom_robot",
            status="queued",
            trigger_source="cli",
            correlation_id="corr-1",
            queued_at=utc_now(),
            last_heartbeat_at=utc_now(),
            created_at=utc_now(),
            updated_at=utc_now(),
            robot_definition=RobotDefinition(
                robot_id="custom_robot",
                display_name="Custom Robot",
                source_system="Tests",
                run_scope="cli",
                description="Test robot",
            ),
        )

        adapter.create_run(run)

        execute_sql = "\n".join(call[1] for call in recorder if call[0] == "execute")
        self.assertIn("CREATE TABLE [api].[RobotRun]", execute_sql)
        self.assertIn("INSERT INTO [api].[Robot]", execute_sql)
        self.assertTrue(any("INSERT INTO [api].[RobotRun]" in sql for kind, sql, *_ in recorder if kind == "cursor.execute"))

    def test_update_run_and_add_event_use_mapped_tables(self):
        recorder = []
        adapter = SQLServerAdapter(
            config=LoggingConfig(auto_create_schema=False),
            sql_factory=lambda: FakeSQL(recorder, returned_value=1),
        )

        adapter.update_run("run-1", {"status": "running", "updated_at": utc_now()})
        adapter.add_event(
            RunEventLog(
                run_id="run-1",
                timestamp=utc_now(),
                event_type="started",
                message="Run started",
                payload_json="{}",
            )
        )

        execute_sql = "\n".join(call[1] for call in recorder if call[0] == "execute")
        self.assertIn("UPDATE [api].[RobotRun]", execute_sql)
        self.assertTrue(any("INSERT INTO [api].[RobotRunEvent]" in sql for kind, sql, *_ in recorder if kind == "cursor.execute"))


if __name__ == "__main__":
    unittest.main()
