import unittest

from API.http_logging import extract_error_code as wrapper_extract_error_code
from API.robot_tracking import RobotRunTracker
from db import InMemoryAdapter
from logger import LoggerService


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text or str(self._payload)

    def json(self):
        return self._payload


class FakeHttpError(Exception):
    def __init__(self, response):
        super().__init__("request failed")
        self.response = response


class LoggerServiceTests(unittest.TestCase):
    def test_http_logging_persists_transaction_entries(self):
        adapter = InMemoryAdapter()
        service = LoggerService(storage=adapter)
        context = service.new_context(source_system="CVR", script_name="tests")

        response = FakeResponse(200, {"ok": True})
        transaction_id = service.log_request_success(
            "CVR_Request",
            context.correlation_id,
            response=response,
            method="POST",
            url="https://example.test/cvr",
            request_body={"cvr": "12345678"},
            context=context,
        )

        self.assertEqual(transaction_id, 1)
        self.assertEqual(adapter.transactions[0]["type"], "CVR_Request")
        self.assertEqual(adapter.transactions[0]["code"], "200")

    def test_exception_logging_uses_error_code_and_marks_error_level(self):
        adapter = InMemoryAdapter()
        service = LoggerService(storage=adapter)
        context = service.new_context(source_system="DUPLA", script_name="tests")
        response = FakeResponse(500, {"errors": [{"kode": "D500"}]})

        service.log_request_exception(
            "DUPLA_RequestError",
            context.correlation_id,
            FakeHttpError(response),
            context=context,
        )

        self.assertEqual(adapter.transactions[0]["code"], "D500")
        self.assertEqual(adapter.transactions[0]["level"], "error")

    def test_run_lifecycle_updates_in_memory_state(self):
        adapter = InMemoryAdapter()
        service = LoggerService(storage=adapter)
        context = service.new_context(source_system="CVR", script_name="tests", robot_id="cvr_all")

        run = service.create_run(
            robot_id="cvr_all",
            trigger_source="cli",
            input_params={"year": 2025},
            context=context,
        )
        run.started()
        run.progress(current=5, total=10, records_processed=5, message="Halfway")
        run.attach_transaction(correlation_id=context.correlation_id, latest_transaction_id=42)
        run.completed(
            correlation_id=context.correlation_id,
            latest_transaction_id=42,
            records_processed=10,
            records_succeeded=10,
            records_failed=0,
        )

        stored_run = adapter.runs[run.run_id]
        self.assertEqual(stored_run["status"], "succeeded")
        self.assertEqual(stored_run["latest_transaction_id"], 42)
        self.assertEqual(stored_run["records_processed"], 10)
        self.assertGreaterEqual(len(adapter.events), 4)

    def test_compatibility_wrappers_delegate_to_shared_service(self):
        adapter = InMemoryAdapter()
        service = LoggerService(storage=adapter)
        tracker = RobotRunTracker(logger_service=service)

        handle = tracker.create_run(robot_id="cvr_single", trigger_source="cli")
        tracker.mark_started(handle.run_id)
        tracker.mark_completed(handle.run_id, latest_transaction_id=99, records_processed=1, records_succeeded=1)

        self.assertEqual(adapter.runs[handle.run_id]["status"], "succeeded")
        self.assertEqual(adapter.runs[handle.run_id]["latest_transaction_id"], 99)
        self.assertEqual(wrapper_extract_error_code(FakeResponse(418, {"errors": [{"kode": "T418"}]})), "T418")


if __name__ == "__main__":
    unittest.main()
