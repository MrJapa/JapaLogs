import os
import unittest
from unittest.mock import patch

from config import SchemaMapping
from utils import (
    extract_error_code,
    generate_correlation_id,
    generate_request_id,
    normalize_event_type,
    normalize_status,
    read_float_env,
    read_int_env,
    serialize_payload,
)


class FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload
        self.text = "" if payload is None else str(payload)

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class UtilsTests(unittest.TestCase):
    def test_extract_error_code_prefers_payload_code(self):
        response = FakeResponse(400, {"errors": [{"kode": "E123"}]})
        self.assertEqual(extract_error_code(response), "E123")

    def test_extract_error_code_falls_back_to_status(self):
        response = FakeResponse(503, ValueError("bad json"))
        self.assertEqual(extract_error_code(response), "503")

    def test_generate_ids_are_strings(self):
        self.assertIsInstance(generate_correlation_id(), str)
        self.assertIsInstance(generate_request_id(), str)

    def test_serialize_payload_truncates_text(self):
        self.assertEqual(serialize_payload("abcdef", max_length=5), "ab...")

    def test_env_helpers_use_defaults_and_minimums(self):
        with patch.dict(os.environ, {"INT_VALUE": "0", "FLOAT_VALUE": "0.5"}, clear=False):
            self.assertEqual(read_int_env("INT_VALUE", 10, min_value=2), 2)
            self.assertEqual(read_float_env("FLOAT_VALUE", 1.0), 0.5)
            self.assertEqual(read_int_env("MISSING_INT", 7), 7)

    def test_status_and_event_validation(self):
        self.assertEqual(normalize_status("RUNNING"), "running")
        self.assertEqual(normalize_event_type("Completed"), "completed")
        with self.assertRaises(ValueError):
            normalize_status("made_up")

    def test_schema_mapping_defaults_match_current_schema(self):
        mapping = SchemaMapping()
        self.assertEqual(mapping.transaction_table, "api.Transaktion")
        self.assertEqual(mapping.run_columns["robot_id"], "Robot_Id")


if __name__ == "__main__":
    unittest.main()
