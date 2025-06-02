"""Unit tests for analytic_schema.py

These tests exercise the public API exposed by analytic_schema:
  • parse_input()
  • validate_input()
  • OutputDoc

Only the Python standard library is used (unittest, tempfile, json, pathlib, os).
Run with:
    python -m unittest test_analytic_schema.py

Assumes test_analytic_schema.py is in the same directory as analytic_schema.py,
or that analytic_schema is otherwise importable on PYTHONPATH.
"""

import json
import os
import tempfile
import unittest
from pathlib import Path

import analytic_schema as asc  # The module under test


class AnalyticSchemaTests(unittest.TestCase):
    """Comprehensive test‑suite for analytic_schema helpers."""

    # ---------------------------------------------------------------------
    # Utility helpers
    # ---------------------------------------------------------------------
    @staticmethod
    def _minimal_input() -> dict:
        """Return a minimal but valid input document."""
        return {
            "input_schema_version": "1.0.0",
            "start_dtg": "2025-06-01T00:00:00Z",
            "end_dtg": "2025-06-02T00:00:00Z",
            "data_source_type": "file",
            "data_source": "/tmp/conn.csv",
        }

    # ------------------------------------------------------------------
    # parse_input() / CLI handling
    # ------------------------------------------------------------------
    def test_parse_dict_valid(self):
        """Dict input passes through unchanged."""
        inp = self._minimal_input()
        self.assertEqual(asc.parse_input(inp), inp)

    def test_parse_cli_string_valid(self):
        """Shell‑string input is parsed correctly."""
        cli = (
            "--input_schema_version 1.0.0 "
            "--start_dtg 2025-06-01T00:00:00Z "
            "--end_dtg 2025-06-02T00:00:00Z "
            "--data_source_type file "
            "--data_source /tmp/conn.csv"
        )
        self.assertEqual(asc.parse_input(cli), self._minimal_input())

    def test_parse_cli_tokens_valid(self):
        """List‑of‑tokens (sys.argv‑like) is handled."""
        tokens = [
            "--input_schema_version", "1.0.0",
            "--start_dtg", "2025-06-01T00:00:00Z",
            "--end_dtg", "2025-06-02T00:00:00Z",
            "--data_source_type", "file",
            "--data_source", "/tmp/conn.csv",
        ]
        self.assertEqual(asc.parse_input(tokens), self._minimal_input())

    def test_parse_json_file_valid(self):
        """A JSON file path is read correctly."""
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".json") as fp:
            json.dump(self._minimal_input(), fp)
            fp.flush()
            path = Path(fp.name)
        try:
            self.assertEqual(asc.parse_input(path), self._minimal_input())
        finally:
            path.unlink(missing_ok=True)

    def test_parse_missing_required_raises(self):
        """Missing required parameters triggers ValueError."""
        with self.assertRaises(ValueError):
            asc.parse_input({"input_schema_version": "1.0.0"})

    # ------------------------------------------------------------------
    # validate_input() – structural & semantic checks
    # ------------------------------------------------------------------
    def test_validate_canonicalises_object_or_path(self):
        """String paths for analytic_parameters / data_map are resolved."""
        tmp_payload = {"foo": "bar"}
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".json") as fp:
            json.dump(tmp_payload, fp)
            fp.flush()
            p = Path(fp.name)
        try:
            data = self._minimal_input()
            data["analytic_parameters"] = str(p)
            validated = asc.validate_input(data)
            self.assertEqual(validated["analytic_parameters"], tmp_payload)
        finally:
            p.unlink(missing_ok=True)

    def test_validate_rejects_bad_datetime(self):
        data = self._minimal_input()
        data["start_dtg"] = "06/01/2025"  # Not ISO‑8601
        with self.assertRaises(asc.SchemaError):
            asc.validate_input(data)

    def test_validate_rejects_bad_enum(self):
        data = self._minimal_input()
        data["data_source_type"] = "database"
        with self.assertRaises(asc.SchemaError):
            asc.validate_input(data)

    def test_validate_rejects_additional_props(self):
        data = self._minimal_input()
        data["unexpected"] = "value"
        with self.assertRaises(asc.SchemaError):
            asc.validate_input(data)

    # ------------------------------------------------------------------
    # OutputDoc helper
    # ------------------------------------------------------------------
    def _make_outputdoc(self):
        inputs = asc.validate_input(self._minimal_input())
        return asc.OutputDoc(
            input_schema_version=inputs["input_schema_version"],
            output_schema_version="1.1.0",
            analytic_id="unit/test.ipynb",
            analytic_name="Unit‑Test",
            analytic_version="0.0.1",
            inputs=inputs,
            input_data_hash="deadbeef",
            status="success",
            exit_code=0,
            findings=[],
            records_processed=0,
        )

    def test_outputdoc_finalise_populates_fields(self):
        doc = self._make_outputdoc()
        doc.finalise()
        # Required fields are now present
        for field in (
            "run_id",
            "run_user",
            "run_host",
            "run_start_dtg",
            "run_end_dtg",
            "run_duration_seconds",
            "input_hash",
            "findings_hash",
        ):
            self.assertIn(field, doc)
        # And the document validates against the schema
        asc._validate(doc, asc.OUTPUT_SCHEMA)

    def test_outputdoc_add_message(self):
        doc = self._make_outputdoc()
        doc.add_message("info", "Sample log entry")
        self.assertEqual(doc["messages"][0]["level"], "INFO")

    def test_outputdoc_missing_inputs_raises(self):
        doc = asc.OutputDoc(
            input_schema_version="1.0.0",
            output_schema_version="1.1.0",
            analytic_id="x",
            analytic_name="x",
            analytic_version="x",
            input_data_hash="deadbeef",
            status="success",
            exit_code=0,
            findings=[],
            records_processed=0,
        )
        with self.assertRaises(asc.SchemaError):
            doc.finalise()


if __name__ == "__main__":
    unittest.main(verbosity=2)
