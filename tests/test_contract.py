import copy
import json
import tempfile
import unittest
from pathlib import Path

from contract_schema import Contract, loader
from contract_schema.validator import SchemaError


class ContractTests(unittest.TestCase):
    def setUp(self):
        self.contract = Contract.load("analytic_schema.json")
        self.payload  = {
            "start_dtg":        "2025-08-03T00:00:00Z",
            "end_dtg":          "2025-08-03T01:00:00Z",
            "data_source_type": "file",
            "data_source":      "/tmp/data.csv",
        }

    def test_parse_and_validate_injects_defaults(self):
        out = self.contract.parse_and_validate_input(self.payload)

        # Original keys preserved
        for k in self.payload:
            self.assertEqual(out[k], self.payload[k])

        # Defaults injected
        self.assertEqual(out["log_path"], "stdout")
        self.assertEqual(out["output"], "stdout")
        self.assertEqual(out["verbosity"], "INFO")

    def test_parse_and_validate_fails_on_invalid_data(self):
        bad_payload = self.payload.copy()
        bad_payload.pop("start_dtg")  # Remove a required field
        with self.assertRaises(SchemaError):
            self.contract.parse_and_validate_input(bad_payload)

    def test_create_document_returns_instance(self):
        doc = self.contract.create_document()
        from contract_schema.document import Document
        self.assertIsInstance(doc, Document)

    def test_contract_load_fails_when_meta_schema_broken(self):
        bad = copy.deepcopy(loader.load_schema("analytic_schema.json"))
        bad.pop("description")  # violate meta-schema

        with tempfile.NamedTemporaryFile("w+", delete=False) as tmp:
            json.dump(bad, tmp)
            tmp.flush()
            p = Path(tmp.name)

        try:
            with self.assertRaises(ValueError):
                Contract.load(p)
        finally:
            p.unlink(missing_ok=True)