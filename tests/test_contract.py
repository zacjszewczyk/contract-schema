import unittest

from contract_schema import Contract


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

    def test_create_document_returns_instance(self):
        doc = self.contract.create_document()
        from contract_schema.document import Document
        self.assertIsInstance(doc, Document)
