import json, unittest, time, tempfile
from pathlib import Path
import pandas as pd

from tests._util import ANALYTIC_C as C, tmp_json, with_defaults, tmp_dir
from contract_schema.validator import SchemaError

DF = pd.DataFrame({"v": [1, 2]})

class AnalyticContractTests(unittest.TestCase):

    def _get_base_payload(self):
        return {
            "start_dtg": "2025-06-01T00:00:00Z",
            "end_dtg":   "2025-06-02T00:00:00Z",
            "data_source_type": "file",
            "data_source": "/tmp/x",
        }

    # --- parsing / defaults -------------------------------------------------
    def test_cli_roundtrip_defaults(self):
        cli = (
            "--start-dtg 2025-06-01T00:00:00Z "
            "--end-dtg   2025-06-02T00:00:00Z "
            "--data-source-type file "
            "--data-source /tmp/x"
        )
        raw   = C.parse_and_validate_input(cli)       # end-to-end helper
        again = C.parse_and_validate_input(raw)       # idempotent
        self.assertEqual(raw, again)                  # no mutation

    def test_dataframe_data_source(self):
        raw = self._get_base_payload()
        raw["data_source"] = "dataframe"
        out = C.parse_and_validate_input(raw)
        self.assertTrue(out["data_source"] == "dataframe")

    # --- deref JSON file ----------------------------------------------------
    def test_external_file_dereference(self):
        tmp = tmp_json({"p": 1})
        raw = self._get_base_payload()
        raw["analytic_parameters"] = str(tmp)
        out = C.parse_and_validate_input(raw)
        self.assertEqual(out["analytic_parameters"], {"p": 1})
        tmp.unlink()

    def test_invalid_type_for_dereferenceable_field(self):
        raw = self._get_base_payload()
        # This type is invalid (neither string nor object) and should fail validation.
        raw["analytic_parameters"] = 12345
        with self.assertRaises(SchemaError):
            C.parse_and_validate_input(raw)


    # --- integration: create + finalise Document ---------------------------
    def test_document_end_to_end(self):
        # minimal valid params
        params = C.parse_and_validate_input(self._get_base_payload())
        
        # A minimal but schema-compliant finding
        finding = {
            "finding_id": "1",
            "title": "Test Finding",
            "description": "A test finding.",
            "event_dtg": "2025-08-03T12:00:00Z",
            "severity": "low",
            "confidence": "low",
            "observables": [],
            "mitre_attack_tactics": [],
            "mitre_attack_techniques": [],
            "recommended_actions": "None",
            "recommended_pivots": "None",
            "classification": "U"
        }

        doc = C.create_document(
            analytic_id="0",
            analytic_name="Unit test analytic",
            analytic_version="1.0",
            input_schema_version="1.0.0",
            output_schema_version="1.0.0",
            inputs=params,
            findings=[finding],
            status="success",
            exit_code=0,
            analytic_description="Test analytic description",
            author="Test author",
            author_organization="Test author organization",
            contact="Test contact",
            data_schema={c: "float" for c in ["length", "width"]},
            dataset_description="Test dataset description",
            dataset_hash="00",
            dataset_size=42,
            documentation_link="Test documentation link",
            feature_names=['feature 1, feature 2'],
            license="Test license"
        )
        doc.add_message("INFO", "integration-test")
        time.sleep(0.01) # ensure runtime > 0
        doc.finalise()

        with tmp_dir() as td:
            out_path = td / "doc.json"
            doc.save(out_path)
            self.assertTrue(out_path.is_file())
            loaded = json.loads(out_path.read_text())
            self.assertIn("total_runtime_seconds", loaded)
            self.assertIn("input_hash", loaded)
            self.assertIn("findings_hash", loaded)
            self.assertIsNotNone(loaded["input_hash"])
            self.assertIsNotNone(loaded["findings_hash"])

    def test_create_document_with_invalid_data_fails(self):
        doc = C.create_document(analytic_id=123) # Wrong type
        with self.assertRaises(SchemaError):
            doc.finalise() # Validation happens on finalise