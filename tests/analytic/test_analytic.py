import json, unittest, time
from pathlib import Path
import pandas as pd

from tests._util import ANALYTIC_C as C, tmp_json, with_defaults, tmp_dir

DF = pd.DataFrame({"v": [1, 2]})

class AnalyticContractTests(unittest.TestCase):

    # --- parsing / defaults -------------------------------------------------
    def test_cli_roundtrip_defaults(self):
        cli = (
            "--start-dtg 2025-06-01T00:00:00Z "
            "--end-dtg   2025-06-02T00:00:00Z "
            "--data-source-type file "
            "--data-source /tmp/x"
        )
        raw   = C.parse_and_validate_input(cli)      # end-to-end helper
        again = C.parse_and_validate_input(raw)      # idempotent
        self.assertEqual(raw, again)                 # no mutation

    def test_dataframe_data_source(self):
        raw = {
            "input_schema_version": "1.0.0",
            "start_dtg": "2025-06-01T00:00:00Z",
            "end_dtg":   "2025-06-02T00:00:00Z",
            "data_source_type": "df",
            "data_source": DF,
        }
        out = C.parse_and_validate_input(raw)
        self.assertTrue(out["data_source"].equals(DF))

    # --- deref JSON file ----------------------------------------------------
    def test_external_file_dereference(self):
        tmp = tmp_json({"p": 1})
        raw = {
            "input_schema_version": "1.0.0",
            "start_dtg": "2025-06-01T00:00:00Z",
            "end_dtg":   "2025-06-02T00:00:00Z",
            "data_source_type": "file",
            "data_source": "/tmp/x",
            "analytic_parameters": str(tmp),
        }
        out = C.parse_and_validate_input(raw)
        self.assertEqual(out["analytic_parameters"], {"p": 1})
        tmp.unlink()

    # --- integration: create + finalise Document ---------------------------
    def test_document_end_to_end(self):
        # minimal valid params
        params = C.parse_and_validate_input({
            "input_schema_version": "1.0.0",
            "start_dtg": "2025-06-01T00:00:00Z",
            "end_dtg":   "2025-06-02T00:00:00Z",
            "data_source_type": "file",
            "data_source": "/tmp/x"
        })

        doc = C.create_document(
            analytic_id="0",
            analytic_name="Unit test analytic",
            analytic_version="1.0",
            input_schema_version="1.0.0",
            output_schema_version="1.0.0",
            inputs=params,
            findings=[],
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
