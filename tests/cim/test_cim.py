"""Tests for the analytic_results_cim contract."""
from __future__ import annotations

import json
import time
import unittest
from typing import Any

from tests._util import CIM_C as C, tmp_dir
from contract_schema.validator import SchemaError


def _base_inputs() -> dict[str, Any]:
    return {
        "start_dtg": "2026-05-11T13:00:00Z",
        "end_dtg":   "2026-05-11T14:00:00Z",
        "data_source_type": "file",
        "data_source": "/tmp/x",
    }


def _base_row() -> dict[str, Any]:
    return {
        "result_id": "ar_test_001",
        "analytic_run_id": "run_test_001",
        "dtg_utc": "2026-05-11T14:15:22Z",
        "observation_start_utc": "2026-05-11T13:00:00Z",
        "observation_end_utc":   "2026-05-11T14:00:00Z",
        "run_username": "tester",
        "analytic_id": "a-001",
        "analytic_name": "Suspicious Scheduled Task Creation",
        "analytic_description": "Identifies suspicious scheduled task creation.",
        "analytic_version": "1.0.0",
        "mitre_attack_domain": "enterprise",
        "mitre_tactic_id": "TA0002",
        "mitre_tactic_name": "Execution",
        "mitre_technique_id": "T1053.005",
        "mitre_technique_name": "Scheduled Task/Job: Scheduled Task",
        "mitre_mapping_confidence": "high",
        "result_severity": "high",
        "behavior_likelihood": "likely",
        "analytic_confidence": "moderate",
        "result_status": "new",
        "validation_state": "candidate",
        "environment_domain": "host",
        "primary_entity_type": "host",
        "evidence_summary": "Scheduled task created by non-admin user.",
    }


def _make_doc(rows):
    inputs = C.parse_and_validate_input(_base_inputs())
    return C.create_document(
        cim_schema_name="analytic_results_cim",
        cim_schema_version="1.0.0",
        input_schema_version=C.version,
        output_schema_version=C.version,
        inputs=inputs,
        results=rows,
        status="success",
        exit_code=0,
        author="tester",
        author_organization="org",
        contact="t@example.com",
        license="MIT",
        documentation_link="https://example.com",
    )


class CIMContractTests(unittest.TestCase):

    # --- contract metadata --------------------------------------------------
    def test_contract_loads(self):
        self.assertEqual(C.title, "Analytic Results CIM")
        self.assertTrue(C.version)

    # --- input parsing ------------------------------------------------------
    def test_input_defaults_applied(self):
        out = C.parse_and_validate_input(_base_inputs())
        self.assertEqual(out["log_path"], "stdout")
        self.assertEqual(out["verbosity"], "INFO")

    # --- end-to-end document ------------------------------------------------
    def test_document_roundtrip_minimal_row(self):
        doc = _make_doc([_base_row()])
        time.sleep(0.01)
        doc.finalise()
        with tmp_dir() as td:
            out_path = td / "doc.json"
            doc.save(out_path)
            loaded = json.loads(out_path.read_text())
        self.assertIn("results_hash", loaded)
        self.assertIn("input_hash", loaded)
        self.assertEqual(loaded["cim_schema_name"], "analytic_results_cim")
        self.assertEqual(len(loaded["results"]), 1)

    def test_results_hash_changes_with_payload(self):
        doc1 = _make_doc([_base_row()])
        doc1.finalise()
        row2 = _base_row()
        row2["result_id"] = "ar_test_002"
        doc2 = _make_doc([row2])
        doc2.finalise()
        self.assertNotEqual(doc1["results_hash"], doc2["results_hash"])

    def test_one_row_per_technique_pattern(self):
        # Same finding, two techniques -> two rows.
        row_a = _base_row()
        row_b = _base_row()
        row_b["result_id"] = "ar_test_002"
        row_b["mitre_technique_id"] = "T1059.001"
        row_b["mitre_technique_name"] = "Command and Scripting Interpreter: PowerShell"
        doc = _make_doc([row_a, row_b])
        doc.finalise()
        self.assertEqual(len(doc["results"]), 2)

    # --- pattern enforcement ------------------------------------------------
    def test_invalid_tactic_id_rejected(self):
        bad = _base_row()
        bad["mitre_tactic_id"] = "TAxxxx"
        doc = _make_doc([bad])
        with self.assertRaises(SchemaError):
            doc.finalise()

    def test_invalid_technique_id_rejected(self):
        bad = _base_row()
        bad["mitre_technique_id"] = "T123"
        doc = _make_doc([bad])
        with self.assertRaises(SchemaError):
            doc.finalise()

    def test_subtechnique_id_accepted(self):
        row = _base_row()
        row["mitre_subtechnique_id"] = "T1059.003"
        row["mitre_subtechnique_name"] = "Windows Command Shell"
        doc = _make_doc([row])
        doc.finalise()
        self.assertEqual(doc["results"][0]["mitre_subtechnique_id"], "T1059.003")

    # --- enum enforcement ---------------------------------------------------
    def test_invalid_environment_domain_rejected(self):
        bad = _base_row()
        bad["environment_domain"] = "mainframe"
        doc = _make_doc([bad])
        with self.assertRaises(SchemaError):
            doc.finalise()

    def test_invalid_validation_state_rejected(self):
        bad = _base_row()
        bad["validation_state"] = "approved"
        doc = _make_doc([bad])
        with self.assertRaises(SchemaError):
            doc.finalise()

    # --- nullable optional fields ------------------------------------------
    def test_nullable_optional_fields_accept_null(self):
        row = _base_row()
        row["primary_entity_id"] = None
        row["src_ip"] = None
        row["result_score"] = None
        row["evidence_count"] = None
        doc = _make_doc([row])
        doc.finalise()  # should not raise

    # --- required fields ----------------------------------------------------
    def test_missing_required_row_field_rejected(self):
        bad = _base_row()
        del bad["evidence_summary"]
        doc = _make_doc([bad])
        with self.assertRaises(SchemaError):
            doc.finalise()

    # --- coverage of example domains ---------------------------------------
    def test_cloud_row_validates(self):
        row = _base_row()
        row["analytic_name"] = "Suspicious IAM Policy Change"
        row["mitre_tactic_id"] = "TA0004"
        row["mitre_tactic_name"] = "Privilege Escalation"
        row["mitre_technique_id"] = "T1098"
        row["mitre_technique_name"] = "Account Manipulation"
        row["environment_domain"] = "cloud"
        row["cloud_provider"] = "aws"
        row["cloud_account_id"] = "123456789012"
        row["primary_entity_type"] = "cloud_principal"
        row["cloud_principal_name"] = "ci-deploy-role"
        row["cloud_api_action"] = "iam:AttachUserPolicy"
        row["cloud_resource_type"] = "iam_policy"
        row["result_severity"] = "critical"
        _make_doc([row]).finalise()

    def test_ot_row_validates(self):
        row = _base_row()
        row["mitre_attack_domain"] = "ics"
        row["environment_domain"] = "ot"
        row["network_zone"] = "ot-cell-3"
        row["primary_entity_type"] = "ot_asset"
        row["ot_asset_type"] = "plc"
        row["ot_protocol"] = "modbus"
        row["ot_function_code"] = "write_multiple_registers"
        row["src_hostname"] = "ENG-WS-02"
        row["dst_ip"] = "172.16.50.12"
        row["result_severity"] = "critical"
        row["analytic_confidence"] = "high"
        # ICS profile uses TA0105 (Inhibit Response Function) etc.; just keep
        # a valid TA pattern here.
        row["mitre_tactic_id"] = "TA0106"
        row["mitre_tactic_name"] = "Impair Process Control"
        row["mitre_technique_id"] = "T0831"
        row["mitre_technique_name"] = "Manipulation of Control"
        _make_doc([row]).finalise()


if __name__ == "__main__":
    unittest.main()
