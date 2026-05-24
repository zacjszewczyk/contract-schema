"""Targeted tests for the v1.5.0 ``results.results_payload`` field.

The pre-existing tests in ``test_cim.py`` were authored against an
earlier schema revision and currently fail for unrelated reasons
(`results` is now a required object on each row). These tests
exercise only the new payload field so a regression there shows up
cleanly even while the legacy suite remains red.
"""
from __future__ import annotations

import json
import unittest

from contract_schema.validator import SchemaError, validate
from tests._util import CIM_C as C


def _row_with_results(results_obj: dict) -> dict:
    """Slim CIM v1.5.0 row -- only the fields the contract actually accepts."""
    return {
        "result_id": "ar_payload_001",
        "analytic_run_id": "run_payload_001",
        "dtg_utc": "2026-05-12T00:00:00Z",
        "observation_start_utc": "2026-05-11T00:00:00Z",
        "observation_end_utc":   "2026-05-12T00:00:00Z",
        "run_username": "tester",
        "analytic_name": "Payload coverage analytic",
        "analytic_description": "Exercises results.results_payload.",
        "mitre_tactic_id": "TA0006",
        "mitre_tactic_name": "Credential Access",
        "mitre_technique_id": "T1003.001",
        "mitre_technique_name": "OS Credential Dumping: LSASS Memory",
        "environment_domain": "host",
        "primary_entity_type": "host",
        "results": results_obj,
    }


def _row_schema():
    return C.output_schema["fields"]["results"]["items"]


class ResultsPayloadTests(unittest.TestCase):
    def test_string_payload_validates(self):
        payload = json.dumps({"hostname": "WIN-01", "score": 95})
        row = _row_with_results({
            "host": {"hostname": "WIN-01"},
            "results_payload": payload,
        })
        validate(row, schema=_row_schema(), path="row")

    def test_null_payload_allowed(self):
        row = _row_with_results({
            "host": {"hostname": "WIN-01"},
            "results_payload": None,
        })
        validate(row, schema=_row_schema(), path="row")

    def test_payload_omittable(self):
        row = _row_with_results({"host": {"hostname": "WIN-01"}})
        validate(row, schema=_row_schema(), path="row")

    def test_non_string_non_null_payload_rejected(self):
        row = _row_with_results({
            "host": {"hostname": "WIN-01"},
            "results_payload": {"not": "a string"},
        })
        with self.assertRaises(SchemaError):
            validate(row, schema=_row_schema(), path="row")

    def test_unknown_results_subfield_still_rejected(self):
        # additionalProperties on the inner `results` object should
        # still bar arbitrary keys -- only `host`, `net`, `cloud`, `ot`,
        # and the new `results_payload` are allowed.
        row = _row_with_results({
            "host": {"hostname": "WIN-01"},
            "made_up_bucket": {"foo": "bar"},
        })
        with self.assertRaises(SchemaError):
            validate(row, schema=_row_schema(), path="row")

    def test_schema_version_reflects_payload_addition(self):
        # Bumped to 1.6.0 in this revision (standardised common header
        # `_timestamp` / `hostname` / `ip` / `user` across every typed
        # bucket plus new identity / container buckets).
        self.assertEqual(C.version, "1.6.0")

    def test_identity_and_container_buckets_validate(self):
        row = _row_with_results({
            "identity": {
                "_timestamp": "2026-05-12T00:00:00Z",
                "hostname": "dc-01",
                "user": "alice",
                "logon_type": "remote-interactive",
                "logon_status": "success",
                "mfa_used": True,
            },
            "container": {
                "_timestamp": "2026-05-12T00:00:00Z",
                "hostname": "node-3",
                "container_id": "deadbeef",
                "container_image": "nginx:1.25",
                "namespace": "default",
                "pod_name": "web-7",
            },
        })
        validate(row, schema=_row_schema(), path="row")

    def test_common_header_present_on_every_bucket(self):
        row = _row_with_results({
            "host":      {"_timestamp": "2026-05-12T00:00:00Z", "hostname": "h", "ip": "1.1.1.1", "user": "u", "process_name": "p.exe"},
            "net":       {"_timestamp": "2026-05-12T00:00:00Z", "hostname": "h", "ip": "1.1.1.1", "user": "u", "src_ip": "2.2.2.2", "dst_ip": "3.3.3.3"},
            "cloud":     {"_timestamp": "2026-05-12T00:00:00Z", "hostname": "h", "ip": "1.1.1.1", "user": "u", "provider": "aws"},
            "ot":        {"_timestamp": "2026-05-12T00:00:00Z", "hostname": "h", "ip": "1.1.1.1", "user": "u", "asset_id": "PLC-1"},
            "identity":  {"_timestamp": "2026-05-12T00:00:00Z", "hostname": "h", "ip": "1.1.1.1", "user": "u", "logon_status": "success"},
            "container": {"_timestamp": "2026-05-12T00:00:00Z", "hostname": "h", "ip": "1.1.1.1", "user": "u", "container_id": "x"},
        })
        validate(row, schema=_row_schema(), path="row")


if __name__ == "__main__":
    unittest.main()
