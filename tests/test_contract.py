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


class AnalyticPlansContractTests(unittest.TestCase):
    def setUp(self):
        self.contract = Contract.load("analytic_plans.json")
        self.payload = {
            "analytic_plans": [
                {
                    "information_requirement": "Has the adversary gained initial access using valid accounts? (PIR)",
                    "tactic_id": "TA0001",
                    "tactic_name": "Initial Access",
                    "tactic_description": "The adversary is trying to get into your network.",
                    "tactic_url": "https://attack.mitre.org/tactics/TA0001",
                    "tactic_created": "2018-10-17 00:14:20.652000+00:00",
                    "tactic_last_modified": "2025-04-25 14:45:36.917000+00:00",
                    "tactic_domain": ["enterprise-attack"],
                    "tactic_version": "1.0",
                    "indicators": [
                        {
                            "technique_id": "T1078",
                            "technique_name": "Valid Accounts",
                            "technique_description": "Adversaries may obtain and abuse credentials of existing accounts.",
                            "technique_url": "https://attack.mitre.org/techniques/T1078",
                            "technique_created": "2017-05-31 21:31:00.645000+00:00",
                            "technique_last_modified": "2025-10-24 17:49:14.095000+00:00",
                            "technique_domain": ["enterprise-attack"],
                            "technique_version": "2.8",
                            "technique_platforms": ["Windows", "Linux"],
                            "technique_analytics": {
                                "AN1543": {
                                    "analytic_name": "Analytic 1543",
                                    "analytic_description": "Detect compromised account use from unusual contexts.",
                                    "analytic_url": "https://attack.mitre.org/detectionstrategies/DET0560#AN1543",
                                    "analytic_created": "2025-10-21 15:10:28.402000+00:00",
                                    "analytic_last_modified": "2025-11-12 22:03:39.105000+00:00",
                                    "analytic_domain": ["enterprise-attack"],
                                    "analytic_version": "1.0",
                                    "analytic_log_source_references": [
                                        {
                                            "data_component_id": "DC0067",
                                            "data_component_title": "Logon Session Creation",
                                            "log_source_name": "WinEventLog:Security",
                                            "log_source_channel": "EventCode=4624"
                                        }
                                    ]
                                }
                            },
                            "detection_strategies": {
                                "DET0560": {
                                    "detection_strategy_name": "Detection of Valid Account Abuse Across Platforms",
                                    "detection_strategy_url": "https://attack.mitre.org/detectionstrategies/DET0560",
                                    "detection_strategy_created": "2025-10-21 15:10:28.402000+00:00",
                                    "detection_strategy_last_modified": "2025-10-21 15:10:28.402000+00:00",
                                    "detection_strategy_domain": ["enterprise-attack"],
                                    "detection_strategy_version": "1.0"
                                }
                            },
                            "evidence": [
                                {
                                    "description": "A successful remote authentication uses an account found in a breach dataset.",
                                    "data_sources": ["Windows Event ID 4624", "Zeek conn.log"],
                                    "data_platforms": ["TBD"],
                                    "nai": "Externally-facing authentication services",
                                    "action": {
                                        "Breached-account remote-login match": "Symbolic Logic: Alert on a remote login from a breached account.",
                                        "Novel source prevalence score": "Statistical Method: Flag remote logins from rare source IPs for the user.",
                                        "Credential misuse classifier": "Machine Learning: Score authentication events for likely credential abuse."
                                    }
                                }
                            ]
                        }
                    ],
                    "last_updated": "2026-04-06",
                    "version": "1.0",
                    "date_created": "2025-05-04",
                    "contributors": ["Zachary Szewczyk"]
                }
            ]
        }

    def test_analytic_plans_contract_parses_valid_payload(self):
        out = self.contract.parse_and_validate_input(self.payload)
        self.assertEqual(out, self.payload)

    def test_analytic_plans_contract_rejects_bad_dynamic_key(self):
        bad = copy.deepcopy(self.payload)
        bad["analytic_plans"][0]["indicators"][0]["technique_analytics"]["BAD1543"] = (
            bad["analytic_plans"][0]["indicators"][0]["technique_analytics"].pop("AN1543")
        )
        with self.assertRaisesRegex(SchemaError, "propertyNamesPattern"):
            self.contract.parse_and_validate_input(bad)

    def test_analytic_plans_contract_accepts_azure_and_cloud_attack_ids(self):
        azure = copy.deepcopy(self.payload)
        azure["analytic_plans"][0]["indicators"][0]["technique_id"] = "AZT505"
        self.assertEqual(
            self.contract.parse_and_validate_input(azure)["analytic_plans"][0]["indicators"][0]["technique_id"],
            "AZT505",
        )

        aws = copy.deepcopy(self.payload)
        aws["analytic_plans"][0]["indicators"][0]["technique_id"] = "T1070.A001"
        self.assertEqual(
            self.contract.parse_and_validate_input(aws)["analytic_plans"][0]["indicators"][0]["technique_id"],
            "T1070.A001",
        )
