import copy
import json
import tempfile
import unittest
from pathlib import Path

import yaml

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

    def test_parse_and_validate_accepts_yaml_file_input(self):
        with tempfile.NamedTemporaryFile("w+", suffix=".yaml", delete=False) as tmp:
            yaml.safe_dump(self.payload, tmp)
            tmp.flush()
            p = Path(tmp.name)

        try:
            out = self.contract.parse_and_validate_input(p)
            for k in self.payload:
                self.assertEqual(out[k], self.payload[k])
            self.assertEqual(out["log_path"], "stdout")
            self.assertEqual(out["output"], "stdout")
            self.assertEqual(out["verbosity"], "INFO")
        finally:
            p.unlink(missing_ok=True)

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

    def test_analytic_plans_contract_accepts_azure_cloud_and_aws_ids(self):
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

        aws_matrix = copy.deepcopy(self.payload)
        aws_matrix["analytic_plans"][0]["indicators"][0]["technique_id"] = "AT1667"
        self.assertEqual(
            self.contract.parse_and_validate_input(aws_matrix)["analytic_plans"][0]["indicators"][0]["technique_id"],
            "AT1667",
        )

        aws_subtechnique = copy.deepcopy(self.payload)
        aws_subtechnique["analytic_plans"][0]["indicators"][0]["technique_id"] = "AT1667.001"
        self.assertEqual(
            self.contract.parse_and_validate_input(aws_subtechnique)["analytic_plans"][0]["indicators"][0]["technique_id"],
            "AT1667.001",
        )

    def test_analytic_plans_contract_allows_empty_analytic_log_source_references(self):
        payload = copy.deepcopy(self.payload)
        payload["analytic_plans"][0]["indicators"][0]["technique_analytics"]["AN1543"][
            "analytic_log_source_references"
        ] = []

        out = self.contract.parse_and_validate_input(payload)
        self.assertEqual(
            out["analytic_plans"][0]["indicators"][0]["technique_analytics"]["AN1543"][
                "analytic_log_source_references"
            ],
            [],
        )

    def test_analytic_plans_contract_allows_empty_analytics_and_detection_strategies(self):
        payload = copy.deepcopy(self.payload)
        payload["analytic_plans"][0]["indicators"][0]["technique_analytics"] = {}
        payload["analytic_plans"][0]["indicators"][0]["detection_strategies"] = {}

        out = self.contract.parse_and_validate_input(payload)
        self.assertEqual(out["analytic_plans"][0]["indicators"][0]["technique_analytics"], {})
        self.assertEqual(out["analytic_plans"][0]["indicators"][0]["detection_strategies"], {})


class D3FendAnalyticPlansContractTests(unittest.TestCase):
    def setUp(self):
        self.contract = Contract.load("analytic_plans_d3fend.json")
        self.payload = {
            "analytic_plans": [
                {
                    "information_requirement": (
                        "What is the security posture of our enterprise regarding "
                        "active logical link mapping? (FFIR)"
                    ),
                    "tactic_id": "D3-D",
                    "tactic_name": "Detect",
                    "indicators": [
                        {
                            "technique_id": "D3-ALLM",
                            "technique_name": "Active Logical Link Mapping",
                            "technique_d3fend_label": "Active Logical Link Mapping",
                            "technique_d3fend_definition": (
                                "Active logical link mapping establishes awareness "
                                "of logical links in the network."
                            ),
                            "technique_d3fend_subclasses": ["d3f:LogicalLinkMapping"],
                            "technique_d3fend_kb_references": [
                                {
                                    "reference_id": "d3f:Reference-Example",
                                    "reference_label": "Reference - Example",
                                    "reference_title": "Example Reference",
                                    "reference_link": "https://d3fend.mitre.org/example"
                                }
                            ],
                            "evidence": [
                                {
                                    "description": "Logical link inventory data exists for critical enclaves.",
                                    "data_sources": ["Zeek conn.log"],
                                    "data_platforms": ["TBD"],
                                    "nai": "Core switching and routing segments",
                                    "action": {
                                        "Build authoritative logical link inventory": (
                                            "Symbolic Logic: Enumerate observed logical links "
                                            "and compare them to the approved baseline."
                                        ),
                                        "Measure unexpected link prevalence": (
                                            "Statistical Method: Track the daily rate of new "
                                            "logical links and alert on deviations from baseline."
                                        ),
                                        "Classify anomalous graph edges": (
                                            "Machine Learning: Score network graph edges for "
                                            "anomalous connectivity patterns."
                                        )
                                    }
                                }
                            ]
                        }
                    ],
                    "last_updated": "2026-04-06",
                    "version": "1.0",
                    "date_created": "2025-10-09",
                    "contributors": ["Zachary Szewczyk"]
                }
            ]
        }

    def test_d3fend_analytic_plans_contract_parses_valid_payload(self):
        out = self.contract.parse_and_validate_input(self.payload)
        self.assertEqual(out, self.payload)

    def test_d3fend_contract_rejects_attack_only_fields_as_substitutes(self):
        bad = copy.deepcopy(self.payload)
        indicator = bad["analytic_plans"][0]["indicators"][0]
        indicator.pop("technique_name")
        indicator["name"] = "Active Logical Link Mapping"
        with self.assertRaisesRegex(SchemaError, "missing required"):
            self.contract.parse_and_validate_input(bad)
