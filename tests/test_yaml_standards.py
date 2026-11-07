"""Tests verifying ruamel.yaml safe loader standards in contract-schema."""
import tempfile
import unittest
from pathlib import Path

from contract_schema import loader, parser
from contract_schema.parser import _safe_load_yaml


class RuamelYamlStandardsTests(unittest.TestCase):
    def setUp(self):
        self.schema = loader.load_schema("analytic_schema.json")["input"]

    def test_scalar_types_and_yaml11_support(self):
        content = "enabled: on\noff_flag: off\ncount: 012\n"
        data = _safe_load_yaml(content)
        self.assertEqual(data, {"enabled": True, "off_flag": False, "count": 10})

    def test_parse_input_via_ruamel(self):
        content = """
start_dtg: "2025-08-03T00:00:00Z"
end_dtg: "2025-08-03T01:00:00Z"
data_source_type: file
data_source: /tmp/data.csv
"""
        with tempfile.NamedTemporaryFile("w+", suffix=".yaml", delete=False) as tmp:
            tmp.write(content)
            tmp.flush()
            p = Path(tmp.name)
        try:
            out = parser.parse_input(p, schema=self.schema)
            self.assertEqual(out["data_source_type"], "file")
            self.assertEqual(out["data_source"], "/tmp/data.csv")
        finally:
            p.unlink(missing_ok=True)
