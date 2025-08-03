import json
import tempfile
import unittest
from pathlib import Path

from contract_schema import loader, parser


class ParserTests(unittest.TestCase):
    def setUp(self):
        self.schema = loader.load_schema("analytic_schema.json")["input"]
        self.start = "2025-08-03T00:00:00Z"
        self.end   = "2025-08-03T01:00:00Z"
        self.base  = {
            "start_dtg":       self.start,
            "end_dtg":         self.end,
            "data_source_type": "file",
            "data_source":      "/tmp/data.csv",
        }

    def test_parse_mapping(self):
        out = parser.parse_input(self.base, schema=self.schema)
        self.assertEqual(out, self.base)

    def test_parse_path(self):
        with tempfile.NamedTemporaryFile("w+", delete=False) as tmp:
            json.dump(self.base, tmp)
            tmp.flush()
            p = Path(tmp.name)
        try:
            out = parser.parse_input(p, schema=self.schema)
            self.assertEqual(out, self.base)
        finally:
            p.unlink(missing_ok=True)

    def test_parse_json_literal(self):
        literal = json.dumps(self.base)
        out = parser.parse_input(literal, schema=self.schema)
        self.assertEqual(out, self.base)

    def test_parse_cli_tokens(self):
        cli = [
            "--start-dtg", self.start,
            "--end-dtg",   self.end,
            "--data-source-type", "file",
            "--data-source", "/tmp/data.csv",
        ]
        out = parser.parse_input(cli, schema=self.schema)
        self.assertEqual(out, self.base)

    def test_unknown_argument_raises(self):
        with self.assertRaises(ValueError):
            parser.parse_input(["--unknown", "x"], schema=self.schema)
