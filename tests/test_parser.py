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
            "start_dtg":        self.start,
            "end_dtg":          self.end,
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

    def test_parse_cli_with_integer(self):
        # Add a temporary integer field to schema for this test
        schema = self.schema.copy()
        schema["fields"]["num_records"] = {"type": ["integer"]}
        cli = ["--num-records", "100"]
        out = parser.parse_input(cli, schema=schema)
        self.assertEqual(out["num_records"], 100)

    def test_parse_cli_with_boolean_flag(self):
        schema = self.schema.copy()
        schema["fields"]["enable_feature"] = {"type": ["boolean"]}
        cli_with_flag = ["--enable-feature"]
        cli_without_flag = []
        
        out_with_flag = parser.parse_input(cli_with_flag, schema=schema)
        self.assertTrue(out_with_flag["enable_feature"])

        out_without_flag = parser.parse_input(cli_without_flag, schema=schema)
        self.assertNotIn("enable_feature", out_without_flag)


    def test_unknown_argument_raises(self):
        with self.assertRaises(ValueError):
            parser.parse_input(["--unknown", "x"], schema=self.schema)

    def test_parse_unsupported_type_raises(self):
        with self.assertRaises(TypeError):
            parser.parse_input(12345, schema=self.schema)

class ParserConfigTests(unittest.TestCase):
    def setUp(self):
        self.schema = loader.load_schema("analytic_schema.json")["input"]
        self.base   = {
            "start_dtg":        "2025-01-01T00:00:00Z",
            "end_dtg":          "2025-01-02T00:00:00Z",
            "data_source_type": "file",
            "data_source":      "/tmp/x",
        }

    def test_config_flag_overrides_everything(self):
        with tempfile.NamedTemporaryFile("w+", delete=False) as tmp:
            json.dump(self.base, tmp)
            tmp.flush()
            cfg = Path(tmp.name)

        try:
            cli = ["--config", str(cfg), "--start-dtg", "BAD"]  # ignored
            out = parser.parse_input(cli, schema=self.schema)
            self.assertEqual(out, self.base)
        finally:
            cfg.unlink(missing_ok=True)

    def test_config_file_not_found_raises(self):
        cli = ["--config", "/tmp/does-not-exist.json"]
        with self.assertRaises(FileNotFoundError):
            parser.parse_input(cli, schema=self.schema)

    def test_config_file_bad_json_raises(self):
        with tempfile.NamedTemporaryFile("w+", delete=False) as tmp:
            tmp.write("{ not json")
            tmp.flush()
            cfg = Path(tmp.name)
        try:
            cli = ["--config", str(cfg)]
            with self.assertRaises(json.JSONDecodeError):
                parser.parse_input(cli, schema=self.schema)
        finally:
            cfg.unlink(missing_ok=True)