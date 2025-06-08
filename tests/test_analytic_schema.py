import unittest
import tempfile
import json
import copy
from pathlib import Path
import pandas as pd

from analytic_schema.parser    import parse_input
from analytic_schema.validator import validate_input, SchemaError, _DEFAULTS
from analytic_schema.output    import OutputDoc

class _Util:
    @staticmethod
    def tmp_json(obj):
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
        Path(tmp.name).write_text(json.dumps(obj), encoding="utf-8")
        return Path(tmp.name)

    @staticmethod
    def with_defaults(base: dict):
        merged = base.copy()
        for k, v in _DEFAULTS.items():
            merged.setdefault(k, copy.deepcopy(v))
        return merged

class AnalyticSchemaTests(unittest.TestCase):
    # 01 CLI → defaults
    def test_01_cli_roundtrip_defaults(self):
        cli = (
            "--input-schema-version 1.0.0 "
            "--start-dtg 2025-06-01T00:00:00Z "
            "--end-dtg 2025-06-02T00:00:00Z "
            "--data-source-type file "
            "--data-source /tmp/conn.csv"
        )
        raw = parse_input(cli)
        canon = validate_input(raw)
        self.assertDictEqual(canon, _Util.with_defaults(raw))

    # 02 inline analytic_parameters
    def test_02_dict_with_analytic_parameters_dereferenced(self):
        raw = {
            "input_schema_version": "1.0.0",
            "start_dtg": "2025-06-01T00:00:00Z",
            "end_dtg":   "2025-06-02T00:00:00Z",
            "data_source_type": "file",
            "data_source": "/tmp/conn.csv",
            "analytic_parameters": '{"param_a": 123}',
        }
        out = validate_input(parse_input(raw))
        self.assertEqual(out["analytic_parameters"], {"param_a": 123})

    # 03 DataFrame as data_source
    def test_03_dataframe_data_source_passes_validation(self):
        df = pd.DataFrame({"Name": ["Alice", "Bob"]})
        raw = {
            "input_schema_version": "1.0.0",
            "start_dtg": "2025-06-01T00:00:00Z",
            "end_dtg":   "2025-06-02T00:00:00Z",
            "data_source_type": "df",
            "data_source": df,
        }
        out = validate_input(parse_input(raw))
        self.assertTrue(out["data_source"].equals(df))

    # 04 --config precedence
    def test_04_config_file_overrides_cli(self):
        cfg = {
            "input_schema_version": "1.0.0",
            "start_dtg": "2025-07-01T00:00:00Z",
            "end_dtg":   "2025-07-02T00:00:00Z",
            "data_source_type": "api endpoint",
            "data_source": "https://api.example.com/data",
        }
        cfg_path = _Util.tmp_json(cfg)
        out = validate_input(parse_input(["--config", str(cfg_path)]))
        self.assertDictEqual(out, _Util.with_defaults(cfg))

    # 05 OutputDoc defaults & hashes
    def test_05_outputdoc_defaults_and_hashes(self):
        inputs = validate_input({
            "input_schema_version": "1.0.0",
            "start_dtg":"2025-06-01T00:00:00Z",
            "end_dtg":"2025-06-02T00:00:00Z",
            "data_source_type":"file",
            "data_source":"/tmp/conn.csv"
        })
        doc = OutputDoc(input_data_hash="0"*64, inputs=inputs)
        doc.add_message("INFO","hello")
        doc.finalise()
        self.assertIn("input_hash", doc)
        self.assertIn("findings_hash", doc)
        self.assertEqual(doc["status"], "UNKNOWN")
        self.assertEqual(doc["messages"][0]["level"], "INFO")

    # 06 Missing required
    def test_06_validate_missing_required_field_raises(self):
        invalid = {
            "input_schema_version": "1.0.0",
            "start_dtg":"2025-06-01T00:00:00Z",
            "end_dtg":"2025-06-02T00:00:00Z",
            "data_source_type":"file",
        }
        with self.assertRaises(SchemaError):
            validate_input(parse_input(invalid))

    # 07 Invalid enum
    def test_07_invalid_enum_value_detected(self):
        bad = {
            "input_schema_version":"1.0.0",
            "start_dtg":"2025-06-01T00:00:00Z",
            "end_dtg":"2025-06-02T00:00:00Z",
            "data_source_type":"ftp",
            "data_source":"/tmp/x",
        }
        with self.assertRaises(SchemaError):
            validate_input(parse_input(bad))

    # 08 Bad datetime
    def test_08_invalid_datetime_format_rejected(self):
        bad = {
            "input_schema_version":"1.0.0",
            "start_dtg":"2025-06-01",
            "end_dtg":"2025-06-02T00:00:00Z",
            "data_source_type":"file",
            "data_source":"/tmp/x",
        }
        with self.assertRaises(SchemaError):
            validate_input(parse_input(bad))

    # 09 analytic_parameters file
    def test_09_analytic_parameters_external_file_loaded(self):
        tmp = _Util.tmp_json({"p": 1})
        raw = {
            "input_schema_version":"1.0.0",
            "start_dtg":"2025-06-01T00:00:00Z",
            "end_dtg":"2025-06-02T00:00:00Z",
            "data_source_type":"file",
            "data_source":"/tmp/x",
            "analytic_parameters": str(tmp),
        }
        out = validate_input(parse_input(raw))
        self.assertEqual(out["analytic_parameters"], {"p":1})

    # 10 additionalProperties => reject extras
    def test_10_unknown_top_level_field_rejected(self):
        bad = {
            "input_schema_version":"1.0.0",
            "start_dtg":"2025-06-01T00:00:00Z",
            "end_dtg":"2025-06-02T00:00:00Z",
            "data_source_type":"file",
            "data_source":"/tmp/x",
            "oops": True
        }
        with self.assertRaises(SchemaError):
            validate_input(parse_input(bad))

    # 11 invalid JSON file
    def test_11_invalid_json_file_raises_decode_error(self):
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
        Path(tmp.name).write_text("NOT_JSON", encoding="utf-8")
        bad = {
            "input_schema_version":"1.0.0",
            "start_dtg":"2025-06-01T00:00:00Z",
            "end_dtg":"2025-06-02T00:00:00Z",
            "data_source_type":"file",
            "data_source":"/tmp/x",
            "analytic_parameters": tmp.name
        }
        with self.assertRaises(json.JSONDecodeError):
            validate_input(parse_input(bad))

    # 12 plain string analytic_parameters
    def test_12_plain_string_analytic_parameters_valid(self):
        raw = {
            "input_schema_version":"1.0.0",
            "start_dtg":"2025-06-01T00:00:00Z",
            "end_dtg":"2025-06-02T00:00:00Z",
            "data_source_type":"file",
            "data_source":"/tmp/x",
            "analytic_parameters":"flag"
        }
        out = validate_input(parse_input(raw))
        self.assertEqual(out["analytic_parameters"], "flag")

    # 13 lowercase enum => reject
    def test_13_lowercase_enum_value_rejected(self):
        bad = {
            "input_schema_version":"1.0.0",
            "start_dtg":"2025-06-01T00:00:00Z",
            "end_dtg":"2025-06-02T00:00:00Z",
            "data_source_type":"file",
            "data_source":"/tmp/x",
            "verbosity":"info"
        }
        with self.assertRaises(SchemaError):
            validate_input(parse_input(bad))

    # 14 OutputDoc without inputs
    def test_14_outputdoc_finalise_without_inputs_raises(self):
        with self.assertRaises(SchemaError):
            OutputDoc(input_data_hash="0"*64).finalise()

    # 15 invalid log level
    def test_15_invalid_log_level_raises(self):
        inputs = validate_input({
            "input_schema_version":"1.0.0",
            "start_dtg":"2025-06-01T00:00:00Z",
            "end_dtg":"2025-06-02T00:00:00Z",
            "data_source_type":"file",
            "data_source":"/tmp/x",
        })
        doc = OutputDoc(input_data_hash="0"*64, inputs=inputs)
        with self.assertRaises(ValueError):
            doc.add_message("TRACE", "msg")

    # 16 malformed finding triggers
    def test_16_malformed_finding_triggers_schema_error(self):
        inputs = validate_input({
            "input_schema_version":"1.0.0",
            "start_dtg":"2025-06-01T00:00:00Z",
            "end_dtg":"2025-06-02T00:00:00Z",
            "data_source_type":"file",
            "data_source":"/tmp/x"
        })
        doc = OutputDoc(input_data_hash="0"*64,
                        inputs=inputs,
                        findings=[{"foo":"bar"}])
        with self.assertRaises(SchemaError):
            doc.finalise()

    # 17 non-string config flag
    def test_17_non_string_config_flag_type_error(self):
        with self.assertRaises(TypeError):
            validate_input({"config": 123})

    # 18 unknown CLI flag
    def test_18_unknown_cli_flag_detected(self):
        with self.assertRaises(ValueError):
            parse_input("--bad-flag 1")

    # 19 wrong type for data_source
    def test_19_data_source_wrong_type_schema_error(self):
        bad = {
            "input_schema_version":"1.0.0",
            "start_dtg":"2025-06-01T00:00:00Z",
            "end_dtg":"2025-06-02T00:00:00Z",
            "data_source_type":"df",
            "data_source":999
        }
        with self.assertRaises(SchemaError):
            validate_input(parse_input(bad))

    # 20 data_map file deref
    def test_20_data_map_external_file_loaded(self):
        tmp = _Util.tmp_json({"a":1})
        raw = {
            "input_schema_version":"1.0.0",
            "start_dtg":"2025-06-01T00:00:00Z",
            "end_dtg":"2025-06-02T00:00:00Z",
            "data_source_type":"file",
            "data_source":"/tmp/x",
            "data_map": str(tmp)
        }
        out = validate_input(parse_input(raw))
        self.assertEqual(out["data_map"], {"a":1})

    # 21 @response-file syntax
    def test_21_at_response_file_expansion(self):
        tokens = [
            "--input-schema-version", "1.0.0",
            "--start-dtg", "2025-06-01T00:00:00Z",
            "--end-dtg",   "2025-06-02T00:00:00Z",
            "--data-source-type", "file",
            "--data-source", "/tmp/x",
        ]
        tmp = tempfile.NamedTemporaryFile(delete=False, mode="w", encoding="utf-8")
        tmp.write("\n".join(tokens)); tmp.close()
        out = validate_input(parse_input(f"@{tmp.name}"))
        self.assertEqual(out["data_source_type"], "file")

    # 22 valid verbosity
    def test_22_valid_enum_value_accepts(self):
        raw = {
            "input_schema_version":"1.0.0",
            "start_dtg":"2025-06-01T00:00:00Z",
            "end_dtg":"2025-06-02T00:00:00Z",
            "data_source_type":"file",
            "data_source":"/tmp/x",
            "verbosity":"DEBUG"
        }
        self.assertEqual(validate_input(parse_input(raw))["verbosity"], "DEBUG")

    # 23 message timestamp & level
    def test_23_outputdoc_message_fields_format(self):
        inputs = validate_input({
            "input_schema_version":"1.0.0",
            "start_dtg":"2025-06-01T00:00:00Z",
            "end_dtg":"2025-06-02T00:00:00Z",
            "data_source_type":"file",
            "data_source":"/tmp/x"
        })
        doc = OutputDoc(input_data_hash="0"*64, inputs=inputs)
        doc.add_message("WARN","msg")
        msg = doc["messages"][0]
        self.assertRegex(msg["timestamp"], r"^\d{4}-\d{2}-\d{2}T")
        self.assertEqual(msg["level"], "WARN")

    # 24 multiple messages append
    def test_24_outputdoc_multiple_messages_append(self):
        inputs = validate_input({
            "input_schema_version":"1.0.0",
            "start_dtg":"2025-06-01T00:00:00Z",
            "end_dtg":"2025-06-02T00:00:00Z",
            "data_source_type":"file",
            "data_source":"/tmp/x"
        })
        doc = OutputDoc(input_data_hash="0"*64, inputs=inputs)
        doc.add_message("INFO","a"); doc.add_message("ERROR","b")
        self.assertEqual([m["text"] for m in doc["messages"]], ["a","b"])

    # 25 save before finalise
    def test_25_save_before_finalise_runtime_error(self):
        inputs = validate_input({
            "input_schema_version":"1.0.0",
            "start_dtg":"2025-06-01T00:00:00Z",
            "end_dtg":"2025-06-02T00:00:00Z",
            "data_source_type":"file",
            "data_source":"/tmp/x"
        })
        doc = OutputDoc(input_data_hash="0"*64, inputs=inputs)
        with self.assertRaises(RuntimeError):
            doc.save(Path(tempfile.NamedTemporaryFile().name))

    # 26 save with DataFrame in inputs
    def test_26_save_with_dataframe_succeeds(self):
        df = pd.DataFrame({"col":[1,2]})
        inputs = validate_input({
            "input_schema_version":"1.0.0",
            "start_dtg":"2025-06-01T00:00:00Z",
            "end_dtg":"2025-06-02T00:00:00Z",
            "data_source_type":"df",
            "data_source":df,
        })
        doc = OutputDoc(input_data_hash="a"*64, inputs=inputs)
        doc.finalise()
        tmp = _Util.tmp_json({})
        doc.save(tmp)
        saved = json.loads(tmp.read_text())
        self.assertIn("__dataframe_sha256__", saved["inputs"]["data_source"])
        tmp.unlink()

    # 27 hash determinism
    def test_27_hash_determinism_and_sensitivity(self):
        df = pd.DataFrame({"v":[1,2]})
        base = {"df":df, "p":1}
        h1 = OutputDoc._hash(base); h2 = OutputDoc._hash(base)
        self.assertEqual(h1, h2)
        df2 = df.copy(); df2.loc[0,"v"]=99
        self.assertNotEqual(h1, OutputDoc._hash({"df":df2,"p":1}))
        self.assertNotEqual(h1, OutputDoc._hash({"df":df,"p":2}))

    # Extra: ensure _DEFAULTS covers all optional INPUT_SCHEMA fields
    def test_defaults_cover_all_optionals(self):
        optionals = [k for k,meta in 
                     __import__("schema_io").validator.INPUT_SCHEMA["fields"].items()
                     if not meta.get("required",False)]
        self.assertTrue(set(optionals).issuperset(set(_DEFAULTS.keys())))

if __name__ == "__main__":
    unittest.main(verbosity=2)