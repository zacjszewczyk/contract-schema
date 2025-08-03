import copy
import json
import tempfile
import unittest
from pathlib import Path

from contract_schema import Contract, loader, validator


class MetaSchemaTests(unittest.TestCase):
    def setUp(self):
        self.good_analytic_schema = loader.load_schema("analytic_schema.json")
        self.good_model_schema = loader.load_schema("model_schema.json")
        self.meta_schema = loader.load_schema("contract_meta_schema.json")

    # ------------------------------------------------------------------ #
    # Happy path                                                         #
    # ------------------------------------------------------------------ #
    def test_good_contracts_load(self):
        Contract.load("analytic_schema.json")  # should not raise
        Contract.load("model_schema.json")     # should not raise

    def test_schemas_are_valid_against_meta_schema(self):
        validator.validate(self.good_analytic_schema, schema=self.meta_schema)
        validator.validate(self.good_model_schema, schema=self.meta_schema)

    # ------------------------------------------------------------------ #
    # Negative paths                                                     #
    # ------------------------------------------------------------------ #
    def test_missing_required_key_fails_meta_validation(self):
        bad = copy.deepcopy(self.good_analytic_schema)
        bad.pop("title")

        with tempfile.NamedTemporaryFile("w+", delete=False) as tmp:
            json.dump(bad, tmp)
            tmp.flush()
            p = Path(tmp.name)

        try:
            with self.assertRaises(ValueError):
                Contract.load(p)
        finally:
            p.unlink(missing_ok=True)

    def test_input_missing_fields_fails_explicit_validation(self):
        bad = copy.deepcopy(self.good_analytic_schema)
        bad["input"].pop("fields")  # violate meta-schema

        with self.assertRaises(validator.SchemaError):
            validator.validate(bad, schema=self.meta_schema)

    def test_bad_type_for_top_level_key_fails(self):
        bad = copy.deepcopy(self.good_analytic_schema)
        bad["version"] = 1.2 # Should be a string
        with self.assertRaises(validator.SchemaError):
            validator.validate(bad, schema=self.meta_schema)