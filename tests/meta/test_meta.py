import copy
import json
import tempfile
import unittest
from pathlib import Path

from contract_schema import Contract, loader, validator


class MetaSchemaTests(unittest.TestCase):
    def setUp(self):
        self.good_schema = loader.load_schema("analytic_schema.json")
        self.meta_schema = loader.load_schema("contract_meta_schema.json")

    # ------------------------------------------------------------------ #
    # Happy path                                                         #
    # ------------------------------------------------------------------ #
    def test_good_contract_loads(self):
        Contract.load("analytic_schema.json")  # should not raise

    # ------------------------------------------------------------------ #
    # Negative paths                                                     #
    # ------------------------------------------------------------------ #
    def test_missing_required_key_fails_meta_validation(self):
        bad = copy.deepcopy(self.good_schema)
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
        bad = copy.deepcopy(self.good_schema)
        bad["input"].pop("fields")  # violate meta-schema

        with self.assertRaises(validator.SchemaError):
            validator.validate(bad, schema=self.meta_schema)
