import unittest

from contract_schema import validator
from contract_schema.validator import SchemaError


class ValidatorTests(unittest.TestCase):
    def setUp(self):
        # Minimal object-level schema used by several tests
        self.schema = {
            "type": "object",
            "fields": {
                "name":  {"type": ["string"],  "required": True},
                "count": {"type": ["integer"], "required": True, "enum": [1, 2, 3]},
                "tags":  {"type": ["list"],    "subtype": "string", "required": False},
            },
            "additionalProperties": False,
        }

    def test_valid_object_passes(self):
        obj = {"name": "foo", "count": 1}
        validator.validate(obj, schema=self.schema)  # should not raise

    def test_missing_required_field_raises(self):
        with self.assertRaises(SchemaError):
            validator.validate({"count": 1}, schema=self.schema)

    def test_enum_violation_raises(self):
        with self.assertRaises(SchemaError):
            validator.validate({"name": "foo", "count": 5}, schema=self.schema)

    def test_list_subtype_enforcement(self):
        good = {"name": "foo", "count": 2, "tags": ["a", "b"]}
        validator.validate(good, schema=self.schema)

        bad = {"name": "foo", "count": 2, "tags": [1, 2]}
        with self.assertRaises(SchemaError):
            validator.validate(bad, schema=self.schema)

    def test_datetime_format_handling(self):
        dt_schema = {"type": "string", "format": "date-time"}
        validator.validate("2025-01-01T12:00:00Z", schema=dt_schema)

        with self.assertRaises(SchemaError):
            validator.validate("not-a-date", schema=dt_schema)

class ValidatorAdditionalPropsTests(unittest.TestCase):
    def setUp(self):
        self.schema = {
            "type": "object",
            "fields": {
                "val": {"type": ["string"], "required": True},
            },
            "additionalProperties": False,
        }

    def test_no_additional_properties_ok(self):
        validator.validate({"val": "x"}, schema=self.schema)  # passes

    def test_extra_property_raises(self):
        with self.assertRaises(SchemaError):
            validator.validate({"val": "x", "extra": 1}, schema=self.schema)