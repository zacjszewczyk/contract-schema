import unittest
import re
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
                "extra": {"type": ["string", "null"], "required": False}, # Allow null
            },
            "additionalProperties": False,
        }

    def test_valid_object_passes(self):
        obj = {"name": "foo", "count": 1}
        validator.validate(obj, schema=self.schema)  # should not raise

    def test_valid_object_with_null_passes(self):
        obj = {"name": "foo", "count": 1, "extra": None}
        validator.validate(obj, schema=self.schema)  # should not raise

    def test_missing_required_field_raises(self):
        with self.assertRaisesRegex(SchemaError, "root: missing required"):
            validator.validate({"count": 1}, schema=self.schema)

    def test_enum_violation_raises(self):
        with self.assertRaisesRegex(SchemaError, "not in"):
            validator.validate({"name": "foo", "count": 5}, schema=self.schema)

    def test_wrong_type_raises(self):
        with self.assertRaisesRegex(SchemaError, r"root\.name: expected \['string'\], got int"):
            validator.validate({"name": 123, "count": 1}, schema=self.schema)

    def test_list_subtype_enforcement(self):
        good = {"name": "foo", "count": 2, "tags": ["a", "b"]}
        validator.validate(good, schema=self.schema)

        bad = {"name": "foo", "count": 2, "tags": [1, 2]}
        with self.assertRaisesRegex(SchemaError, r"root\.tags\[0\]: expected \['string'\], got int"):
            validator.validate(bad, schema=self.schema)

    def test_datetime_format_handling(self):
        dt_schema = {"type": "string", "format": "date-time"}
        validator.validate("2025-01-01T12:00:00Z", schema=dt_schema)

        with self.assertRaises(SchemaError):
            validator.validate("not-a-date", schema=dt_schema)
            
    def test_implicit_object_type(self):
        schema = {
            "fields": { "key": {"type": "string", "required": True} }
        }
        # Should pass, as `type: "object"` is implied
        validator.validate({"key": "value"}, schema=schema)
        # Should fail for the same reason
        with self.assertRaisesRegex(SchemaError, "expected \['object'\]"):
            validator.validate("not-an-object", schema=schema)

    def test_deeply_nested_error_path(self):
        schema = {
            "fields": {
                "level1": {
                    "type": "object",
                    "fields": {
                        "level2": {
                            "type": "object",
                            "fields": { "bad_field": { "type": "string" } }
                        }
                    }
                }
            }
        }
        bad_doc = {
            "level1": { "level2": { "bad_field": 123 } } # incorrect type
        }
        with self.assertRaisesRegex(SchemaError, r"root\.level1\.level2\.bad_field: expected \['string'\]"):
            validator.validate(bad_doc, schema=schema)


class ValidatorAdditionalPropsTests(unittest.TestCase):
    def test_no_additional_properties_ok(self):
        schema = {
            "type": "object",
            "fields": {"val": {"type": ["string"], "required": True}},
            "additionalProperties": False,
        }
        validator.validate({"val": "x"}, schema=schema)  # passes

    def test_extra_property_raises_when_disallowed(self):
        schema = {
            "type": "object",
            "fields": {"val": {"type": ["string"], "required": True}},
            "additionalProperties": False,
        }
        with self.assertRaisesRegex(SchemaError, "unexpected fields"):
            validator.validate({"val": "x", "extra": 1}, schema=schema)

    def test_extra_property_ok_when_allowed(self):
        schema = {
            "type": "object",
            "fields": {"val": {"type": ["string"], "required": True}},
            "additionalProperties": True,
        }
        validator.validate({"val": "x", "extra": 1}, schema=schema) # passes