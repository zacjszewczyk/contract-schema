"""
validator.py - unified, schema-agnostic validation utilities
===========================================================

This file merges the *input-schema* and *output-schema* validators into a
single engine that you can point at **any** contract that follows the same
compact JSON-Schema-lite conventions used across *analytic_schema* and
*model_schema*.

Public API
----------
SchemaError
    Exception raised for any contract violation.

validate(doc: Mapping, *, schema: Mapping[str, Any], path: str = "root")
    Depth-first validation that enforces type, enum, format, required fields,
    additionalProperties, list item validation, etc.

The old *specialised* helpers (`validate_input`, `validate_manifest`) can now
be written in one line each.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
import re
from typing import Any, Mapping

from . import utils

__all__ = [
    "SchemaError",
    "validate"
]

# --------------------------------------------------------------------------- #
# Exceptions                                                                  #
# --------------------------------------------------------------------------- #

class SchemaError(ValueError):
    """Raised when a document violates the supplied contract."""


# --------------------------------------------------------------------------- #
# Core recursive validator                                                    #
# --------------------------------------------------------------------------- #

def validate(value: Any, *, schema: Mapping[str, Any], path: str = "root") -> None:
    """Recursively assert that *value* satisfies *schema*.

    The function implements the minimal-viable subset of JSON-Schema required
    by our contracts:

    * ``type`` (list or scalar)
    * ``enum``
    * ``format: date-time`` / ``date`` / ``mitre-date-time``
    * ``pattern`` for string regex checks
    * object validation via ``fields`` / ``required`` / ``additionalProperties``
      / ``propertyNamesPattern`` / ``minProperties``
    * list validation via ``items`` or custom ``subtype`` shorthand
      / ``minItems``
    """

    stype = schema.get("type")
    if stype is None and "fields" in schema:
        stype = ["object"]  # implicit object when only `fields` is present

    # 1) type check ---------------------------------------------------------
    if stype:
        allowed = list(stype) if isinstance(stype, (list, tuple)) else [stype]
        if not any(isinstance(value, utils._TYPE_MAP.get(t, object)) for t in allowed):
            raise SchemaError(f"{path}: expected {allowed}, got {type(value).__name__}")

    # 2) enum --------------------------------------------------------------
    if "enum" in schema and value not in schema["enum"]:
        raise SchemaError(f"{path}: '{value}' not in {schema['enum']}")

    # 3) scalar constraints -------------------------------------------------
    fmt = schema.get("format")
    if fmt == "date-time" and not utils._is_datetime(value):
        raise SchemaError(f"{path}: '{value}' is not ISO-8601 date-time")
    if fmt == "date" and not utils._is_date(value):
        raise SchemaError(f"{path}: '{value}' is not ISO-8601 date")
    if fmt == "mitre-date-time" and not utils._is_flexible_datetime(value):
        raise SchemaError(f"{path}: '{value}' is not a supported MITRE date-time")

    if isinstance(value, str):
        pattern = schema.get("pattern")
        if pattern is not None and re.fullmatch(pattern, value) is None:
            raise SchemaError(f"{path}: '{value}' does not match pattern '{pattern}'")

        min_length = schema.get("minLength")
        if min_length is not None and len(value) < min_length:
            raise SchemaError(f"{path}: length {len(value)} is less than minLength {min_length}")

    # 4) object recursion ---------------------------------------------------
    if isinstance(value, dict):
        min_properties = schema.get("minProperties")
        if min_properties is not None and len(value) < min_properties:
            raise SchemaError(f"{path}: has {len(value)} properties, below minProperties {min_properties}")

        key_pattern = schema.get("propertyNamesPattern")
        if key_pattern is not None:
            for key in value:
                if re.fullmatch(key_pattern, key) is None:
                    raise SchemaError(
                        f"{path}: key '{key}' does not match propertyNamesPattern '{key_pattern}'"
                    )

        fields = schema.get("fields")
        if fields is not None:  # only validate known object schemas
            required = {k for k, meta in fields.items() if meta.get("required")}
            missing = required - set(value)
            if missing:
                raise SchemaError(f"{path}: missing required {sorted(missing)}")

            extras = set(value) - set(fields)
            addl = schema.get("additionalProperties", True)
            if addl is False and extras:
                raise SchemaError(f"{path}: unexpected fields {sorted(extras)}")
            if isinstance(addl, Mapping):
                for k in extras:
                    child_path = f"{path}.{k}" if path else k
                    validate(value[k], schema=addl, path=child_path)
            # recurse into children ---------------------------------------
            for k, v in value.items():
                child_path = f"{path}.{k}" if path else k
                validate(v, schema=fields.get(k, {}), path=child_path)
        else:
            addl = schema.get("additionalProperties", True)
            if addl is False and value:
                raise SchemaError(f"{path}: unexpected fields {sorted(value)}")
            if isinstance(addl, Mapping):
                for k, v in value.items():
                    child_path = f"{path}.{k}" if path else k
                    validate(v, schema=addl, path=child_path)

    # 5) list recursion -----------------------------------------------------
    if isinstance(value, list):
        min_items = schema.get("minItems")
        if min_items is not None and len(value) < min_items:
            raise SchemaError(f"{path}: has {len(value)} items, below minItems {min_items}")

        if "items" in schema:
            item_schema = schema["items"]
        elif "subtype" in schema:
            item_schema = {"type": [schema["subtype"]]}
        else:
            item_schema = None
        if item_schema is not None:
            for idx, item in enumerate(value):
                validate(item, schema=item_schema, path=f"{path}[{idx}]")
