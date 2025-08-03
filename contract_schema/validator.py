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
    * ``format: date-time``
    * object validation via ``fields`` / ``required`` / ``additionalProperties``
    * list validation via ``items`` or custom ``subtype`` shorthand
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

    # 3) format                                                             #
    if schema.get("format") == "date-time" and not utils._is_datetime(value):
        raise SchemaError(f"{path}: '{value}' is not ISO-8601 date-time")

    # 4) object recursion ---------------------------------------------------
    if isinstance(value, dict):
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
            # recurse into children ---------------------------------------
            for k, v in value.items():
                child_path = f"{path}.{k}" if path else k
                validate(v, schema=fields.get(k, {}), path=child_path)

    # 5) list recursion -----------------------------------------------------
    if isinstance(value, list):
        if "items" in schema:
            item_schema = schema["items"]
        elif "subtype" in schema:
            item_schema = {"type": [schema["subtype"]]}
        else:
            item_schema = None
        if item_schema is not None:
            for idx, item in enumerate(value):
                validate(item, schema=item_schema, path=f"{path}[{idx}]")