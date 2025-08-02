"""A lightweight, dependency-free validation engine for model manifests.

This module provides the core logic for validating a model manifest dictionary
against the official schema loaded by the `loader` module. It implements a
minimal subset of the JSON Schema specification, focusing on the essential
rules required for ensuring manifest integrity: type checking, enumerations,
required fields, and structural validation for nested objects and lists.

The main public component is the `validate_manifest` function, which serves
as the entry point for all validation tasks.
"""

from __future__ import annotations
import re
from typing import Any, Mapping, Tuple, Dict, Union, List

import pandas as pd

# Internal package imports for accessing the loaded schema contract.
from .loader import OUTPUT_SCHEMA

class SchemaError(ValueError):
    """Raised on any manifest-validation failure."""

# A pre-compiled regular expression for efficiently validating strings against
# the ISO 8601 datetime format. This improves performance over recompiling the
# pattern on each function call.
_DT_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+\-]\d{2}:\d{2})$"
)

def _is_dtg(s: Any) -> bool:
    """Validate if a string is a compliant ISO 8601 datetime.

    Args:
        s (Any): The value to be checked.

    Returns:
        bool: True if `s` is a valid ISO 8601 string, False otherwise.
    """
    from datetime import datetime
    # First, perform a quick pattern match. If it fails, no further (and more
    # expensive) processing is needed.
    if not isinstance(s, str) or not _DT_RE.fullmatch(s):
        return False
    # If the pattern matches, attempt a full parse to catch invalid dates
    # like non-existent days or months. 'Z' is replaced for compatibility.
    try:
        datetime.fromisoformat(s.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False

# A mapping from JSON Schema type names to their corresponding Python types.
# This dictionary is used by the validator to perform `isinstance` checks.
_TYPE: Dict[str, Union[type, Tuple[type, ...]]] = {
    "string": str,
    "integer": int,
    "number": (int, float),  # A 'number' can be an integer or a float.
    "boolean": bool,
    "object": dict,
    "list": list,
    "dataframe": pd.DataFrame,
}

def _validate(val: Any, schema: Mapping[str, Any], *, path: str = "") -> None:
    """Recursively validate a given value against a schema definition.

    This function is the core of the validation engine. It traverses a data
    structure and a corresponding schema structure in parallel, checking for
    compliance at each level.

    Args:
        val (Any): The data value or structure to be validated.
        schema (Mapping[str, Any]): The schema rules to apply to `val`.
        path (str): The dot-notation path to the current value, used for
                    generating clear error messages.

    Raises:
        SchemaError: If `val` fails to conform to any rule in `schema`.
    """
    # Check that the value's type matches the type(s) specified in the schema.
    stype = schema.get("type")
    if stype:
        allowed = stype if isinstance(stype, list) else [stype]
        py_ok = any(isinstance(val, _TYPE[t]) for t in allowed if t in _TYPE)
        if not py_ok:
            raise SchemaError(f"{path or 'value'}: expected {allowed}, got {type(val).__name__}")

    # Check if the value is a member of the allowed set in an 'enum'.
    if "enum" in schema and val not in schema["enum"]:
        raise SchemaError(f"{path}: '{val}' not in {schema['enum']}")

    # Check if a string value conforms to a specified 'format'.
    if schema.get("format") == "date-time" and not _is_dtg(val):
        raise SchemaError(f"{path}: '{val}' is not ISO-8601")

    # Recursively validate dictionary objects.
    want_obj = ("object" in (stype or [])) or "fields" in schema
    if want_obj and isinstance(val, dict):
        fields = schema.get("fields", {})
        # Check for missing required fields.
        req    = {k for k, meta in fields.items() if meta.get("required")}
        miss = req - set(val)
        if miss:
            raise SchemaError(f"{path}: missing {sorted(miss)}")

        # Check for unexpected fields if `additionalProperties` is false.
        extra  = set(val) - set(fields)
        if not schema.get("additionalProperties", True) and extra:
            raise SchemaError(f"{path}: unexpected {sorted(extra)}")

        # Recursively validate each field defined in the schema.
        for k, v in val.items():
            if k in fields:
                _validate(v, fields[k], path=f"{path}.{k}" if path else k)

    # Recursively validate list items.
    if stype == "list" and isinstance(val, list):
        if "items" in schema:
            # If 'items' is defined, all items in the list must conform to it.
            for i, item in enumerate(val):
                _validate(item, schema["items"], path=f"{path}[{i}]")
        elif "subtype" in schema:
            # 'subtype' is a non-standard shorthand for a simple list where
            # all items must be of a single, basic type.
            sub = {"type": [schema["subtype"]]}
            for i, item in enumerate(val):
                _validate(item, sub, path=f"{path}[{i}]")

def validate_manifest(doc: Mapping[str, Any]) -> None:
    """Validate a document against the global `OUTPUT_SCHEMA`.

    This function is the public-facing entry point for the validation engine.
    It initiates the recursive validation process for a complete manifest
    document, starting from the root.

    Args:
        doc (Mapping[str, Any]): The model manifest dictionary to be validated.

    Raises:
        SchemaError: If the manifest `doc` violates any rule defined in the
                     `OUTPUT_SCHEMA`.
    """
    _validate(doc, OUTPUT_SCHEMA, path="manifest")