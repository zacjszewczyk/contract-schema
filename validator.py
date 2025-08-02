"""
validator.py – unified, schema‑agnostic validation utilities
===========================================================

This file merges the *input‑schema* and *output‑schema* validators into a
single engine that you can point at **any** contract that follows the same
compact JSON‑Schema‑lite conventions used across *analytic_schema* and
*model_schema*.

Public API
----------
SchemaError
    Exception raised for any contract violation.

validate(doc: Mapping, *, schema: Mapping[str, Any], path: str = "root")
    Depth‑first validation that enforces type, enum, format, required fields,
    additionalProperties, list item validation, etc.

apply_defaults(doc: dict, defaults: Mapping[str, Any]) -> None
    Inject default values (deep‑copied) for keys missing from *doc*.

validate_with_defaults(raw, *, schema, defaults=None, deref_json_files=False)
    Convenience wrapper used by the analytic input pipeline – handles
    ``--config`` overrides, JSON‑file dereferencing, default injection and then
    calls :pyfunc:`validate`.

The old *specialised* helpers (`validate_input`, `validate_manifest`) can now
be written in one line each:

```python
validated = validate_with_defaults(obj, schema=INPUT_SCHEMA, defaults=_DEFAULTS)
validate(manifest, schema=OUTPUT_SCHEMA)
```
"""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any, Mapping, Sequence, Tuple, Union

import pandas as pd

__all__ = [
    "SchemaError",
    "validate",
    "apply_defaults",
    "validate_with_defaults",
]

# --------------------------------------------------------------------------- #
# Exceptions                                                                  #
# --------------------------------------------------------------------------- #

class SchemaError(ValueError):
    """Raised when a document violates the supplied contract."""


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #

_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+\-]\d{2}:\d{2})$")


def _is_datetime(value: Any) -> bool:
    """Return True iff *value* is a valid ISO‑8601 date‑time string."""
    if not isinstance(value, str) or not _DT_RE.fullmatch(value):
        return False
    from datetime import datetime

    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


_TYPE_MAP: dict[str, Union[type, Tuple[type, ...]]] = {
    "string": str,
    "integer": int,
    "number": (int, float),
    "boolean": bool,
    "object": dict,
    "list": list,
    "dataframe": pd.DataFrame,
}


# --------------------------------------------------------------------------- #
# Core recursive validator                                                    #
# --------------------------------------------------------------------------- #

def validate(value: Any, *, schema: Mapping[str, Any], path: str = "root") -> None:  # noqa: C901, PLR0912 – recursion
    """Recursively assert that *value* satisfies *schema*.

    The function implements the minimal‑viable subset of JSON‑Schema required
    by our contracts:

    * ``type`` (list or scalar)
    * ``enum``
    * ``format: date‑time``
    * object validation via ``fields`` / ``required`` / ``additionalProperties``
    * list validation via ``items`` or custom ``subtype`` shorthand
    """

    stype = schema.get("type")
    if stype is None and "fields" in schema:
        stype = ["object"]  # implicit object when only `fields` is present

    # 1) type check ---------------------------------------------------------
    if stype:
        allowed = list(stype) if isinstance(stype, (list, tuple)) else [stype]
        if not any(isinstance(value, _TYPE_MAP.get(t, object)) for t in allowed):
            raise SchemaError(f"{path}: expected {allowed}, got {type(value).__name__}")

    # 2) enum --------------------------------------------------------------
    if "enum" in schema and value not in schema["enum"]:
        raise SchemaError(f"{path}: '{value}' not in {schema['enum']}")

    # 3) format                                                              #
    if schema.get("format") == "date-time" and not _is_datetime(value):
        raise SchemaError(f"{path}: '{value}' is not ISO‑8601 date‑time")

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


# --------------------------------------------------------------------------- #
# Defaults helper                                                             #
# --------------------------------------------------------------------------- #

def apply_defaults(doc: dict[str, Any], defaults: Mapping[str, Any]) -> None:
    """In‑place fill *doc* with deep‑copies from *defaults* where absent."""
    for k, v in defaults.items():
        if k not in doc:
            doc[k] = copy.deepcopy(v)


# --------------------------------------------------------------------------- #
# Convenience wrapper (used by analytic input pipeline)                       #
# --------------------------------------------------------------------------- #

def validate_with_defaults(
    raw: Mapping[str, Any] | dict,
    *,
    schema: Mapping[str, Any],
    defaults: Mapping[str, Any] | None = None,
    deref_json_files: bool = False,
) -> dict[str, Any]:
    """Full helper with config‑file override, JSON deref and default injection."""

    if not isinstance(raw, dict):
        raise TypeError("validate_with_defaults expects a mapping")

    data: dict[str, Any] = dict(raw)  # shallow copy – we mutate below

    # --config override -----------------------------------------------------
    cfg = data.pop("config", None)
    if cfg is not None:
        p = Path(cfg)
        if not p.is_file():
            raise FileNotFoundError(f"--config '{cfg}' is not a file")
        data = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("Config JSON must be an object at top level")

    # Optional dereference for specific fields -----------------------------
    if deref_json_files:
        for key, val in list(data.items()):
            if isinstance(val, str):
                p = Path(val)
                if p.is_file():
                    try:
                        parsed = json.loads(p.read_text(encoding="utf-8"))
                        data[key] = parsed
                    except json.JSONDecodeError:
                        pass  # leave as path string
                else:
                    try:
                        data[key] = json.loads(val)
                    except json.JSONDecodeError:
                        pass  # leave as raw string

    # defaults -------------------------------------------------------------
    if defaults is not None:
        apply_defaults(data, defaults)

    # deep validation -------------------------------------------------------
    validate(data, schema=schema, path="root")
    return data
