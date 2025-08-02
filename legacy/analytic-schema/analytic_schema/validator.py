import json
import copy
from pathlib import Path
import re
from typing import Any

import pandas as pd

from .loader import INPUT_SCHEMA

class SchemaError(ValueError):
    """Raised on any schema‐validation failure."""

# defaults for *optional* input fields (as per your updated contract)
_DEFAULTS: dict[str, Any] = {
    "log_path": "./{run_id}_{execution_dtg}.log",
    "output": "stdout",
    "analytic_parameters": {},
    "data_map": {},
    "verbosity": "INFO",
}

# ISO-8601 date-time regex (Z or ±HH:MM)
_DT_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"      # date & time
    r"(?:\.\d{1,6})?"                            # optional .microseconds
    r"(?:Z|[+\-]\d{2}:\d{2})$"                   # Z or ±HH:MM
)

def _is_datetime(s: Any) -> bool:
    if not isinstance(s, str) or not _DT_RE.match(s):
        return False
    # rely on fromisoformat for strictness
    try:
        # Python expects +00:00 not Z
        dt = s.replace("Z", "+00:00")
        _ = type(pd.Timestamp("now")).__qualname__  # dummy to keep pandas import alive
        # just test parsing
        from datetime import datetime
        datetime.fromisoformat(dt)
        return True
    except Exception:
        return False

# map schema types → Python classes
_TYPE_MAP: dict[str, any] = {
    "string": str,
    "integer": int,
    "number": (int, float),
    "boolean": bool,
    "object": dict,
    "list": list,
    "dataframe": pd.DataFrame,
}

def _apply_defaults(data: dict[str, Any]) -> None:
    """
    In-place inject any missing optional fields.
    """
    for k, v in _DEFAULTS.items():
        if k not in data:
            # copy for mutables
            data[k] = copy.deepcopy(v)

def validate_input(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Apply --config override, dereference JSON strings/files,
    inject defaults, then deep‐validate against INPUT_SCHEMA.
    """
    if not isinstance(raw, dict):
        raise TypeError(f"validate_input expects dict, got {type(raw).__name__}")

    data = raw.copy()
    cfg = data.pop("config", None)
    # 1) handle --config file
    if isinstance(cfg, str):
        p = Path(cfg)
        if not p.is_file():
            raise FileNotFoundError(f"--config '{cfg}' is not a file")
        content = p.read_text(encoding="utf-8")
        data = json.loads(content)
        if not isinstance(data, dict):
            raise ValueError("Config JSON must be an object")
    elif cfg is not None:
        raise TypeError("--config must be a file‐path string")

    # 2) dereference analytic_parameters / data_map if string
    for key in ("analytic_parameters", "data_map"):
        if key in data and isinstance(data[key], str):
            p = Path(data[key])
            # file?
            if p.is_file():
                loaded = json.loads(p.read_text(encoding="utf-8"))
                if not isinstance(loaded, dict):
                    raise ValueError(f"File '{p}' for '{key}' is not an object")
                data[key] = loaded
            else:
                # try JSON literal
                try:
                    lit = json.loads(data[key])
                    if isinstance(lit, dict):
                        data[key] = lit
                except json.JSONDecodeError:
                    pass  # leave as string

    # 3) inject defaults
    _apply_defaults(data)

    # 4) deep‐validate
    _validate(data, INPUT_SCHEMA, path="")

    return data

def _validate(value: Any, schema: dict[str, Any], *, path: str) -> None:
    """
    Recursively validate *value* against our contract representation:
    - type: list of allowed types
    - enum
    - format: date-time
    - object → fields / required per-field / additionalProperties
    - list → items or subtype
    """
    # 1) type‐union
    types = schema.get("type")
    # If the contract uses the compact "fields" form but forgets to
    # declare `"type": "object"`, infer it so that deep validation still
    # happens (needed for findings items, etc.).
    if types is None and "fields" in schema:
        types = ["object"]

    if types:
        allowed = types if isinstance(types, list) else [types]
        for t in allowed:
            py = _TYPE_MAP.get(t)
            if py and isinstance(value, py):
                break
        else:
            got = type(value).__name__
            raise SchemaError(f"{path or 'value'}: expected one of {allowed}, got {got}")

    # 2) enum
    if "enum" in schema and value not in schema["enum"]:
        raise SchemaError(f"{path}: '{value}' not in {schema['enum']}")

    # 3) format: date-time
    if schema.get("format") == "date-time" and not _is_datetime(value):
        raise SchemaError(f"{path}: '{value}' is not valid ISO-8601 date-time")

    # 4) object recursion
    is_object_schema = ("object" in (types if isinstance(types, list) else [types])
                        if types else False) or "fields" in schema

    if isinstance(value, dict) and is_object_schema:
        fields = schema.get("fields", {})
        req = {k for k,meta in fields.items() if meta.get("required")}
        extras = set(value) - set(fields)
        if not schema.get("additionalProperties", True) and extras:
            raise SchemaError(f"{path}: unexpected fields {sorted(extras)}")
        miss = req - set(value)
        if miss:
            raise SchemaError(f"{path}: missing required fields {sorted(miss)}")
        for k,v in value.items():
            if k in fields:
                child = f"{path}.{k}" if path else k
                _validate(v, fields[k], path=child)

    # 5) list recursion
    if isinstance(value, list) and "list" in (types if types else []):
        # items‐by‐fields (for objects arrays) or subtype
        if "items" in schema:
            item_schema = schema["items"]
        elif "subtype" in schema:
            item_schema = {"type": [schema["subtype"]]}
        else:
            item_schema = None
        if item_schema:
            for i, itm in enumerate(value):
                _validate(itm, item_schema, path=f"{path}[{i}]")