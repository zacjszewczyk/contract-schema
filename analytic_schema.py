"""
analytic_schema.py
~~~~~~~~~~~~~~~~~~

Validate analytics-notebook *input* and *output* documents against the contract
defined in `INPUT_SCHEMA` and `OUTPUT_SCHEMA`.

Typical execution flow
----------------------
1. parse_input(...)   -> dict   # Collect parameters from CLI, file, or dict
2. validate_input(...) -> dict  # Schema-check & canonicalise the input dict
3. (run analytic code)          # Your notebook / script logic
4. out = OutputDoc(...); out.finalise(); out.save(...)

Core public API
---------------
parse_input(...)    -> dict
validate_input(...) -> dict
OutputDoc(**kwargs) – subclass of dict

Example
-------
>>> from analytic_schema import parse_input, validate_input, OutputDoc
>>> raw_params = parse_input('--input_schema_version 1.0.0 '
...                          '--start_dtg 2025-06-01T00:00:00Z '
...                          '--end_dtg 2025-06-02T00:00:00Z '
...                          '--data_source_type file '
...                          '--data_source /tmp/conn.csv'.split())
>>> params = validate_input(raw_params)
>>> raw_data_sha256 = "e3b0c4...55"  # Example hash
>>> out = OutputDoc(
...        input_schema_version=params['input_schema_version'],
...        output_schema_version='1.1.0',
...        analytic_id='notebooks/beacon_detection.ipynb',
...        analytic_name='Beacon Detection',
...        analytic_version='2.3.1',
...        inputs=params,
...        input_data_hash=raw_data_sha256,
...        status='success',
...        exit_code=0,
...        findings=[],
...        records_processed=0
... )
>>> out.finalise()
>>> out.save('run-results.json')
"""

from __future__ import annotations

import argparse
import datetime as _dt
import enum
import functools
import hashlib
import json
import pathlib
import re
import sys
import uuid
import shlex
import getpass        # For _get_user
import socket         # For _get_host
from typing import Any, Dict, List, Mapping, Sequence, Tuple, Union

# ──────────────────────────────────────────────────────────────────────────────
# 1.  Schema definitions  (verbatim copy of the user-provided contract)
# ──────────────────────────────────────────────────────────────────────────────

INPUT_SCHEMA: Dict[str, Any] = {
  "title": "Analytics Notebook Input Schema",
  "type": "object",
  "description": "Parameter block passed to a notebook when executed non-interactively.",
  "properties": {
    "input_schema_version": {
      "type": "string",
      "description": "Semantic-version identifier for this input contract so runners can verify compatibility."
    },
    "start_dtg": {
      "type": "string",
      "format": "date-time",
      "description": "Inclusive UTC timestamp (ISO 8601) marking the first data element to analyse."
    },
    "end_dtg": {
      "type": "string",
      "format": "date-time",
      "description": "Exclusive UTC timestamp (ISO 8601) marking the end of the data window."
    },
    "data_source_type": {
      "type": "string",
      "enum": ["file", "IONIC dataset", "api endpoint"],
      "description": "Transport mechanism used to retrieve data for the run."
    },
    "data_source": {
      "type": "string",
      "description": "Path, identifier or URL that the runner will use to fetch the dataset (e.g., '/data/conn.csv', 'ion:zeek_daily', 'https://api.example.com/logs')."
    },
    "log_path": {
      "type": "string",
      "description": "Filesystem path or stream for execution logs. Default './{run_id}_{execution_dtg}.log'. Supports 'stdout' and 'stderr'."
    },
    "output": {
      "type": "string",
      "description": "Destination for the notebook’s findings. Accepts 'stdout', 'stderr', or a file path."
    },
    "analytic_parameters": {
      "description": "Arbitrary JSON object or path to JSON file containing analytic-specific tuning knobs passed verbatim to the notebook.",
      "oneOf": [
        { "type": "object" },
        { "type": "string" }
      ]
    },
    "data_map": {
      "description": "JSON object or file path that maps non-SchemaONE fields in the input data to their SchemaONE equivalents expected by the notebook.",
      "oneOf": [
        { "type": "object" },
        { "type": "string" }
      ]
    },
    "verbosity": {
      "type": "string",
      "enum": ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"],
      "description": "Lowest log-severity level that will be emitted (default INFO)."
    }
  },
  "required": [
    "input_schema_version",
    "start_dtg",
    "end_dtg",
    "data_source_type",
    "data_source"
  ],
  "optional": [  # Non-standard JSON-Schema extension
    "log_path",
    "output",
    "analytic_parameters",
    "data_map",
    "verbosity"
  ],
  "additionalProperties": False
}

OUTPUT_SCHEMA: Dict[str, Any] = {
  "title": "Analytics Notebook Output Schema",
  "type": "object",
  "description": "Structured results emitted by the notebook after non-interactive execution.",
  "properties": {
    "input_schema_version": { "type": "string" },
    "output_schema_version": { "type": "string" },
    "run_id": { "type": "string" },
    "analytic_id": { "type": "string" },
    "analytic_name": { "type": "string" },
    "analytic_version": { "type": "string" },
    "run_user": { "type": "string" },
    "run_host": { "type": "string" },
    "inputs": { "type": "object" },  # Overridden below with full INPUT_SCHEMA
    "input_hash": { "type": "string" },
    "input_data_hash": { "type": "string" },
    "status": { "type": "string", "enum": ["success", "fail", "warning"] },
    "exit_code": { "type": "integer" },
    "messages": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "timestamp": { "type": "string", "format": "date-time" },
          "level":     { "type": "string", "enum": ["DEBUG","INFO","WARN","ERROR","FATAL"] },
          "text":      { "type": "string" }
        },
        "required": ["timestamp", "level", "text"],
        "additionalProperties": False
      }
    },
    "records_processed": { "type": "integer" },
    "run_start_dtg": { "type": "string", "format": "date-time" },
    "run_end_dtg":   { "type": "string", "format": "date-time" },
    "run_duration_seconds": { "type": "number" },
    "findings_hash": { "type": "string" },
    "findings": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "finding_id": { "type": "string" },
          "title": { "type": "string" },
          "description": { "type": "string" },
          "event_dtg": { "type": "string", "format": "date-time" },
          "severity": { "type": "string" },
          "confidence": { "type": "string" },
          "observables": { "type": "array", "items": { "type": "string" } },
          "mitre_attack_tactics": { "type": "array", "items": { "type": "string" } },
          "mitre_attack_techniques": { "type": "array", "items": { "type": "string" } },
          "recommended_actions": { "type": "string" },
          "recommended_pivots": { "type": "string" },
          "classification": { "type": "string" }
        },
        "required": [
          "finding_id","title","description","event_dtg","severity","confidence",
          "observables","mitre_attack_tactics","mitre_attack_techniques",
          "recommended_actions","recommended_pivots","classification"
        ],
        "additionalProperties": False
      }
    }
  },
  "required": [
    "input_schema_version","output_schema_version","run_id","analytic_id",
    "analytic_name","analytic_version","run_user","run_host","inputs","input_hash",
    "input_data_hash","status","exit_code","records_processed","run_start_dtg",
    "run_end_dtg","run_duration_seconds","findings_hash","findings"
  ],
  "optional": ["messages"],  # Non-standard extension
  "additionalProperties": False
}
# Provide full INPUT_SCHEMA for the 'inputs' field reference
OUTPUT_SCHEMA["properties"]["inputs"] = INPUT_SCHEMA

# ──────────────────────────────────────────────────────────────────────────────
# 2.  Validation engine (schema-lite, enough for this contract)
# ──────────────────────────────────────────────────────────────────────────────

class SchemaError(ValueError):
    """Raised when an object fails validation."""

# Helpers ─────────────────────────────────────────────────────────────────────
_DT_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})$")

def _is_datetime(s: Any) -> bool:
    """Return True if *s* is valid ISO-8601 date-time."""
    if not isinstance(s, str) or not _DT_RE.match(s):
        return False
    try:
        _dt.datetime.fromisoformat(s)
        return True
    except ValueError:
        return False

_TYPE_DISPATCH: Dict[str, Union[type, Tuple[type, ...]]] = {
    "string":  str,
    "integer": int,
    "number":  (int, float),
    "object":  dict,
    "array":   list,
    "boolean": bool,
}

def _validate(data: Any, schema: Mapping[str, Any], path: str = "") -> None:
    """Recursive JSON-Schema-lite validator."""
    stype = schema.get("type")
    if stype:
        exp = _TYPE_DISPATCH.get(stype)
        if exp and not isinstance(data, exp):
            raise SchemaError(f"{path or 'value'}: expected {stype}, got {type(data).__name__}")

    if "enum" in schema and data not in schema["enum"]:
        raise SchemaError(f"{path}: '{data}' not in enum {schema['enum']}")

    if schema.get("format") == "date-time" and not _is_datetime(data):
        raise SchemaError(f"{path}: '{data}' is not a valid ISO-8601 date-time")

    if "oneOf" in schema:
        errors = []
        for i, sub in enumerate(schema["oneOf"]):
            try:
                _validate(data, sub, path)
                break
            except SchemaError as e:
                errors.append(f"  oneOf option {i}: {e}")
        else:
            raise SchemaError(f"{path}: does not match any allowed schema in oneOf\n" + "\n".join(errors))

    if stype == "object":
        props = schema.get("properties", {})
        req   = set(schema.get("required", []))
        if not schema.get("additionalProperties", True):
            unknown = set(data.keys()) - set(props.keys())
            if unknown:
                raise SchemaError(f"{path}: unexpected fields {sorted(list(unknown))}")

        missing = req - set(data.keys())
        if missing:
            raise SchemaError(f"{path}: missing required fields {sorted(list(missing))}")

        for k, v in data.items():
            if k in props:
                _validate(v, props[k], f"{path}.{k}" if path else k)

    elif stype == "array":
        items_schema = schema.get("items")
        if items_schema:
            for idx, item in enumerate(data):
                _validate(item, items_schema, f"{path}[{idx}]")

# ──────────────────────────────────────────────────────────────────────────────
# 3.  Input helpers
# ──────────────────────────────────────────────────────────────────────────────

def _build_arg_parser() -> argparse.ArgumentParser:
    """Construct an ArgumentParser from INPUT_SCHEMA."""
    p = argparse.ArgumentParser(description=INPUT_SCHEMA.get("description", "Analytics Notebook Inputs"))
    p.add_argument("--config", help="Path to JSON file containing the full input object. Overrides other flags.")
    for prop, spec in INPUT_SCHEMA["properties"].items():
        arg = f"--{prop.replace('_','-')}"
        kwargs: Dict[str, Any] = {"help": spec.get("description", "")}
        stype = spec.get("type")
        if stype == "integer":
            kwargs["type"] = int
        elif stype == "number":
            kwargs["type"] = float
        else:
            kwargs["type"] = str
        if "enum" in spec:
            kwargs["choices"] = spec["enum"]
        kwargs["required"] = False
        p.add_argument(arg, **kwargs)
    return p

def parse_input(source: Union[None, pathlib.Path, str, Sequence[str], Mapping[str, Any]] = None) -> Dict[str, Any]:
    """
    Collect parameters from various sources and return a *raw* dict suitable for
    `validate_input()`.

    Parameters
    ----------
    source
        • dict           – pre-constructed parameters (shallow-copied)
        • Path / str     – JSON file path OR JSON string OR treated as CLI string
        • Sequence[str]  – list of CLI tokens (e.g. sys.argv[1:])
        • None           – defaults to sys.argv[1:]

    This function performs only minimal checks (e.g., required keys present)
    and deliberately **does not** run full schema validation.
    """
    # 1. Pre-built dict
    if isinstance(source, Mapping):
        data = dict(source)
    else:
        # Normalise *source* into a list of CLI tokens
        if source is None:
            argv: List[str] = sys.argv[1:]
        elif isinstance(source, Sequence) and not isinstance(source, (str, bytes, pathlib.Path)):
            argv = list(source)
        elif isinstance(source, (str, pathlib.Path)):
            p = pathlib.Path(source)
            if p.is_file():
                return json.loads(p.read_text(encoding="utf-8"))
            # Try JSON string
            try:
                return json.loads(str(source))
            except json.JSONDecodeError:
                # Fallback: treat as a shell-like string
                argv = shlex.split(str(source))
        else:
            raise TypeError("Unsupported input to parse_input()")

        # Heuristic: single non-flag argument that looks like a file → treat as file
        if len(argv) == 1 and not argv[0].startswith('-') and pathlib.Path(argv[0]).is_file():
            return json.loads(pathlib.Path(argv[0]).read_text(encoding="utf-8"))

        # Build from CLI flags
        parser = _build_arg_parser()
        ns = parser.parse_args(argv)

        if ns.config:
            cfg_path = pathlib.Path(ns.config)
            if not cfg_path.is_file():
                parser.error(f"--config path '{ns.config}' does not exist or is not a file.")
            data = json.loads(cfg_path.read_text(encoding="utf-8"))
        else:
            data = {prop: getattr(ns, prop, None)
                    for prop in INPUT_SCHEMA["properties"]
                    if getattr(ns, prop, None) is not None}

    # Quick check for required keys (detailed checks happen in validate_input)
    missing = [k for k in INPUT_SCHEMA["required"] if k not in data]
    if missing:
        raise ValueError(f"Missing required parameters: {missing}")

    return data

def _parse_cli(argv: List[str]) -> Dict[str, Any]:
    """
    Legacy helper retained for `validate_input()` – builds an input dict directly
    from CLI tokens and *immediately* validates if --config is used.
    """
    p = _build_arg_parser()
    ns = p.parse_args(argv)

    if ns.config:
        try:
            return validate_input(pathlib.Path(ns.config))
        except Exception as e:
            p.error(f"Error loading --config file '{ns.config}': {e}")

    out: Dict[str, Any] = {prop: getattr(ns, prop, None)
                           for prop in INPUT_SCHEMA["properties"]
                           if getattr(ns, prop, None) is not None}
    return out

# ──────────────────────────────────────────────────────────────────────────────
# 4.  Public validator
# ──────────────────────────────────────────────────────────────────────────────

def validate_input(source: Union[pathlib.Path, str, Sequence[str], dict]) -> Dict[str, Any]:
    """
    Validate and canonicalise an *input* dict.

    This performs full schema checks and resolves any object-or-path-or-JSON-string
    fields (currently ``analytic_parameters`` and ``data_map``).
    """
    if isinstance(source, dict):
        data = source.copy()

    elif isinstance(source, (str, pathlib.Path)):
        p_source = pathlib.Path(source)
        if p_source.is_file():
            data = json.loads(p_source.read_text(encoding="utf-8"))
        else:
            try:
                data = json.loads(str(source))
            except json.JSONDecodeError:
                raise ValueError("String source must be a valid JSON file path or JSON string.")

    elif isinstance(source, list):
        data = _parse_cli(source)
    else:
        raise TypeError("Unsupported source type for validate_input")

    # Structural validation
    _validate(data, INPUT_SCHEMA)

    # Canonicalise object-or-path fields
    for key in ("analytic_parameters", "data_map"):
        if key in data and isinstance(data[key], str):
            p = pathlib.Path(data[key])
            if p.is_file():
                data[key] = json.loads(p.read_text(encoding="utf-8"))
            else:
                try:
                    data[key] = json.loads(data[key])
                except json.JSONDecodeError:
                    pass  # Leave as plain string

    return data

# ──────────────────────────────────────────────────────────────────────────────
# 5.  Output document helper
# ──────────────────────────────────────────────────────────────────────────────

class _Level(enum.Enum):
    DEBUG = "DEBUG"; INFO = "INFO"; WARN = "WARN"; ERROR = "ERROR"; FATAL = "FATAL"

class OutputDoc(dict):
    """Helper for producing contract-compliant output documents."""

    def __init__(self, *, input_data_hash: str, **kwargs: Any):
        super().__init__(**kwargs)
        self["input_data_hash"] = input_data_hash
        self.__start_time = _dt.datetime.now(_dt.timezone.utc)

    @staticmethod
    def _hash(obj: Any) -> str:
        return hashlib.sha256(
            json.dumps(obj, sort_keys=True, separators=(',', ':')).encode()).hexdigest()

    def add_message(self, level: Union[str, _Level], text: str) -> None:
        if "messages" not in self:
            self["messages"] = []

        if isinstance(level, str):
            try:
                level_val = _Level[level.upper()].value
            except KeyError:
                raise ValueError(f"Invalid log level: {level}")
        elif isinstance(level, _Level):
            level_val = level.value
        else:
            raise TypeError("Log level must be str or _Level Enum")

        self["messages"].append({
            "timestamp": _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds"),
            "level": level_val,
            "text": text
        })

    def finalise(self) -> None:
        run_end = _dt.datetime.now(_dt.timezone.utc)

        self.setdefault("run_id", str(uuid.uuid4()))
        self.setdefault("run_user", _get_user())
        self.setdefault("run_host", _get_host())
        self.setdefault("run_start_dtg", self.__start_time.isoformat(timespec="seconds"))
        self.setdefault("run_end_dtg", run_end.isoformat(timespec="seconds"))
        self.setdefault("run_duration_seconds", (run_end - self.__start_time).total_seconds())

        if "inputs" not in self:
            raise SchemaError("Missing 'inputs' field before finalise()")
        self["input_hash"] = self._hash(self["inputs"])

        if "findings" not in self:
            raise SchemaError("Missing 'findings' field before finalise()")
        self["findings_hash"] = self._hash(self["findings"])

        _validate(self, OUTPUT_SCHEMA, path="OutputDoc")

    def save(self, path: Union[str, pathlib.Path], *, indent: int = 2) -> None:
        pathlib.Path(path).write_text(json.dumps(self, indent=indent), encoding="utf-8")

def _get_user() -> str:
    return getpass.getuser()

def _get_host() -> str:
    return socket.gethostname()

# ──────────────────────────────────────────────────────────────────────────────
# 6.  CLI entry-point  (python -m analytic_schema …)
# ──────────────────────────────────────────────────────────────────────────────
def _main(argv: List[str]) -> None:
    """Main CLI handler for validating input documents."""
    if not argv:
        _build_arg_parser().print_help(sys.stdout)
        sys.exit(0)

    try:
        raw_input = parse_input(argv)
        data = validate_input(raw_input)

        print("✓ Input document is valid and conforms to the schema.")
        print("\nCanonicalised Input:")
        print(json.dumps(data, indent=2, sort_keys=True))

    except (SchemaError, FileNotFoundError, json.JSONDecodeError, ValueError, TypeError) as exc:
        print(f"✗ Validation Error: {exc}", file=sys.stderr)
        sys.exit(1)
    except SystemExit as e:
        sys.exit(e.code if e.code is not None else 0)

if __name__ == "__main__":
    _main(sys.argv[1:])