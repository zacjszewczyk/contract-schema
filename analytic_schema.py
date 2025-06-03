"""
analytic_schema.py
~~~~~~~~~~~~~~~~~~

Validate analytics-notebook *input* and *output* documents against the contract
defined in `INPUT_SCHEMA` and `OUTPUT_SCHEMA`.

Typical execution flow
----------------------
1. raw_params = parse_input(...) # Collect parameters from CLI, file, or dict into a raw dict
2. params = validate_input(raw_params) # Schema-check, handle --config, & canonicalise the input dict
3. (run analytic code)             # Your notebook / script logic
4. out = OutputDoc(...); out.finalise(); out.save(...)

Core public API
---------------
parse_input(...)    -> dict
validate_input(...) -> dict
OutputDoc(**kwargs) – subclass of dict

Example
-------
>>> from analytic_schema import parse_input, validate_input, OutputDoc
>>> # Example 1: From CLI-like string (Corrected argument names with hyphens)
>>> raw_cli_params = parse_input('--input-schema-version 1.0.0 '
...                              '--start-dtg 2025-06-01T00:00:00Z '
...                              '--end-dtg 2025-06-02T00:00:00Z '
...                              '--data-source-type file '
...                              '--data-source /tmp/conn.csv')
>>> params_from_cli = validate_input(raw_cli_params)
>>>
>>> # Example 2: From a dictionary
>>> raw_dict_params = {
...     "input_schema_version": "1.0.0",
...     "start_dtg": "2025-06-01T00:00:00Z",
...     "end_dtg": "2025-06-02T00:00:00Z",
...     "data_source_type": "file",
...     "data_source": "/tmp/conn.csv",
...     "analytic_parameters": '{"param_a": 123}' # As JSON string
... }
>>> params_from_dict = validate_input(raw_dict_params)
>>> # params_from_dict['analytic_parameters'] would be {'param_a': 123}
>>>
>>> # Example 3: Using --config (imagine 'my_config.json' exists)
>>> # Contents of 'my_config.json':
>>> # {
>>> #   "input_schema_version": "1.0.0",
>>> #   "start_dtg": "2025-07-01T00:00:00Z",
>>> #   "end_dtg": "2025-07-02T00:00:00Z",
>>> #   "data_source_type": "api endpoint",
>>> #   "data_source": "https://api.example.com/data"
>>> # }
>>> # import pathlib
>>> # _ = pathlib.Path('my_config.json').write_text(json.dumps({
... #   "input_schema_version": "1.0.0",
... #   "start_dtg": "2025-07-01T00:00:00Z",
... #   "end_dtg": "2025-07-02T00:00:00Z",
... #   "data_source_type": "api endpoint",
... #   "data_source": "https://api.example.com/data"
... # }))
>>> # raw_params_with_config = parse_input(['--config', 'my_config.json', '--start-dtg', '2000-01-01T00:00:00Z'])
>>> # params_from_config_file = validate_input(raw_params_with_config)
>>> # params_from_config_file['start_dtg'] # Should be '2025-07-01T00:00:00Z' from config
>>> # pathlib.Path('my_config.json').unlink() # Clean up
>>>
>>> raw_data_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" # Example hash
>>> out = OutputDoc(
...     input_schema_version=params_from_dict['input_schema_version'],
...     output_schema_version='1.1.0',
...     analytic_id='notebooks/beacon_detection.ipynb',
...     analytic_name='Beacon Detection',
...     analytic_version='2.3.1',
...     inputs=params_from_dict, # Pass the validated and canonicalized inputs
...     input_data_hash=raw_data_sha256,
...     status='success',
...     exit_code=0,
...     findings=[], # Ensure findings is a list, even if empty
...     records_processed=0
... )
>>> out.add_message("INFO", "Processing started.")
>>> out.finalise() # This should now pass
>>> # print(json.dumps(out, indent=2))
>>> # out.save('run-results.json') # Example save
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
import getpass      # For _get_user
import socket       # For _get_host
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
        # datetime.fromisoformat handles 'Z' correctly if it's the *only* timezone indicator.
        # It also handles '+HH:MM' or '-HH:MM'.
        # The regex _DT_RE already ensures it's one of these forms.
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
        matched_oneof = False
        for i, sub_schema in enumerate(schema["oneOf"]):
            try:
                _validate(data, sub_schema, path) # Pass current path
                matched_oneof = True
                break
            except SchemaError as e:
                errors.append(f"  oneOf option {i} ({sub_schema.get('type', 'unknown type')}): {e}")
        if not matched_oneof:
            raise SchemaError(f"{path}: does not match any allowed schema in oneOf\n" + "\n".join(errors))


    if stype == "object":
        props = schema.get("properties", {})
        req   = set(schema.get("required", []))
        if not schema.get("additionalProperties", True) and isinstance(data, dict):
            unknown = set(data.keys()) - set(props.keys())
            if unknown:
                raise SchemaError(f"{path}: unexpected fields {sorted(list(unknown))}")

        if isinstance(data, dict):
            missing = req - set(data.keys())
            if missing:
                raise SchemaError(f"{path}: missing required fields {sorted(list(missing))}")

            for k, v in data.items():
                if k in props:
                    _validate(v, props[k], f"{path}.{k}" if path else k)
        elif req: # if data is not a dict, but fields are required, it's an error
             raise SchemaError(f"{path}: expected an object with required fields {sorted(list(req))}, got {type(data).__name__}")


    elif stype == "array":
        items_schema = schema.get("items")
        if items_schema and isinstance(data, list):
            for idx, item in enumerate(data):
                _validate(item, items_schema, f"{path}[{idx}]")
        elif items_schema and not isinstance(data, list): # Data is not a list but items schema exists
            raise SchemaError(f"{path}: expected an array, got {type(data).__name__}")


# ──────────────────────────────────────────────────────────────────────────────
# 3.  Input parsing and validation
# ──────────────────────────────────────────────────────────────────────────────

@functools.lru_cache(maxsize=1) # Cache the parser construction
def _build_arg_parser() -> argparse.ArgumentParser:
    """Construct an ArgumentParser from INPUT_SCHEMA."""
    p = argparse.ArgumentParser(description=INPUT_SCHEMA.get("description", "Analytics Notebook Inputs"), add_help=False)
    # Add help manually to control its position or to customize.
    p.add_argument(
        '-h', '--help', action='help', default=argparse.SUPPRESS,
        help='Show this help message and exit.'
    )
    p.add_argument("--config", help="Path to JSON file containing the full input object. If provided, other CLI flags for input properties are typically overridden by the file's content during validation.")
    for prop, spec in INPUT_SCHEMA["properties"].items():
        arg = f"--{prop.replace('_','-')}" # Converts 'input_schema_version' to '--input-schema-version'
        kwargs: Dict[str, Any] = {"help": spec.get("description", "")}
        
        stype = spec.get("type")
        if stype == "boolean": 
            kwargs["action"] = argparse.BooleanOptionalAction 
        elif stype == "integer":
            kwargs["type"] = int
        elif stype == "number":
            kwargs["type"] = float
        
        if "enum" in spec:
            kwargs["choices"] = spec["enum"]
        
        kwargs["required"] = False 
        p.add_argument(arg, dest=prop, **kwargs) 
    return p

def parse_input(source: Union[None, pathlib.Path, str, Sequence[str], Mapping[str, Any]] = None) -> Dict[str, Any]:
    """
    Parse parameters from various sources into a *raw* dictionary.
    This function does NOT perform schema validation or --config file loading.
    """
    if isinstance(source, Mapping):
        return dict(source)

    if isinstance(source, pathlib.Path):
        try:
            return json.loads(source.read_text(encoding="utf-8"))
        except FileNotFoundError:
            raise FileNotFoundError(f"Input file not found: {source}")
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(f"Invalid JSON in file {source}: {e.msg}", e.doc, e.pos)

    argv: List[str] 
    if isinstance(source, str):
        p_source = pathlib.Path(source)
        if p_source.is_file():
            try:
                return json.loads(p_source.read_text(encoding="utf-8"))
            except json.JSONDecodeError as e:
                raise json.JSONDecodeError(f"Invalid JSON in file {source}: {e.msg}", e.doc, e.pos)
            except Exception as e: 
                raise ValueError(f"Error reading file {source} as JSON: {e}")
        try:
            return json.loads(source)
        except json.JSONDecodeError:
            argv = shlex.split(source)
    elif source is None:
        argv = sys.argv[1:]
    elif isinstance(source, Sequence) and not isinstance(source, (str, bytes)): 
        argv = list(source) 
    else:
        raise TypeError(f"Unsupported input type for parse_input: {type(source).__name__}")

    parser = _build_arg_parser()
    try:
        ns, unknown_args = parser.parse_known_args(argv)
        if unknown_args:
            print(f"Warning: Unknown arguments found by parse_input and ignored: {unknown_args}", file=sys.stderr)
    except SystemExit: 
        raise 
    parsed_args = {k: v for k, v in vars(ns).items() if v is not None}
    return parsed_args


def validate_input(raw_params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate and canonicalise an input dictionary against INPUT_SCHEMA.
    Handles --config file loading and resolves object-or-path fields.
    """
    if not isinstance(raw_params, dict):
        raise TypeError(f"validate_input expects a dictionary, got {type(raw_params).__name__}")

    data_to_validate = raw_params.copy() 
    config_path_str_for_error_msg = data_to_validate.get("config") # Keep for error message if needed
    config_path_str = data_to_validate.pop("config", None) 
    
    if isinstance(config_path_str, str):
        config_path = pathlib.Path(config_path_str)
        if not config_path.is_file():
            raise FileNotFoundError(f"--config path '{config_path_str}' does not exist or is not a file.")
        try:
            print(f"Info: Loading parameters from --config file: {config_path_str}", file=sys.stderr)
            data_to_validate = json.loads(config_path.read_text(encoding="utf-8"))
            if not isinstance(data_to_validate, dict):
                 raise ValueError(f"Content of --config file '{config_path_str}' is not a JSON object.")
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(f"Invalid JSON in --config file {config_path_str}: {e.msg}", e.doc, e.pos)
        except Exception as e:
            raise ValueError(f"Error reading or parsing --config file {config_path_str}: {e}")
    elif config_path_str is not None: 
        raise TypeError(f"Unexpected type for 'config' parameter: {type(config_path_str)}. Expected string path.")

    for key in ("analytic_parameters", "data_map"):
        if key in data_to_validate and isinstance(data_to_validate[key], str):
            val_str = data_to_validate[key]
            path_val = pathlib.Path(val_str)
            loaded_from_file = False
            if val_str and path_val.is_file(): 
                try:
                    loaded_json = json.loads(path_val.read_text(encoding="utf-8"))
                    if not isinstance(loaded_json, dict): 
                         raise ValueError(f"Content of file '{val_str}' for '{key}' is not a JSON object.")
                    data_to_validate[key] = loaded_json
                    loaded_from_file = True
                except json.JSONDecodeError:
                    raise json.JSONDecodeError(f"Invalid JSON in file '{val_str}' for parameter '{key}'.",val_str,0 )
                except Exception as e:
                     raise ValueError(f"Error reading file '{val_str}' for parameter '{key}': {e}")
            if not loaded_from_file: 
                try:
                    parsed_json_string = json.loads(val_str)
                    if isinstance(parsed_json_string, dict): 
                        data_to_validate[key] = parsed_json_string
                except json.JSONDecodeError:
                    pass
    try:
        _validate(data_to_validate, INPUT_SCHEMA)
    except SchemaError as e:
        config_file_info = f" (when using config file: {config_path_str_for_error_msg})" if isinstance(config_path_str_for_error_msg, str) else ""
        raise SchemaError(f"Input validation failed{config_file_info}: {e}")
    return data_to_validate

# ──────────────────────────────────────────────────────────────────────────────
# 4.  Output document helper
# ──────────────────────────────────────────────────────────────────────────────

class _Level(enum.Enum):
    DEBUG = "DEBUG"; INFO = "INFO"; WARN = "WARN"; ERROR = "ERROR"; FATAL = "FATAL"

class OutputDoc(dict):
    """Helper for producing contract-compliant output documents."""

    def __init__(self, *, input_data_hash: str, **kwargs: Any):
        super().__init__(**kwargs)
        if not isinstance(input_data_hash, str): 
            raise TypeError("input_data_hash must be a string.")
        self["input_data_hash"] = input_data_hash
        self.__start_time = _dt.datetime.now(_dt.timezone.utc)
        self["messages"] = [] 

    @staticmethod
    def _hash(obj: Any) -> str:
        return hashlib.sha256(
            json.dumps(obj, sort_keys=True, separators=(',', ':')).encode()).hexdigest()

    def add_message(self, level: Union[str, _Level], text: str) -> None:
        if isinstance(level, str):
            try:
                level_val = _Level[level.upper()].value
            except KeyError:
                raise ValueError(f"Invalid log level: {level}. Must be one of {_Level._member_names_}")
        elif isinstance(level, _Level):
            level_val = level.value
        else:
            raise TypeError(f"Log level must be str or _Level Enum, got {type(level).__name__}")

        if not isinstance(text, str):
            raise TypeError(f"Log message text must be a string, got {type(text).__name__}")

        self["messages"].append({
            "timestamp": _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds"), # CORRECTED: Removed + "Z"
            "level": level_val,
            "text": text
        })

    def finalise(self) -> None:
        run_end = _dt.datetime.now(_dt.timezone.utc)

        self.setdefault("run_id", str(uuid.uuid4()))
        self.setdefault("run_user", _get_user())
        self.setdefault("run_host", _get_host())
        # CORRECTED: Removed + "Z" as isoformat() on timezone-aware datetime already includes offset
        self.setdefault("run_start_dtg", self.__start_time.isoformat(timespec="seconds"))
        self.setdefault("run_end_dtg", run_end.isoformat(timespec="seconds"))
        self.setdefault("run_duration_seconds", round((run_end - self.__start_time).total_seconds(), 6))

        if "inputs" not in self:
            raise SchemaError("OutputDoc: Missing 'inputs' field before finalise(). Provide the validated input parameters.")
        if not isinstance(self["inputs"], dict):
             raise SchemaError(f"OutputDoc: 'inputs' field must be a dictionary, got {type(self['inputs']).__name__}.")
        self["input_hash"] = self._hash(self["inputs"])

        self.setdefault("findings", [])
        if not isinstance(self["findings"], list):
            raise SchemaError(f"OutputDoc: 'findings' field must be a list, got {type(self['findings']).__name__}.")
        self["findings_hash"] = self._hash(self["findings"])
        
        self.setdefault("input_schema_version", self["inputs"].get("input_schema_version", "UNKNOWN")) 
        self.setdefault("output_schema_version", "UNKNOWN") 
        self.setdefault("analytic_id", "UNKNOWN")           
        self.setdefault("analytic_name", "UNKNOWN")         
        self.setdefault("analytic_version", "UNKNOWN")      
        self.setdefault("status", "UNKNOWN")                
        self.setdefault("exit_code", -1)                    
        self.setdefault("records_processed", 0)             

        _validate(self, OUTPUT_SCHEMA, path="OutputDoc")

    def save(self, path: Union[str, pathlib.Path], *, indent: int = 2) -> None:
        if not self.get("run_id"): 
            print("Warning: OutputDoc.save() called before finalise(). Document may be incomplete or fail validation.", file=sys.stderr)
        
        path_obj = pathlib.Path(path)
        try:
            path_obj.write_text(json.dumps(self, indent=indent,ensure_ascii=False), encoding="utf-8")
            print(f"Output document saved to: {path_obj.resolve()}", file=sys.stderr)
        except Exception as e:
            print(f"Error saving output document to {path_obj}: {e}", file=sys.stderr)
            raise

def _get_user() -> str:
    try:
        return getpass.getuser()
    except Exception: 
        return "unknown_user"

def _get_host() -> str:
    try:
        return socket.gethostname()
    except Exception:
        return "unknown_host"

# ──────────────────────────────────────────────────────────────────────────────
# 5.  CLI entry-point  (python -m analytic_schema …)
# ──────────────────────────────────────────────────────────────────────────────
def _main(cli_args: List[str]) -> None:
    """Main CLI handler for validating input documents."""
    parser = _build_arg_parser() 
    if not cli_args or ('-h' in cli_args or '--help' in cli_args):
        parser.print_help(sys.stdout)
        sys.exit(0)

    print("Attempting to parse and validate input...", file=sys.stderr)
    try:
        raw_input_params = parse_input(cli_args) 
        print(f"Raw parsed input parameters: {raw_input_params}", file=sys.stderr)
        
        validated_params = validate_input(raw_input_params)

        print("\n✓ Input document is valid and conforms to the schema.")
        print("\nCanonicalised Input:")
        print(json.dumps(validated_params, indent=2, sort_keys=True))
        sys.exit(0)

    except (SchemaError, FileNotFoundError, json.JSONDecodeError, ValueError, TypeError) as exc:
        print(f"\n✗ Validation Error: {exc}", file=sys.stderr)
        sys.exit(1)
    except SystemExit as e: 
        sys.exit(e.code if e.code is not None else 1) 
    except Exception as exc: 
        import traceback
        print(f"\n✗ An unexpected error occurred: {exc}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(2)

if __name__ == "__main__":
    _main(sys.argv[1:])

