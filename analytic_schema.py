#!/usr/bin/env python3
"""
analytic_schema.py
~~~~~~~~~~~~~~~~~~

A **utility module** for validating *input* and *output* documents used by
analytics notebooks or batch-style data-processing scripts.

The module contains **three major capabilities**:

1. **Schema definition loader** – Reads *analytic_schema.json* from disk and
   exposes two dictionaries—``INPUT_SCHEMA`` and ``OUTPUT_SCHEMA``—that are then
   used throughout the module.

2. **Light-weight validation engine** – Implements the minimal subset of
   JSON-Schema required for the contract (enough for types, enums, ``format:
   date-time``, ``required`` fields, simple ``oneOf`` branches, and
   ``additionalProperties``).  It deliberately avoids external dependencies so
   the file can ship with a self-contained notebook or run on restricted
   hosts.

3. **Public helper API**

   - :func:`parse_input` …… Parse CLI strings, ``sys.argv``, JSON files, or
     already-constructed ``dict`` objects into a raw *input* dictionary.
   - :func:`validate_input` …… Canonicalise & validate dictionaries against
     ``INPUT_SCHEMA`` (handles ``--config`` files and “object-or-path”
     convenience parameters and now **injects sensible defaults for any missing
     optional fields**).
   - :class:`OutputDoc` …… A thin subclass of :class:`dict` that
     accumulates results and meta-data, self-hashes its payload, validates
     itself against ``OUTPUT_SCHEMA``, and finally serialises to disk.

Typical execution flow
----------------------
>>> import analytic_schema as asc
>>>
>>> raw_params = asc.parse_input("--input-schema-version 1.0.0 "
...                              "--start-dtg 2025-06-01T00:00:00Z "
...                              "--end-dtg 2025-06-02T00:00:00Z "
...                              "--data-source-type file "
...                              "--data-source /tmp/conn.csv")
>>> params = asc.validate_input(raw_params)
>>> # … run notebook / analytic code …
>>> out = asc.OutputDoc(input_data_hash="deadbeef"*8, inputs=params)
>>> out.add_message("info", "Analysis completed without error.")
>>> out.finalise()
>>> out.save("output.json")

Command-line usage
------------------
The module can also be run *directly* as a script for quick validation:

.. code-block:: console

   $ python -m analytic_schema --help
   $ python -m analytic_schema --config params.json
   $ python -m analytic_schema --input-schema-version 1.0.0 …

All command-line options are auto-generated from ``INPUT_SCHEMA``.

Design philosophy
-----------------
* **Self-contained** – No non-stdlib dependencies (aside from *pandas*, which
  many analytics stacks already ship) so the file can be vendor-dropped into
  an air-gapped notebook.
* **Explicit, defensive errors** – Every user-facing error tries to pin-point
  the offending field *and* the reason (wrong type, missing enum, bad datetime,
  etc.).
* **Helpful run-time logging** – During CLI validation the script prints what
  it’s doing and why, aiding fast iteration in notebooks or CI pipelines.
"""

from __future__ import annotations

# ─── Standard library ─────────────────────────────────────────────────────────
import argparse
import datetime as _dt
import enum
import functools
import getpass          # User lookup for OutputDoc meta-data
import hashlib
import json
import pathlib
import re
import shlex
import socket           # Hostname lookup for OutputDoc meta-data
import sys
import uuid
from typing import Any, Dict, List, Mapping, Sequence, Tuple, Union
import copy

# ─── Third-party (widely available) ───────────────────────────────────────────
import pandas as pd

# =============================================================================
# 0.  Support execution in notebook or as standalone scripts
# =============================================================================

def display_output(obj, *args, **kwargs):
    """
    Smart wrapper that renders *obj* in a notebook **or** prints it in a
    terminal while still accepting the usual ``print`` keyword arguments
    (e.g. ``file=sys.stderr``).

    Parameters
    ----------
    obj
        The object or string to display.
    *args, **kwargs
        Forwarded verbatim to :func:`IPython.display.display` when an IPython
        shell is detected, otherwise to :func:`print`.
    """
    try:
        get_ipython  # type: ignore
        from IPython.display import display
        display(obj, *args, **kwargs)
    except NameError:
        print(obj, *args, **kwargs)

# =============================================================================
# 1.  Load the contract – INPUT_SCHEMA / OUTPUT_SCHEMA
# =============================================================================
# NOTE: The JSON file is considered the *single source of truth*.  This Python
# file merely *loads* the contract at run-time so any change to
# analytic_schema.json is automatically picked up without touching code.

# Use a path relative to THIS script file, not the current working directory.
_SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()
SCHEMA_PATH = _SCRIPT_DIR / "analytic_schema.json"

try:
    with SCHEMA_PATH.open("r", encoding="utf-8") as fd:
        _schema_file = json.load(fd)
except FileNotFoundError as exc:   # Fail fast if the schema is missing
    raise FileNotFoundError(
        f"Contract file {SCHEMA_PATH!s} not found.  "
        "The module cannot validate without it."
    ) from exc
except json.JSONDecodeError as exc:
    raise ValueError(
        f"Contract file {SCHEMA_PATH!s} is not valid JSON: {exc}"
    ) from exc

# NOTE: deepcopy – ensures downstream mutation of one dict cannot corrupt the
#       other (e.g. when we embed INPUT_SCHEMA inside OUTPUT_SCHEMA).
INPUT_SCHEMA: Dict[str, Any] = copy.deepcopy(_schema_file["input"])
OUTPUT_SCHEMA: Dict[str, Any] = copy.deepcopy(_schema_file["output"])

# For convenience the *entire* INPUT_SCHEMA object is embedded by reference
# inside OUTPUT_SCHEMA → properties → inputs so the validator can recurse.
OUTPUT_SCHEMA["properties"]["inputs"] = copy.deepcopy(INPUT_SCHEMA)

# --------------------------------------------------------------------------- #
# 1.a  Optional‑field defaults – applied during `validate_input`
# --------------------------------------------------------------------------- #
_DEFAULTS: Dict[str, Any] = {
    # NOTE: `{run_id}` & `{execution_dtg}` placeholders are preserved so the
    #       calling code (or OutputDoc) can substitute concrete values later.
    "log_path": "./{run_id}_{execution_dtg}.log",
    "output": "stdout",
    "analytic_parameters": {},
    "data_map": {},
    "verbosity": "INFO",
}


def _apply_defaults(mapping: Dict[str, Any]) -> None:
    """Inject default values for any *optional* INPUT_SCHEMA fields that are
    missing from *mapping* **in‑place**.

    This ensures downstream code never has to guard against absent keys
    and that the *canonical* parameter dictionary is fully populated.
    """
    for key, default_val in _DEFAULTS.items():
        if key not in mapping:
            # deepcopy to avoid mutation surprises for mutable defaults
            mapping[key] = copy.deepcopy(default_val)

# =============================================================================
# 2.  Tiny validation engine – implements *just enough* of JSON-Schema
# =============================================================================

class SchemaError(ValueError):
    """Raised whenever a value fails validation against the schema contract."""

# --------------------------------------------------------------------------- #
# Regex helper for ISO-8601 timestamps.  We accept:
#   • 2025-06-01T12:34:56Z
#   • 2025-06-01T12:34:56.123Z
#   • 2025-06-01T12:34:56+00:00
#   • 2025-06-01T12:34:56-05:00
# --------------------------------------------------------------------------- #
_DT_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"  # date & time
    r"(?:\.\d{1,6})?"          # Optional .microseconds
    r"(?:Z|[+-]\d{2}:\d{2})$"  # Z or ±HH:MM
)


def _is_datetime(s: Any) -> bool:
    """Return ``True`` if *s* is a **string** that satisfies ISO‑8601."""
    if not isinstance(s, str) or not _DT_RE.match(s):
        return False
    try:
        _dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False

# Map JSON-Schema “type” parameter → Python class(es)
_TYPE_DISPATCH: Dict[str, Union[type, Tuple[type, ...]]] = {
    "string": str,
    "integer": int,
    "number": (int, float),
    "object": dict,
    "array": list,
    "boolean": bool,
    "dataframe": pd.DataFrame,  # convenience extension
}

def _validate(data: Any, schema: Mapping[str, Any], *, path: str = "") -> None:
    """
    Recursively validate *data* against *schema*.

    The implementation supports the handful of JSON-Schema features required by
    the analytics contract and is intentionally minimalistic:

    * ``type`` & ``enum``
    * ``format: date-time``
    * ``oneOf`` branches
    * ``required`` and ``additionalProperties`` for objects
    * Homogeneous ``array`` item types

    Parameters
    ----------
    data
        Arbitrary Python object to validate.
    schema
        A mapping that follows the JSON-Schema vocabulary.
    path
        “Breadcrumb” path used to build human-readable error messages
        (e.g. ``inputs.start_dtg``).

    Raises
    ------
    SchemaError
        If *data* does not conform to *schema*.
    """
    # ── 1) Type check ────────────────────────────────────────────────────────
    stype = schema.get("type")
    if stype:
        expected = _TYPE_DISPATCH.get(stype)
        if expected and not isinstance(data, expected):
            raise SchemaError(
                f"{path or 'value'}: expected {stype}, got {type(data).__name__}"
            )

    # ── 2) Enum check ────────────────────────────────────────────────────────
    if "enum" in schema and data not in schema["enum"]:
        raise SchemaError(
            f"{path}: '{data}' not in allowed values {schema['enum']}"
        )

    # ── 3) Format “date-time” special-case ───────────────────────────────────
    if schema.get("format") == "date-time" and not _is_datetime(data):
        raise SchemaError(
            f"{path}: '{data}' is not a valid ISO-8601 date-time string"
        )

    # ── 4) oneOf – short-circuit on first success ───────────────────────────
    if "oneOf" in schema:
        sub_errors: List[str] = []
        for i, sub_schema in enumerate(schema["oneOf"]):
            try:
                _validate(data, sub_schema, path=path)  # same breadcrumb
                break  # Success – do not evaluate other branches
            except SchemaError as exc:
                sub_errors.append(
                    f"  option {i} ({sub_schema.get('type', 'unknown')}): {exc}"
                )
        else:  # No branch matched
            raise SchemaError(
                f"{path}: does not match any allowed schema in oneOf\n"
                + "\n".join(sub_errors)
            )

    # ── 5) Object recursive property validation ─────────────────────────────
    if stype == "object":
        props = schema.get("properties", {})
        required = set(schema.get("required", []))

        if not schema.get("additionalProperties", True) and isinstance(data, dict):
            unknown = set(data.keys()) - props.keys()
            if unknown:
                raise SchemaError(
                    f"{path}: unexpected fields {sorted(unknown)}"
                )

        if isinstance(data, dict):
            missing = required - data.keys()
            if missing:
                raise SchemaError(
                    f"{path}: missing required fields {sorted(missing)}"
                )

            # Recurse into known properties
            for key, val in data.items():
                if key in props:
                    child_path = f"{path}.{key}" if path else key
                    _validate(val, props[key], path=child_path)
        else:  # Provided value is *not* a dict but object required
            raise SchemaError(
                f"{path}: expected object, got {type(data).__name__}"
            )

    # ── 6) Array items ───────────────────────────────────────────────────────
    elif stype == "array":
        items_schema = schema.get("items")
        if items_schema and isinstance(data, list):
            for idx, item in enumerate(data):
                _validate(item, items_schema, path=f"{path}[{idx}]")
        elif items_schema:  # Non-list supplied where list is required
            raise SchemaError(
                f"{path}: expected array, got {type(data).__name__}"
            )

# =============================================================================
# 3.  Input parsing helpers
# =============================================================================
@functools.lru_cache(maxsize=1)
def _build_arg_parser() -> argparse.ArgumentParser:
    """
    Dynamically build an :class:`argparse.ArgumentParser` from ``INPUT_SCHEMA``.

    UX improvements (v3.3)
    ----------------------
    * ``fromfile_prefix_chars='@'`` – supply huge parameter sets via *response
      files* (e.g. ``@params.txt``).
    * ``--version`` – quickly print the schema version embedded in the JSON
      contract and exit.
    """

    parser = argparse.ArgumentParser(
        description=INPUT_SCHEMA.get(
            "description", "Analytics Notebook Input Parameters"
        ),
        add_help=False,  # we inject our own so it appears first
        fromfile_prefix_chars="@",  # NEW: enable @response‑file syntax
    )

    # Core generic flags ------------------------------------------------------
    parser.add_argument(
        "-h", "--help",
        action="help",
        default=argparse.SUPPRESS,
        help="Show this help message and exit.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"{SCHEMA_PATH.name} : {INPUT_SCHEMA.get('version', 'unknown')}",
        help="Print schema contract version and exit.",
    )
    parser.add_argument(
        "--config",
        metavar="FILE",
        help=(
            "Path to JSON file containing the full input object.  If supplied, "
            "all other CLI flags are ignored (the file becomes authoritative)."
        ),
    )

    # Auto‑generate one flag per top‑level schema property --------------------
    for prop_name, prop_spec in INPUT_SCHEMA["properties"].items():
        cli_flag = f"--{prop_name.replace('_', '-')}"  # e.g. --input-schema-version
        kwargs: Dict[str, Any] = {
            "dest": prop_name,
            "help": prop_spec.get("description", ""),
            "required": False,  # schema handles requiredness later
        }
        stype = prop_spec.get("type")
        if stype == "boolean":
            kwargs["action"] = argparse.BooleanOptionalAction
        elif stype == "integer":
            kwargs["type"] = int
        elif stype == "number":
            kwargs["type"] = float
        if "enum" in prop_spec:
            kwargs["choices"] = prop_spec["enum"]
        parser.add_argument(cli_flag, **kwargs)

    return parser

# --------------------------------------------------------------------------- #
# Public Input helpers
# --------------------------------------------------------------------------- #
def parse_input(
    source: Union[None, pathlib.Path, str, Sequence[str], Mapping[str, Any]] = None) -> Dict[str, Any]:
    """
    Convert *source* into a **raw** parameter dictionary (no validation yet).

    Parameters
    ----------
    source
        • ``None`` …… use ``sys.argv`` (skip the executable path).  
        • :class:`pathlib.Path` …… Read the file content as JSON.  
        • ``str``  
          – If the *string* is valid JSON → ``json.loads``.  
          – If it is a path to a file that exists → read & loads JSON.  
          – Otherwise treat as a command-line string and ``shlex.split``.  
        • :class:`Sequence[str]`` …… Already tokenised CLI arguments.  
        • :class:`Mapping`` …… Assumed to be *already* the raw parameter dict.

    Returns
    -------
    dict
        Raw parameter dictionary **exactly** as supplied – no coercion,
        canonicalisation, or schema validation has occurred yet.

    Raises
    ------
    FileNotFoundError
        If a path argument points to a non-existent file.
    json.JSONDecodeError
        If a file or string is not valid JSON where JSON was expected.
    TypeError
        For unsupported *source* types.
    """
    # ── 1) Already a mapping → nothing to do
    if isinstance(source, Mapping):
        return dict(source)  # shallow copy for isolation

    # ── 2) pathlib.Path → load JSON file
    if isinstance(source, pathlib.Path):
        try:
            return json.loads(source.read_text(encoding="utf-8"))
        except FileNotFoundError:
            raise  # propagate as is
        except json.JSONDecodeError as exc:
            raise json.JSONDecodeError(
                f"Invalid JSON in file {source}: {exc.msg}", exc.doc, exc.pos
            ) from exc

    # Prepare argv list for argparse (may come from multiple pathways below)
    argv: List[str]

    # ── 3) str (could be JSON, path, or CLI string) ─────────────────────────
    if isinstance(source, str):
        path_candidate = pathlib.Path(source)
        if path_candidate.is_file():
            # Treat as file path
            try:
                return json.loads(path_candidate.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:
                raise json.JSONDecodeError(
                    f"Invalid JSON in file {source}: {exc.msg}", exc.doc, exc.pos
                ) from exc
            except Exception as exc:
                raise ValueError(
                    f"Error reading file {source} as JSON: {exc}"
                ) from exc
        # Not a file – maybe raw JSON string?
        try:
            return json.loads(source)
        except json.JSONDecodeError:
            # Fallback: treat as CLI string
            argv = shlex.split(source)

    # ── 4) None → use sys.argv[1:]  |  Sequence[str] → list() ───────────────
    elif source is None:
        argv = sys.argv[1:]
    elif isinstance(source, Sequence) and not isinstance(source, (str, bytes)):
        argv = list(source)  # copy
    else:
        raise TypeError(f"Unsupported input type for parse_input: {type(source)}")

    # ── 5) Use argparse to convert CLI tokens → dict ────────────────────────
    parser = _build_arg_parser()
    try:
        namespace, unknown = parser.parse_known_args(argv)
        if unknown:
            raise ValueError(
                f"Unknown argument(s): {unknown}.  Use --help for valid options."
            )

    except SystemExit as exc:
        # Re-raise so callers can handle (e.g. exit(2) in CLI _main)
        raise exc

    return {k: v for k, v in vars(namespace).items() if v is not None}

# --------------------------------------------------------------------------- #
def validate_input(raw_params: Dict[str, Any]) -> Dict[str, Any]:
    """Canonicalise *raw_params*, apply defaults, and validate.

    **NEW BEHAVIOUR** (v3.4)
    -------------------------
    After dereferencing any *object‑or‑path* parameters, this function now
    populates **all optional fields** with sensible defaults so that the
    returned dictionary is *self‑contained* and can be embedded verbatim into
    the Output document without further massaging.
    """

    # Defensive: ensure param is dict-like
    if not isinstance(raw_params, dict):
        raise TypeError(
            f"validate_input expects dict, got {type(raw_params).__name__}"
        )

    # --------------------------------------------------------------------- #
    # 1) Handle --config override (identical logic)                         #
    # --------------------------------------------------------------------- #
    data: Dict[str, Any] = raw_params.copy()  # work on a *copy*
    config_flag = data.pop("config", None)    # remove early

    if isinstance(config_flag, str):
        config_path = pathlib.Path(config_flag)
        if not config_path.is_file():
            raise FileNotFoundError(
                f"--config path '{config_flag}' does not exist or is not a file."
            )
        try:
            data = json.loads(config_path.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                raise ValueError("Config file must contain a JSON object.")
        except json.JSONDecodeError as exc:
            raise json.JSONDecodeError(
                f"Invalid JSON in --config file {config_flag}: {exc.msg}",
                exc.doc,
                exc.pos,
            ) from exc
    elif config_flag is not None:  # Some non-string value supplied
        raise TypeError("--config expects a string path to a JSON file.")

    # --------------------------------------------------------------------- #
    # 2) Dereference “object-or-path” convenience fields (unchanged logic)  #
    # --------------------------------------------------------------------- #
    for key in ("analytic_parameters", "data_map"):
        if key not in data:
            continue
        val = data[key]
        if isinstance(val, str):
            path_val = pathlib.Path(val)
            # a) File path?
            if path_val.is_file():
                try:
                    loaded = json.loads(path_val.read_text(encoding="utf-8"))
                    if not isinstance(loaded, dict):
                        raise ValueError(f"File '{val}' for '{key}' is not a JSON object.")
                    data[key] = loaded
                    continue  # done
                except json.JSONDecodeError as exc:
                    raise json.JSONDecodeError(
                        f"Invalid JSON in file '{val}' for '{key}': {exc.msg}",
                        exc.doc,
                        exc.pos,
                    ) from exc
            # b) Inline JSON string?
            try:
                parsed = json.loads(val)
                if isinstance(parsed, dict):
                    data[key] = parsed
            except json.JSONDecodeError:
                # leave as original string – schema will decide if that’s allowed
                pass

    # --------------------------------------------------------------------- #
    # 3) **Apply defaults for any optional fields not supplied**            #
    # --------------------------------------------------------------------- #
    _apply_defaults(data)

    # --------------------------------------------------------------------- #
    # 4) Final schema validation                                            #
    # --------------------------------------------------------------------- #
    try:
        _validate(data, INPUT_SCHEMA)
    except SchemaError as exc:
        if isinstance(config_flag, str):
            raise SchemaError(
                f"Input validation failed (from config={config_flag}): {exc}"
            ) from exc
        raise

    return data

# =============================================================================
# 4.  Output document helper
# =============================================================================
class _Level(enum.Enum):
    """Internal enum for structured log message severity."""
    DEBUG = "DEBUG"
    INFO  = "INFO"
    WARN  = "WARN"
    ERROR = "ERROR"
    FATAL = "FATAL"

class OutputDoc(dict):
    """
    Convenience helper for building *output* documents that conform to
    ``OUTPUT_SCHEMA``.

    The class piggy-backs on :class:`dict` so users can freely add custom
    fields.  Core responsibilities:

    * Record run meta-data (start/end time, user, host, UUID, duration).
    * Self-hash critical sections (``inputs`` and ``findings``) for tamper
      detection.
    * Provide :py:meth:`add_message` for structured logging during execution.
    * Validate itself against the contract in :py:meth:`finalise`.
    * Serialize to disk via :py:meth:`save`.

    **Typical usage**

    >>> out = OutputDoc(input_data_hash="…", inputs=validated_inputs)
    >>> out.add_message("info", "Begin main loop…")
    >>> out["findings"].append({"score": 0.92, "comment": "Suspicious beacon"})
    >>> out.finalise()
    >>> out.save("analysis_output.json")
    """

    # --------------------------------------------------------------------- #
    # Construction
    # --------------------------------------------------------------------- #
    def __init__(self, *, input_data_hash: str, **kwargs: Any) -> None:
        """
        Create a new *OutputDoc*.

        Parameters
        ----------
        input_data_hash
            SHA-256 hash (hex-encoded) of the **raw input data** used by the
            analytic.  Including it allows external auditors to confirm which
            data led to which findings.
        **kwargs
            Any top-level fields the caller wishes to pre-populate.  Common
            ones are ``inputs`` (validated input dict) and ``findings`` (list).
        """
        super().__init__(**kwargs)

        if not isinstance(input_data_hash, str):
            raise TypeError("input_data_hash must be a hex-encoded string.")
        self["input_data_hash"] = input_data_hash

        # Private attr to compute run duration later
        self.__start_time = _dt.datetime.now(_dt.timezone.utc)

        # Guarantee messages list exists early
        self.setdefault("messages", [])

    # --------------------------------------------------------------------- #
    # Public logger
    # --------------------------------------------------------------------- #
    def add_message(self, level: Union[str, _Level], text: str) -> None:
        """
        Append a structured log entry (ISO timestamp, severity, text).

        Parameters
        ----------
        level
            Human-friendly string (case-insensitive) or :class:`_Level` enum.
        text
            The message payload.

        Raises
        ------
        ValueError | TypeError
            For invalid enum values or non-string messages.
        """
        # Normalise level to string version
        if isinstance(level, str):
            try:
                level_val = _Level[level.upper()].value
            except KeyError:
                allowed = ", ".join(_Level.__members__)
                raise ValueError(f"Invalid log level '{level}'. Allowed: {allowed}")
        elif isinstance(level, _Level):
            level_val = level.value
        else:
            raise TypeError("level must be str or _Level enum.")

        if not isinstance(text, str):
            raise TypeError("text must be a string.")

        self["messages"].append(
            {
                "timestamp": _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds"),
                "level": level_val,
                "text": text,
            }
        )

    # ────────────────────────────────────────────────────────────────────
    # Internal helpers
    # ────────────────────────────────────────────────────────────────────
    @staticmethod
    def _json_safe(x: Any) -> Any:
        """
        Recursively convert *x* into a structure that ``json.dumps`` accepts.

        * **pandas.DataFrame** → replace with
          ``{"__dataframe_sha256__": "<digest>"}``.
        * dicts/lists/tuples → recurse.
        * primitives are returned unchanged.
        """
        if isinstance(x, pd.DataFrame):
            df_json = x.to_json(orient="split", date_unit="ns")
            return {"__dataframe_sha256__": hashlib.sha256(df_json.encode()).hexdigest()}
        if isinstance(x, dict):
            return {k: OutputDoc._json_safe(v) for k, v in x.items()}
        if isinstance(x, (list, tuple)):
            return [OutputDoc._json_safe(v) for v in x]
        return x  # str / int / float / bool / None

    @classmethod
    def _hash(cls, obj: Any) -> str:
        """
        Stable SHA-256 over *obj* after JSON-safe conversion.
        """
        safe = cls._json_safe(obj)
        return hashlib.sha256(
            json.dumps(safe, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    def _to_serialisable(self) -> Dict[str, Any]:
        """Deep-copy of self with all values passed through `_json_safe`."""
        return OutputDoc._json_safe(self)

    # In finalise(), update the hashing call to use the serialised version
    def finalise(self) -> None:
        # ------------------------------------------------------------------
        # Guard: inputs must exist and be a dict
        # ------------------------------------------------------------------
        if "inputs" not in self or not isinstance(self["inputs"], dict):
            raise SchemaError("OutputDoc.finalise(): missing or invalid 'inputs' dict.")

        run_end = _dt.datetime.now(_dt.timezone.utc)

        # --- Populate meta defaults ------------------------------------------------
        self.setdefault("run_id", str(uuid.uuid4()))
        self.setdefault("run_user", _get_user())
        self.setdefault("run_host", _get_host())
        self.setdefault("run_start_dtg", self.__start_time.isoformat(timespec="seconds"))
        self.setdefault("run_end_dtg",   run_end.isoformat(timespec="seconds"))
        self.setdefault(
            "run_duration_seconds",
            round((run_end - self.__start_time).total_seconds(), 6),
        )
        
        serialisable_inputs = self._to_serialisable()["inputs"]
        self["input_hash"] = self._hash(self["inputs"])
        
        self.setdefault("findings", [])
        if not isinstance(self["findings"], list):
            raise SchemaError("'findings' must be a list.")

        # Findings are already JSON-native, no conversion needed.
        self["findings_hash"] = self._hash(self["findings"])

        # --- Additional optional fields (safe defaults) ---------------------------
        self.setdefault("input_schema_version",  self["inputs"].get("input_schema_version", "UNKNOWN"))
        self.setdefault("output_schema_version", "UNKNOWN")
        self.setdefault("analytic_id",           "UNKNOWN")
        self.setdefault("analytic_name",         "UNKNOWN")
        self.setdefault("analytic_version",      "UNKNOWN")
        self.setdefault("status",                "UNKNOWN")
        self.setdefault("exit_code",             -1)
        self.setdefault("records_processed",     0)

        # --- Final schema validation ----------------------------------------------
        _validate(self, OUTPUT_SCHEMA, path="OutputDoc")

    def save(self, path: Union[str, pathlib.Path], *, indent: int = 2) -> None:
        """
        Write the validated document to *path* as pretty-printed JSON.
        ... (docstring) ...
        """
        if not self.get("run_id"):
            raise RuntimeError(
                "OutputDoc.save() called before finalise(); "
                "document could be incomplete."
            )

        path_obj = pathlib.Path(path)
        # Create a fully serialisable version of the document before dumping
        serialisable_doc = self._to_serialisable()

        try:
            path_obj.write_text(
                json.dumps(serialisable_doc, indent=indent, ensure_ascii=False),
                encoding="utf-8"
            )
            display_output(f"Output document saved to: {path_obj.resolve()}", file=sys.stderr)
        except Exception as exc:
            display_output(f"Error saving output document to {path_obj}: {exc}", file=sys.stderr)
            raise  # Re-throw to allow caller handling

# =============================================================================
# 5.  Tiny helpers (user / host lookup)
# =============================================================================
def _get_user() -> str:
    """Return the current OS user or *unknown_user* if lookup fails."""
    try:
        return getpass.getuser()
    except Exception:
        return "unknown_user"

def _get_host() -> str:
    """Return the current hostname or *unknown_host* if lookup fails."""
    try:
        return socket.gethostname()
    except Exception:
        return "unknown_host"

# =============================================================================
# 6.  Unit tests
# =============================================================================
if __name__ == "__main__":
    """
    When the module is executed directly, run a comprehensive self-test suite.
    These tests cover **all** public behaviours of the helper API
    (`parse_input`, `validate_input`) and of `OutputDoc`.  Each test checks
    every parameter supplied **and** confirms that *default* values are set
    correctly whenever fields are omitted by the caller.
    """
    import unittest
    import sys
    import json
    import tempfile
    from pathlib import Path
    import pandas as pd

    # --------------------------------------------------------------------- #
    # Test-cases
    # --------------------------------------------------------------------- #
    class AnalyticSchemaTests(unittest.TestCase):
        """
        Exhaustive integration tests for *analytic_schema.py*.

        Workflow coverage:
        1. CLI → parse_input → validate_input
        2. Dict + inline-JSON dereference (`analytic_parameters`)
        3. Dict with embedded Pandas DataFrame
        4. --config precedence
        5. OutputDoc lifecycle (including default-value population)
        """

        @staticmethod
        def _tmp_json(obj) -> Path:
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
            Path(tmp.name).write_text(json.dumps(obj), encoding="utf-8")
            return Path(tmp.name)

        # --------------------------------------------------------------- #
        # Helper: build expected canonical dict with defaults injected    #
        # --------------------------------------------------------------- #
        def _with_defaults(self, base: Dict[str, Any]) -> Dict[str, Any]:
            x = base.copy()
            for k, v in _DEFAULTS.items():
                x.setdefault(k, copy.deepcopy(v))
            return x

        # 1) CLI round‑trip now returns dict **with defaults**
        def test_cli_roundtrip(self):
            cli = (
                "--input-schema-version 1.0.0 "
                "--start-dtg 2025-06-01T00:00:00Z "
                "--end-dtg 2025-06-02T00:00:00Z "
                "--data-source-type file "
                "--data-source /tmp/conn.csv"
            )
            raw = parse_input(cli)
            canonical = validate_input(raw)
            expected = self._with_defaults(raw)
            self.assertDictEqual(canonical, expected)

        # 2) Dict + JSON dereference (unchanged – defaults already handled)
        def test_dict_with_analytic_parameters(self):
            raw_dict = {
                "input_schema_version": "1.0.0",
                "start_dtg": "2025-06-01T00:00:00Z",
                "end_dtg": "2025-06-02T00:00:00Z",
                "data_source_type": "file",
                "data_source": "/tmp/conn.csv",
                "analytic_parameters": '{"param_a": 123}',
            }
            canonical = validate_input(parse_input(raw_dict))
            self.assertEqual(canonical["analytic_parameters"], {"param_a": 123})

        # 3) DataFrame source (unchanged)
        def test_dataframe_source(self):
            df = pd.DataFrame({"Name": ["Alice", "Bob"]})
            raw_dict = {
                "input_schema_version": "1.0.0",
                "start_dtg": "2025-06-01T00:00:00Z",
                "end_dtg": "2025-06-02T00:00:00Z",
                "data_source_type": "df",
                "data_source": df,
            }
            canonical = validate_input(parse_input(raw_dict))
            self.assertTrue(canonical["data_source"].equals(df))

        # 4) --config precedence – now with defaults merged in
        def test_config_file_override(self):
            cfg = {
                "input_schema_version": "1.0.0",
                "start_dtg": "2025-07-01T00:00:00Z",
                "end_dtg": "2025-07-02T00:00:00Z",
                "data_source_type": "api endpoint",
                "data_source": "https://api.example.com/data",
            }
            cfg_path = self._tmp_json(cfg)
            raw = parse_input(["--config", str(cfg_path)])
            canonical = validate_input(raw)
            expected = self._with_defaults(cfg)
            self.assertDictEqual(canonical, expected)

        # =============================
        # 5) OutputDoc lifecycle + defaults
        # =============================
        def test_output_doc_defaults(self):
            """
            Create an OutputDoc with *minimal* user-supplied fields and verify:
            • Required hashes present after finalise()
            • All schema-defined defaults populated with correct values
              (output_schema_version, analytic_id, status, exit_code, etc.)
            • Messages list initialised and accepts entries
            """
            inputs = validate_input(
                {
                    "input_schema_version": "1.0.0",
                    "start_dtg": "2025-06-01T00:00:00Z",
                    "end_dtg":   "2025-06-02T00:00:00Z",
                    "data_source_type": "file",
                    "data_source": "/tmp/conn.csv",
                }
            )

            out = OutputDoc(input_data_hash="0" * 64, inputs=inputs)
            out.add_message("INFO", "Initial message.")
            out.finalise()

            # — Mandatory hashes —
            self.assertIn("input_hash", out)
            self.assertIn("findings_hash", out)

            # — Default fields (not explicitly set by caller) —
            self.assertEqual(out["output_schema_version"], "UNKNOWN")
            self.assertEqual(out["analytic_id"],  "UNKNOWN")
            self.assertEqual(out["analytic_name"], "UNKNOWN")
            self.assertEqual(out["analytic_version"], "UNKNOWN")
            self.assertEqual(out["status"], "UNKNOWN")
            self.assertEqual(out["exit_code"], -1)
            self.assertEqual(out["records_processed"], 0)

            # — Auto meta —
            self.assertIn("run_id", out)
            self.assertIn("run_user", out)
            self.assertIn("run_host", out)
            self.assertIn("run_start_dtg", out)
            self.assertIn("run_end_dtg", out)
            self.assertGreater(out["run_duration_seconds"], 0)

            # — Messages list correctly initialised —
            self.assertEqual(len(out["messages"]), 1)
            msg = out["messages"][0]
            self.assertIn("timestamp", msg)
            self.assertEqual(msg["level"], "INFO")
            self.assertEqual(msg["text"], "Initial message.")

        # =============================
        # 6) Missing required field
        # =============================
        def test_missing_required_field(self):
            """
            Omitting any **required** top-level parameter must raise
            `SchemaError`.  Here we drop `data_source` and expect failure.
            """
            invalid = {
                "input_schema_version": "1.0.0",
                "start_dtg": "2025-06-01T00:00:00Z",
                "end_dtg":   "2025-06-02T00:00:00Z",
                "data_source_type": "file",
                # data_source  ← deliberately omitted
            }
            with self.assertRaises(SchemaError):
                validate_input(parse_input(invalid))

        # =============================
        # 7) Invalid enum value
        # =============================
        def test_invalid_enum_value(self):
            """
            Supplying a value outside an `enum` must raise `SchemaError`.
            `data_source_type='ftp'` is not allowed by the contract.
            """
            invalid = {
                "input_schema_version": "1.0.0",
                "start_dtg": "2025-06-01T00:00:00Z",
                "end_dtg":   "2025-06-02T00:00:00Z",
                "data_source_type": "ftp",       # ← invalid
                "data_source": "/tmp/conn.csv",
            }
            with self.assertRaises(SchemaError):
                validate_input(parse_input(invalid))

        # =============================
        # 8) Invalid date-time format
        # =============================
        def test_invalid_datetime_format(self):
            """
            Timestamps must be full ISO-8601 with time + zone.  A date-only
            string should fail validation.
            """
            invalid = {
                "input_schema_version": "1.0.0",
                "start_dtg": "2025-06-01",                # ← bad format
                "end_dtg":   "2025-06-02T00:00:00Z",
                "data_source_type": "file",
                "data_source": "/tmp/conn.csv",
            }
            with self.assertRaises(SchemaError):
                validate_input(parse_input(invalid))

        # =============================
        # 9) File-path dereference
        # =============================
        def test_analytic_parameters_file_deref(self):
            """
            `analytic_parameters` supplied as a *file path* must be
            auto-loaded into a dict during validation.
            """
            tmp_file = self._tmp_json({"param_x": 42})
            raw = {
                "input_schema_version": "1.0.0",
                "start_dtg": "2025-06-01T00:00:00Z",
                "end_dtg":   "2025-06-02T00:00:00Z",
                "data_source_type": "file",
                "data_source": "/tmp/conn.csv",
                "analytic_parameters": str(tmp_file),
            }
            canonical = validate_input(parse_input(raw))
            self.assertEqual(canonical["analytic_parameters"], {"param_x": 42})

        # =============================
        # 10) AdditionalProperties check
        # =============================
        def test_unknown_field_rejected(self):
            """
            The schema sets `"additionalProperties": false`; any extraneous
            field should trigger `SchemaError`.
            """
            invalid = {
                "input_schema_version": "1.0.0",
                "start_dtg": "2025-06-01T00:00:00Z",
                "end_dtg":   "2025-06-02T00:00:00Z",
                "data_source_type": "file",
                "data_source": "/tmp/conn.csv",
                "unexpected_field": "oops",      # ← not in schema
            }
            with self.assertRaises(SchemaError):
                validate_input(parse_input(invalid))

        # =============================
        # 11) Invalid JSON in file path
        # =============================
        def test_analytic_parameters_invalid_json_file(self):
            """
            A file path supplied for `analytic_parameters` that is **not**
            valid JSON must raise `json.JSONDecodeError`.
            """
            bad_file = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
            Path(bad_file.name).write_text("NOT-JSON", encoding="utf-8")

            raw = {
                "input_schema_version": "1.0.0",
                "start_dtg": "2025-06-01T00:00:00Z",
                "end_dtg":   "2025-06-02T00:00:00Z",
                "data_source_type": "file",
                "data_source": "/tmp/conn.csv",
                "analytic_parameters": bad_file.name,
            }
            with self.assertRaises(json.JSONDecodeError):
                validate_input(parse_input(raw))

        # =============================
        # 12) Plain-string analytic_parameters
        # =============================
        def test_analytic_parameters_plain_string(self):
            """
            If `analytic_parameters` is an arbitrary string that is **neither**
            a path nor valid JSON, it should pass unchanged (schema allows
            string via `oneOf`).
            """
            raw = {
                "input_schema_version": "1.0.0",
                "start_dtg": "2025-06-01T00:00:00Z",
                "end_dtg":   "2025-06-02T00:00:00Z",
                "data_source_type": "file",
                "data_source": "/tmp/conn.csv",
                "analytic_parameters": "just-a-flag",   # ← arbitrary string
            }
            canonical = validate_input(parse_input(raw))
            self.assertEqual(canonical["analytic_parameters"], "just-a-flag")

        # =============================
        # 13) Verbosity enum case-sensitivity
        # =============================
        def test_verbosity_case_sensitive(self):
            """
            The `verbosity` enum is uppercase in the contract.  Lower-case
            should fail validation.
            """
            invalid = {
                "input_schema_version": "1.0.0",
                "start_dtg": "2025-06-01T00:00:00Z",
                "end_dtg":   "2025-06-02T00:00:00Z",
                "data_source_type": "file",
                "data_source": "/tmp/conn.csv",
                "verbosity": "info",   # ← invalid casing
            }
            with self.assertRaises(SchemaError):
                validate_input(parse_input(invalid))

        # =============================
        # 14) OutputDoc without inputs
        # =============================
        def test_outputdoc_missing_inputs(self):
            """
            Calling `finalise()` without an `inputs` dict must raise
            `SchemaError`.
            """
            out = OutputDoc(input_data_hash="0" * 64)
            with self.assertRaises(SchemaError):
                out.finalise()

        # =============================
        # 15) Invalid log level in add_message
        # =============================
        def test_outputdoc_invalid_log_level(self):
            """`add_message()` with an unknown severity should raise `ValueError`."""
            inputs = validate_input(
                {
                    "input_schema_version": "1.0.0",
                    "start_dtg": "2025-06-01T00:00:00Z",
                    "end_dtg":   "2025-06-02T00:00:00Z",
                    "data_source_type": "file",
                    "data_source": "/tmp/conn.csv",
                }
            )
            out = OutputDoc(input_data_hash="0" * 64, inputs=inputs)
            with self.assertRaises(ValueError):
                out.add_message("TRACE", "should fail")   # not in enum

        # =============================
        # 16) Invalid findings structure
        # =============================
        def test_outputdoc_invalid_findings_structure(self):
            """
            Findings array items must match the nested object schema.  A
            malformed finding should trigger `SchemaError` during `finalise()`.
            """
            inputs = validate_input(
                {
                    "input_schema_version": "1.0.0",
                    "start_dtg": "2025-06-01T00:00:00Z",
                    "end_dtg":   "2025-06-02T00:00:00Z",
                    "data_source_type": "file",
                    "data_source": "/tmp/conn.csv",
                }
            )
            bad_finding = {"foo": "bar"}  # missing all required fields
            out = OutputDoc(input_data_hash="0"*64, inputs=inputs, findings=[bad_finding])
            with self.assertRaises(SchemaError):
                out.finalise()

        # =============================
        # 17) Non-string --config value
        # =============================
        def test_config_flag_non_string(self):
            """
            If the raw param dict contains a non-string `config` value,
            `validate_input` must raise `TypeError`.
            """
            with self.assertRaises(TypeError):
                validate_input({"config": 123})

        # =============================
        # 18) Unknown CLI argument
        # =============================
        def test_cli_unknown_argument(self):
            """An unrecognised CLI flag should raise `ValueError`."""
            with self.assertRaises(ValueError):
                parse_input("--bogus-flag true")

        # =============================
        # 19) data_source wrong Python type
        # =============================
        def test_data_source_invalid_python_type(self):
            """
            Supplying a non-string / non-DataFrame object for `data_source`
            must violate the `oneOf` and raise `SchemaError`.
            """
            invalid = {
                "input_schema_version": "1.0.0",
                "start_dtg": "2025-06-01T00:00:00Z",
                "end_dtg":   "2025-06-02T00:00:00Z",
                "data_source_type": "df",
                "data_source": 123,   # ← not string, not DataFrame
            }
            with self.assertRaises(SchemaError):
                validate_input(parse_input(invalid))

        # =============================
        # 20) data_map file dereference
        # =============================
        def test_data_map_file_deref(self):
            """
            `data_map` supplied as a JSON file path must be auto-loaded into a
            dict during validation.
            """
            tmp_file = self._tmp_json({"src_ip": "source_ip", "dst_ip": "dest_ip"})
            raw = {
                "input_schema_version": "1.0.0",
                "start_dtg": "2025-06-01T00:00:00Z",
                "end_dtg":   "2025-06-02T00:00:00Z",
                "data_source_type": "file",
                "data_source": "/tmp/conn.csv",
                "data_map": str(tmp_file),
            }
            canonical = validate_input(parse_input(raw))
            self.assertEqual(
                canonical["data_map"],
                {"src_ip": "source_ip", "dst_ip": "dest_ip"},
            )

        # =============================
        # 21) @response-file CLI syntax
        # =============================
        def test_cli_response_file(self):
            """
            The ArgumentParser supports GNU ‘@file’ response files.  Ensure
            `parse_input` correctly expands an @-prefixed filename where each
            *token* is written on a separate line.

            (Using one token per line avoids the edge-case where argparse
            treats “flag + value” as a single argument.)
            """
            tokens = [
                "--input-schema-version",
                "1.0.0",
                "--start-dtg",
                "2025-06-01T00:00:00Z",
                "--end-dtg",
                "2025-06-02T00:00:00Z",
                "--data-source-type",
                "file",
                "--data-source",
                "/tmp/conn.csv",
            ]
            tmp = tempfile.NamedTemporaryFile(
                delete=False, mode="w", encoding="utf-8"
            )
            tmp.write("\n".join(tokens))
            tmp.close()

            canonical = validate_input(parse_input(f"@{tmp.name}"))
            self.assertEqual(canonical["data_source_type"], "file")
            self.assertEqual(canonical["data_source"], "/tmp/conn.csv")

        # =============================
        # 22) Valid verbosity enum value
        # =============================
        def test_verbosity_valid_value(self):
            """Upper-case verbosity values present in the enum must validate."""
            raw = {
                "input_schema_version": "1.0.0",
                "start_dtg": "2025-06-01T00:00:00Z",
                "end_dtg":   "2025-06-02T00:00:00Z",
                "data_source_type": "file",
                "data_source": "/tmp/conn.csv",
                "verbosity": "DEBUG",
            }
            canonical = validate_input(parse_input(raw))
            self.assertEqual(canonical["verbosity"], "DEBUG")

        # =============================
        # 23) Message timestamp & level
        # =============================
        def test_outputdoc_message_iso_timestamp(self):
            """
            `add_message` must embed ISO-8601 timestamps and a level from the
            allowed enum.
            """
            inputs = validate_input(
                {
                    "input_schema_version": "1.0.0",
                    "start_dtg": "2025-06-01T00:00:00Z",
                    "end_dtg":   "2025-06-02T00:00:00Z",
                    "data_source_type": "file",
                    "data_source": "/tmp/conn.csv",
                }
            )
            out = OutputDoc(input_data_hash="0"*64, inputs=inputs)
            out.add_message("WARN", "something happened")
            ts = out["messages"][0]["timestamp"]
            self.assertRegex(ts, r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
            self.assertEqual(out["messages"][0]["level"], "WARN")

        # =============================
        # 24) Multiple messages append
        # =============================
        def test_outputdoc_multiple_messages(self):
            """Consecutive `add_message` calls should append to the list."""
            inputs = validate_input(
                {
                    "input_schema_version": "1.0.0",
                    "start_dtg": "2025-06-01T00:00:00Z",
                    "end_dtg":   "2025-06-02T00:00:00Z",
                    "data_source_type": "file",
                    "data_source": "/tmp/conn.csv",
                }
            )
            out = OutputDoc(input_data_hash="0"*64, inputs=inputs)
            out.add_message("INFO", "first")
            out.add_message("ERROR", "second")
            self.assertEqual([m["text"] for m in out["messages"]], ["first", "second"])

        # =============================
        # 25) save() before finalise() must fail
        # =============================
        def test_outputdoc_save_without_finalise(self):
            """
            Calling `save()` prior to `finalise()` must now raise
            `RuntimeError` to prevent incomplete documents from being written.
            """
            inputs = validate_input(
                {
                    "input_schema_version": "1.0.0",
                    "start_dtg": "2025-06-01T00:00:00Z",
                    "end_dtg":   "2025-06-02T00:00:00Z",
                    "data_source_type": "file",
                    "data_source": "/tmp/conn.csv",
                }
            )
            out = OutputDoc(input_data_hash="0"*64, inputs=inputs)
            tmp_path = Path(tempfile.NamedTemporaryFile(delete=False, suffix=".json").name)
            with self.assertRaises(RuntimeError):
                out.save(tmp_path)

        # =============================
        # 26) OutputDoc created with a dataframe as the data source
        # =============================
        
        def test_outputdoc_save_with_dataframe_input(self):
            """
            Ensure `save()` works correctly when the input contains a DataFrame,
            which is not natively JSON-serialisable.
            """
            df = pd.DataFrame({"col1": [1, 2]})
            inputs = validate_input({
                "input_schema_version": "1.0.0",
                "start_dtg": "2025-06-01T00:00:00Z",
                "end_dtg": "2025-06-02T00:00:00Z",
                "data_source_type": "df",
                "data_source": df,
            })
        
            out = OutputDoc(input_data_hash="a"*64, inputs=inputs)
            out.finalise()
        
            tmp_path = self._tmp_json({}) # create a temp file
        
            try:
                out.save(tmp_path) # This should NOT raise a TypeError
        
                # Optional: verify the content of the saved file
                saved_data = json.loads(tmp_path.read_text())
                self.assertIsInstance(saved_data["inputs"]["data_source"], dict)
                self.assertIn("__dataframe_sha256__", saved_data["inputs"]["data_source"])
        
            except TypeError as e:
                self.fail(f"OutputDoc.save() raised an unexpected TypeError: {e}")
            finally:
                tmp_path.unlink() # clean up

        # =============================
        # 27) Test the hash method
        # =============================
        def test_outputdoc_hash_method(self):
            """
            Dedicated test for OutputDoc._hash to verify determinism and
            sensitivity to changes, especially with embedded DataFrames.
            """
            # 1. --- Baseline Data ---
            # Create a base DataFrame and a parent object containing it.
            base_df = pd.DataFrame({'id': [1, 2], 'name': ['A', 'B']})
            base_obj = {
                "report_id": "rep-001",
                "source_df": base_df,
                "parameters": {"threshold": 42}
            }

            # 2. --- Test for Determinism ---
            # Hashing the exact same object twice must produce the same hash.
            hash1 = OutputDoc._hash(base_obj)
            hash2 = OutputDoc._hash(base_obj)
            self.assertEqual(hash1, hash2, "Hashing the same object should be deterministic.")

            # 3. --- Test Sensitivity to DataFrame CONTENT Change ---
            # A minor change to the DataFrame's data must result in a different hash.
            content_changed_df = base_df.copy()
            content_changed_df.loc[0, 'name'] = 'Z' # Change one value
            content_changed_obj = {
                "report_id": "rep-001",
                "source_df": content_changed_df,
                "parameters": {"threshold": 42}
            }
            hash3 = OutputDoc._hash(content_changed_obj)
            self.assertNotEqual(hash1, hash3, "Changing DataFrame content must alter the final hash.")

            # 4. --- Test Sensitivity to DataFrame STRUCTURE Change ---
            # A change to the DataFrame's structure (e.g., column name) must change the hash.
            structure_changed_df = base_df.copy()
            structure_changed_df.rename(columns={'id': 'record_id'}, inplace=True)
            structure_changed_obj = {
                "report_id": "rep-001",
                "source_df": structure_changed_df,
                "parameters": {"threshold": 42}
            }
            hash4 = OutputDoc._hash(structure_changed_obj)
            self.assertNotEqual(hash1, hash4, "Changing DataFrame structure must alter the final hash.")

            # 5. --- Test Sensitivity to NON-DataFrame Data Change ---
            # A change to other data in the object must also change the hash.
            other_data_changed_obj = {
                "report_id": "rep-001",
                "source_df": base_df,
                "parameters": {"threshold": 99} # Change a parameter
            }
            hash5 = OutputDoc._hash(other_data_changed_obj)
            self.assertNotEqual(hash1, hash5, "Changing non-DataFrame data must alter the final hash.")

    # --------------------------------------------------------------------- #
    # Run the suite
    # --------------------------------------------------------------------- #
    unittest.main(argv=[sys.argv[0]], verbosity=2)