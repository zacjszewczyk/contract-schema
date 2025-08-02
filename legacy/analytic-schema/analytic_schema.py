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

    def save(self, path: Union[str, pathlib.Path], *, indent: int = 2, quiet = False) -> None:
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
            if not quiet:
                display_output(f"Output document saved to: {path_obj.resolve()}", file=sys.stderr)
        except Exception as exc:
            if not quiet:
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
    Execute a comprehensive integration-style test-suite when the module is
    run directly (e.g. ``python analytic_schema.py``).  The suite exercises
    every public entry-point (`parse_input`, `validate_input`, `OutputDoc`) and
    verifies that defaults, validation errors, and helper behaviours all work
    exactly as documented.
    """
    import unittest
    import sys
    import json
    import tempfile
    from pathlib import Path
    import pandas as pd

    # --------------------------------------------------------------------- #
    # Helper utilities used by multiple test-cases                          #
    # --------------------------------------------------------------------- #
    class _Util:
        """Shared helpers for the test-suite."""

        @staticmethod
        def tmp_json(obj) -> Path:
            """Write *obj* to a temporary ``*.json`` file and return the path."""
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
            Path(tmp.name).write_text(json.dumps(obj), encoding="utf-8")
            return Path(tmp.name)

        @staticmethod
        def with_defaults(base: Dict[str, Any]) -> Dict[str, Any]:
            """
            Return *base* **merged** with module-level `_DEFAULTS`, mimicking
            what `validate_input` produces after default injection.
            """
            merged = base.copy()
            for k, v in _DEFAULTS.items():
                merged.setdefault(k, copy.deepcopy(v))
            return merged

    # --------------------------------------------------------------------- #
    # Test-cases                                                            #
    # --------------------------------------------------------------------- #
    class AnalyticSchemaTests(unittest.TestCase):
        """
        End-to-end tests for *analytic_schema.py* covering:

        01. CLI parsing            → `parse_input`
        02. Dict / JSON handling   → `parse_input`
        03. Validation engine      → `validate_input`
        04. Default injection      → `validate_input`
        05. Output document flow   → `OutputDoc`
        """

        # ────────────────────────────────────────────────────────────────────
        # 01  CLI round-trip
        # ────────────────────────────────────────────────────────────────────
        def test_01_cli_roundtrip_defaults(self):
            """CLI → parse → validate should round-trip and inject defaults."""
            cli = (
                "--input-schema-version 1.0.0 "
                "--start-dtg 2025-06-01T00:00:00Z "
                "--end-dtg 2025-06-02T00:00:00Z "
                "--data-source-type file "
                "--data-source /tmp/conn.csv"
            )
            raw = parse_input(cli)
            canonical = validate_input(raw)
            self.assertDictEqual(canonical, _Util.with_defaults(raw))

        # ────────────────────────────────────────────────────────────────────
        # 02  Dict source with inline JSON dereference
        # ────────────────────────────────────────────────────────────────────
        def test_02_dict_with_analytic_parameters_dereferenced(self):
            """Stringified JSON in `analytic_parameters` must be parsed → dict."""
            raw = {
                "input_schema_version": "1.0.0",
                "start_dtg": "2025-06-01T00:00:00Z",
                "end_dtg":   "2025-06-02T00:00:00Z",
                "data_source_type": "file",
                "data_source": "/tmp/conn.csv",
                "analytic_parameters": '{"param_a": 123}',
            }
            out = validate_input(parse_input(raw))
            self.assertEqual(out["analytic_parameters"], {"param_a": 123})

        # ────────────────────────────────────────────────────────────────────
        # 03  DataFrame as data_source
        # ────────────────────────────────────────────────────────────────────
        def test_03_dataframe_data_source_passes_validation(self):
            """A Pandas DataFrame is valid when `data_source_type=='df'`."""
            df = pd.DataFrame({"Name": ["Alice", "Bob"]})
            raw = {
                "input_schema_version": "1.0.0",
                "start_dtg": "2025-06-01T00:00:00Z",
                "end_dtg":   "2025-06-02T00:00:00Z",
                "data_source_type": "df",
                "data_source": df,
            }
            out = validate_input(parse_input(raw))
            self.assertTrue(out["data_source"].equals(df))

        # ────────────────────────────────────────────────────────────────────
        # 04  --config precedence
        # ────────────────────────────────────────────────────────────────────
        def test_04_config_file_overrides_cli(self):
            """`--config` JSON file should replace all other CLI flags."""
            cfg = {
                "input_schema_version": "1.0.0",
                "start_dtg": "2025-07-01T00:00:00Z",
                "end_dtg":   "2025-07-02T00:00:00Z",
                "data_source_type": "api endpoint",
                "data_source": "https://api.example.com/data",
            }
            cfg_path = _Util.tmp_json(cfg)
            out = validate_input(parse_input(["--config", str(cfg_path)]))
            self.assertDictEqual(out, _Util.with_defaults(cfg))

        # ────────────────────────────────────────────────────────────────────
        # 05  OutputDoc lifecycle & default population
        # ────────────────────────────────────────────────────────────────────
        def test_05_outputdoc_defaults_and_hashes(self):
            """`OutputDoc.finalise` must fill meta, hashes, and default fields."""
            inputs = validate_input(
                {
                    "input_schema_version": "1.0.0",
                    "start_dtg": "2025-06-01T00:00:00Z",
                    "end_dtg":   "2025-06-02T00:00:00Z",
                    "data_source_type": "file",
                    "data_source": "/tmp/conn.csv",
                }
            )
            doc = OutputDoc(input_data_hash="0"*64, inputs=inputs)
            doc.add_message("INFO", "hello")
            doc.finalise()

            self.assertIn("input_hash",     doc)
            self.assertIn("findings_hash",  doc)
            self.assertEqual(doc["status"], "UNKNOWN")
            self.assertEqual(doc["messages"][0]["level"], "INFO")

        # ────────────────────────────────────────────────────────────────────
        # 06  Missing required field
        # ────────────────────────────────────────────────────────────────────
        def test_06_validate_missing_required_field_raises(self):
            """Omitting `data_source` must raise `SchemaError`."""
            invalid = {
                "input_schema_version": "1.0.0",
                "start_dtg": "2025-06-01T00:00:00Z",
                "end_dtg":   "2025-06-02T00:00:00Z",
                "data_source_type": "file",
            }
            with self.assertRaises(SchemaError):
                validate_input(parse_input(invalid))

        # ────────────────────────────────────────────────────────────────────
        # 07  Enum value outside allowed set
        # ────────────────────────────────────────────────────────────────────
        def test_07_invalid_enum_value_detected(self):
            """`data_source_type='ftp'` violates the enum constraint."""
            bad = {
                "input_schema_version": "1.0.0",
                "start_dtg": "2025-06-01T00:00:00Z",
                "end_dtg":   "2025-06-02T00:00:00Z",
                "data_source_type": "ftp",
                "data_source": "/tmp/x",
            }
            with self.assertRaises(SchemaError):
                validate_input(parse_input(bad))

        # ────────────────────────────────────────────────────────────────────
        # 08  Invalid ISO-8601 timestamp
        # ────────────────────────────────────────────────────────────────────
        def test_08_invalid_datetime_format_rejected(self):
            """Date-only string must fail the `format:"date-time"` check."""
            bad = {
                "input_schema_version": "1.0.0",
                "start_dtg": "2025-06-01",
                "end_dtg":   "2025-06-02T00:00:00Z",
                "data_source_type": "file",
                "data_source": "/tmp/x",
            }
            with self.assertRaises(SchemaError):
                validate_input(parse_input(bad))

        # ────────────────────────────────────────────────────────────────────
        # 09  File-path dereference for analytic_parameters
        # ────────────────────────────────────────────────────────────────────
        def test_09_analytic_parameters_external_file_loaded(self):
            """Passing a JSON file path in `analytic_parameters` must load → dict."""
            tmp = _Util.tmp_json({"p": 1})
            raw = {
                "input_schema_version": "1.0.0",
                "start_dtg": "2025-06-01T00:00:00Z",
                "end_dtg":   "2025-06-02T00:00:00Z",
                "data_source_type": "file",
                "data_source": "/tmp/x",
                "analytic_parameters": str(tmp),
            }
            out = validate_input(parse_input(raw))
            self.assertEqual(out["analytic_parameters"], {"p": 1})

        # ────────────────────────────────────────────────────────────────────
        # 10  additionalProperties=false enforcement
        # ────────────────────────────────────────────────────────────────────
        def test_10_unknown_top_level_field_rejected(self):
            """Extraneous field should trigger `SchemaError`."""
            bad = {
                "input_schema_version": "1.0.0",
                "start_dtg": "2025-06-01T00:00:00Z",
                "end_dtg":   "2025-06-02T00:00:00Z",
                "data_source_type": "file",
                "data_source": "/tmp/x",
                "oops": True,
            }
            with self.assertRaises(SchemaError):
                validate_input(parse_input(bad))

        # ────────────────────────────────────────────────────────────────────
        # 11  Invalid JSON in external file
        # ────────────────────────────────────────────────────────────────────
        def test_11_invalid_json_file_raises_decode_error(self):
            """Non-JSON content in file path must raise `json.JSONDecodeError`."""
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
            Path(tmp.name).write_text("NOT_JSON", encoding="utf-8")
            bad = {
                "input_schema_version": "1.0.0",
                "start_dtg": "2025-06-01T00:00:00Z",
                "end_dtg":   "2025-06-02T00:00:00Z",
                "data_source_type": "file",
                "data_source": "/tmp/x",
                "analytic_parameters": tmp.name,
            }
            with self.assertRaises(json.JSONDecodeError):
                validate_input(parse_input(bad))

        # ────────────────────────────────────────────────────────────────────
        # 12  Plain-string analytic_parameters passes
        # ────────────────────────────────────────────────────────────────────
        def test_12_plain_string_analytic_parameters_valid(self):
            """Arbitrary string is allowed by `oneOf` branch."""
            raw = {
                "input_schema_version": "1.0.0",
                "start_dtg": "2025-06-01T00:00:00Z",
                "end_dtg":   "2025-06-02T00:00:00Z",
                "data_source_type": "file",
                "data_source": "/tmp/x",
                "analytic_parameters": "flag",
            }
            out = validate_input(parse_input(raw))
            self.assertEqual(out["analytic_parameters"], "flag")

        # ────────────────────────────────────────────────────────────────────
        # 13  Verbosity enum case sensitivity
        # ────────────────────────────────────────────────────────────────────
        def test_13_lowercase_enum_value_rejected(self):
            """Enum field `verbosity` is case-sensitive."""
            bad = {
                "input_schema_version": "1.0.0",
                "start_dtg": "2025-06-01T00:00:00Z",
                "end_dtg":   "2025-06-02T00:00:00Z",
                "data_source_type": "file",
                "data_source": "/tmp/x",
                "verbosity": "info",
            }
            with self.assertRaises(SchemaError):
                validate_input(parse_input(bad))

        # ────────────────────────────────────────────────────────────────────
        # 14  OutputDoc without inputs
        # ────────────────────────────────────────────────────────────────────
        def test_14_outputdoc_finalise_without_inputs_raises(self):
            """`finalise()` must fail if `inputs` missing."""
            with self.assertRaises(SchemaError):
                OutputDoc(input_data_hash="0"*64).finalise()

        # ────────────────────────────────────────────────────────────────────
        # 15  add_message invalid log level
        # ────────────────────────────────────────────────────────────────────
        def test_15_invalid_log_level_raises(self):
            """Unknown severity string should raise `ValueError`."""
            inputs = validate_input(
                {
                    "input_schema_version": "1.0.0",
                    "start_dtg": "2025-06-01T00:00:00Z",
                    "end_dtg":   "2025-06-02T00:00:00Z",
                    "data_source_type": "file",
                    "data_source": "/tmp/x",
                }
            )
            doc = OutputDoc(input_data_hash="0"*64, inputs=inputs)
            with self.assertRaises(ValueError):
                doc.add_message("TRACE", "msg")

        # ────────────────────────────────────────────────────────────────────
        # 16  Invalid findings structure
        # ────────────────────────────────────────────────────────────────────
        def test_16_malformed_finding_triggers_schema_error(self):
            """Bad finding dict must be caught by `finalise()` validation."""
            inputs = validate_input(
                {
                    "input_schema_version": "1.0.0",
                    "start_dtg": "2025-06-01T00:00:00Z",
                    "end_dtg":   "2025-06-02T00:00:00Z",
                    "data_source_type": "file",
                    "data_source": "/tmp/x",
                }
            )
            doc = OutputDoc(input_data_hash="0"*64,
                            inputs=inputs,
                            findings=[{"foo": "bar"}])
            with self.assertRaises(SchemaError):
                doc.finalise()

        # ────────────────────────────────────────────────────────────────────
        # 17  Non-string --config flag
        # ────────────────────────────────────────────────────────────────────
        def test_17_non_string_config_flag_type_error(self):
            """Non-string `config` value should raise `TypeError`."""
            with self.assertRaises(TypeError):
                validate_input({"config": 123})

        # ────────────────────────────────────────────────────────────────────
        # 18  Unknown CLI argument
        # ────────────────────────────────────────────────────────────────────
        def test_18_unknown_cli_flag_detected(self):
            """Unrecognised CLI flag should raise `ValueError`."""
            with self.assertRaises(ValueError):
                parse_input("--bad-flag 1")

        # ────────────────────────────────────────────────────────────────────
        # 19  Wrong Python type for data_source
        # ────────────────────────────────────────────────────────────────────
        def test_19_data_source_wrong_type_schema_error(self):
            """`data_source` must be string or DataFrame as per `oneOf`."""
            bad = {
                "input_schema_version": "1.0.0",
                "start_dtg": "2025-06-01T00:00:00Z",
                "end_dtg":   "2025-06-02T00:00:00Z",
                "data_source_type": "df",
                "data_source": 999,
            }
            with self.assertRaises(SchemaError):
                validate_input(parse_input(bad))

        # ────────────────────────────────────────────────────────────────────
        # 20  data_map file dereference
        # ────────────────────────────────────────────────────────────────────
        def test_20_data_map_external_file_loaded(self):
            """External JSON file in `data_map` should be auto-loaded."""
            tmp = _Util.tmp_json({"a": 1})
            raw = {
                "input_schema_version": "1.0.0",
                "start_dtg": "2025-06-01T00:00:00Z",
                "end_dtg":   "2025-06-02T00:00:00Z",
                "data_source_type": "file",
                "data_source": "/tmp/x",
                "data_map": str(tmp),
            }
            out = validate_input(parse_input(raw))
            self.assertEqual(out["data_map"], {"a": 1})

        # ────────────────────────────────────────────────────────────────────
        # 21  @response-file syntax
        # ────────────────────────────────────────────────────────────────────
        def test_21_at_response_file_expansion(self):
            """Argparse should expand @file with one token per line."""
            tokens = [
                "--input-schema-version", "1.0.0",
                "--start-dtg", "2025-06-01T00:00:00Z",
                "--end-dtg",   "2025-06-02T00:00:00Z",
                "--data-source-type", "file",
                "--data-source", "/tmp/x",
            ]
            tmp = tempfile.NamedTemporaryFile(delete=False, mode="w", encoding="utf-8")
            tmp.write("\n".join(tokens)); tmp.close()
            out = validate_input(parse_input(f"@{tmp.name}"))
            self.assertEqual(out["data_source_type"], "file")

        # ────────────────────────────────────────────────────────────────────
        # 22  Valid verbosity passes
        # ────────────────────────────────────────────────────────────────────
        def test_22_valid_enum_value_accepts(self):
            """Upper-case `verbosity` value must validate."""
            raw = {
                "input_schema_version": "1.0.0",
                "start_dtg": "2025-06-01T00:00:00Z",
                "end_dtg":   "2025-06-02T00:00:00Z",
                "data_source_type": "file",
                "data_source": "/tmp/x",
                "verbosity": "DEBUG",
            }
            self.assertEqual(validate_input(parse_input(raw))["verbosity"], "DEBUG")

        # ────────────────────────────────────────────────────────────────────
        # 23  Message timestamp & level format
        # ────────────────────────────────────────────────────────────────────
        def test_23_outputdoc_message_fields_format(self):
            """Timestamp must be ISO-8601 and level from enum."""
            inputs = validate_input(
                {
                    "input_schema_version": "1.0.0",
                    "start_dtg": "2025-06-01T00:00:00Z",
                    "end_dtg":   "2025-06-02T00:00:00Z",
                    "data_source_type": "file",
                    "data_source": "/tmp/x",
                }
            )
            doc = OutputDoc(input_data_hash="0"*64, inputs=inputs)
            doc.add_message("WARN", "msg")
            msg = doc["messages"][0]
            self.assertRegex(msg["timestamp"], r"^\d{4}-\d{2}-\d{2}T")
            self.assertEqual(msg["level"], "WARN")

        # ────────────────────────────────────────────────────────────────────
        # 24  Multiple add_message calls append
        # ────────────────────────────────────────────────────────────────────
        def test_24_outputdoc_multiple_messages_append(self):
            """Two consecutive `add_message` calls should produce two entries."""
            inputs = validate_input(
                {
                    "input_schema_version": "1.0.0",
                    "start_dtg": "2025-06-01T00:00:00Z",
                    "end_dtg":   "2025-06-02T00:00:00Z",
                    "data_source_type": "file",
                    "data_source": "/tmp/x",
                }
            )
            doc = OutputDoc(input_data_hash="0"*64, inputs=inputs)
            doc.add_message("INFO", "a"); doc.add_message("ERROR", "b")
            self.assertEqual([m["text"] for m in doc["messages"]], ["a", "b"])

        # ────────────────────────────────────────────────────────────────────
        # 25  save() before finalise()
        # ────────────────────────────────────────────────────────────────────
        def test_25_save_before_finalise_runtime_error(self):
            """Calling `save()` without `finalise()` must raise `RuntimeError`."""
            inputs = validate_input(
                {
                    "input_schema_version": "1.0.0",
                    "start_dtg": "2025-06-01T00:00:00Z",
                    "end_dtg":   "2025-06-02T00:00:00Z",
                    "data_source_type": "file",
                    "data_source": "/tmp/x",
                }
            )
            doc = OutputDoc(input_data_hash="0"*64, inputs=inputs)
            with self.assertRaises(RuntimeError):
                doc.save(Path(tempfile.NamedTemporaryFile().name))

        # ────────────────────────────────────────────────────────────────────
        # 26  save() with DataFrame in inputs
        # ────────────────────────────────────────────────────────────────────
        def test_26_save_with_dataframe_succeeds(self):
            """`save()` must succeed when `inputs` contains a DataFrame."""
            df = pd.DataFrame({"col": [1, 2]})
            inputs = validate_input({
                "input_schema_version": "1.0.0",
                "start_dtg": "2025-06-01T00:00:00Z",
                "end_dtg": "2025-06-02T00:00:00Z",
                "data_source_type": "df",
                "data_source": df,
            })
            doc = OutputDoc(input_data_hash="a"*64, inputs=inputs); doc.finalise()
            tmp = _Util.tmp_json({})
            doc.save(tmp)
            saved = json.loads(tmp.read_text())
            self.assertIn("__dataframe_sha256__", saved["inputs"]["data_source"])
            tmp.unlink()

        # ────────────────────────────────────────────────────────────────────
        # 27  _hash behaviour with DataFrames
        # ────────────────────────────────────────────────────────────────────
        def test_27_hash_determinism_and_sensitivity(self):
            """`_hash` must be deterministic and sensitive to data changes."""
            df = pd.DataFrame({"v": [1, 2]})
            base = {"df": df, "p": 1}
            h1 = OutputDoc._hash(base); h2 = OutputDoc._hash(base)
            self.assertEqual(h1, h2, "Hash must be deterministic.")

            df2 = df.copy(); df2.loc[0, "v"] = 99
            self.assertNotEqual(h1, OutputDoc._hash({"df": df2, "p": 1}),
                                "Changing DF content must change hash.")
            self.assertNotEqual(h1, OutputDoc._hash({"df": df, "p": 2}),
                                "Changing non-DF field must change hash.")

    # --------------------------------------------------------------------- #
    # Run the suite with named output                                       #
    # --------------------------------------------------------------------- #
    unittest.main(argv=[sys.argv[0]], verbosity=2)