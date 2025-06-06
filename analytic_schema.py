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
     convenience parameters).
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
# 1.  Load the contract – INPUT_SCHEMA / OUTPUT_SCHEMA
# =============================================================================
# NOTE: The JSON file is considered the *single source of truth*.  This Python
# file merely *loads* the contract at run-time so any change to
# analytic_schema.json is automatically picked up without touching code.
SCHEMA_PATH = pathlib.Path("./analytic_schema.json")

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
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
    r"(?:\.\d{1,6})?"          # Optional .microseconds
    r"(?:Z|[+-]\d{2}:\d{2})$"  # Z or ±HH:MM
)

def _is_datetime(s: Any) -> bool:
    """
    Return ``True`` if *s* is a **string** that satisfies ISO-8601 (as accepted
    by :py:meth:`datetime.datetime.fromisoformat`).

    The helper is split out so the main validator remains readable.
    """
    if not isinstance(s, str) or not _DT_RE.match(s):
        return False
    try:
        # ``fromisoformat`` supports ‘Z’ and ±HH:MM offsets once the pattern
        # above guarantees they are the only timezone indicators.
        _dt.datetime.fromisoformat(s)
        return True
    except ValueError:
        return False

# Map JSON-Schema “type” parameter → Python class(es)
_TYPE_DISPATCH: Dict[str, Union[type, Tuple[type, ...]]] = {
    "string"  : str,
    "integer" : int,
    "number"  : (int, float),
    "object"  : dict,
    "array"   : list,
    "boolean" : bool,
    "dataframe": pd.DataFrame, # Note that “pd.DataFrame” is *not* a 
    # JSON-Schema type; it’s inclusion is a convenience to allow for the inline
    # use of Pandas DataFrames as a source if analytic_schema.py is used in a
    # Jupyter notebook, for example.
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
    """
    Canonicalise *raw_params* and validate against ``INPUT_SCHEMA``.

    Steps performed
    ---------------
    1. If ``--config`` present → load that file and *replace* ``raw_params``.  
    2. Any *object-or-path* convenience fields (``analytic_parameters``,
       ``data_map``) are automatically dereferenced if they contain a valid
       JSON file path or an inline JSON string.
    3. The resulting dictionary is run through the internal validator –
       failures raise :class:`SchemaError`.

    Returns
    -------
    dict
        The *canonical* input dictionary suitable for use by notebooks.

    Raises
    ------
    FileNotFoundError | json.JSONDecodeError | ValueError | TypeError
        For I/O or typing issues before schema validation.
    SchemaError
        If the final dictionary violates the contract.
    """
    # Defensive: ensure param is dict-like
    if not isinstance(raw_params, dict):
        raise TypeError(
            f"validate_input expects dict, got {type(raw_params).__name__}"
        )

    # --------------------------------------------------------------------- #
    # 1) Handle --config override
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
            print(f"Info: Loading parameters from --config={config_path}", file=sys.stderr)
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
    # 2) Dereference “object-or-path” convenience fields
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
    # 3) Final schema validation
    # --------------------------------------------------------------------- #
    try:
        _validate(data, INPUT_SCHEMA)
    except SchemaError as exc:
        # Add context if we came from a config file
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
    # Internal hashing helper
    # --------------------------------------------------------------------- #
    @staticmethod
    def _hash(obj: Any) -> str:
        """Return *stable* SHA-256 hex digest of *obj* (JSON-serialised)."""
        return hashlib.sha256(
            json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

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

    # --------------------------------------------------------------------- #
    def finalise(self) -> None:
        """
        Freeze the document – fills meta-data, hashes sections, **validates**.

        Users *must* call this before :py:meth:`save` or shipping the JSON to a
        downstream system.

        Raises
        ------
        SchemaError
            If required fields are missing or have wrong types.
        """
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

        # --- Input / findings processing ------------------------------------------
        if "inputs" not in self:
            raise SchemaError("OutputDoc.finalise(): missing 'inputs' dictionary.")
        if not isinstance(self["inputs"], dict):
            raise SchemaError("'inputs' must be a dict.")

        self["input_hash"] = self._hash(self["inputs"])

        self.setdefault("findings", [])
        if not isinstance(self["findings"], list):
            raise SchemaError("'findings' must be a list.")
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

    # --------------------------------------------------------------------- #
    def save(self, path: Union[str, pathlib.Path], *, indent: int = 2) -> None:
        """
        Write the validated document to *path* as pretty-printed JSON.

        Parameters
        ----------
        path
            File location (string or :class:`pathlib.Path`).
        indent
            Number of spaces for JSON indentation (default: 2).

        Notes
        -----
        * If :py:meth:`finalise` has **not** been called, the method will emit a
          warning and attempt to write the current dictionary anyway.  The JSON
          may fail downstream validation!
        """
        if not self.get("run_id"):
            print(
                "Warning: OutputDoc.save() called before finalise(). "
                "Document may be incomplete.",
                file=sys.stderr,
            )

        path_obj = pathlib.Path(path)
        try:
            path_obj.write_text(
                json.dumps(self, indent=indent, ensure_ascii=False), encoding="utf-8"
            )
            print(f"Output document saved to: {path_obj.resolve()}", file=sys.stderr)
        except Exception as exc:
            print(f"Error saving output document to {path_obj}: {exc}", file=sys.stderr)
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
# 6.  Command-line entry-point
# =============================================================================
def _main(cli_args: List[str]) -> None:  # pragma: no cover
    """
    Internal CLI handler (invoked when the module is executed with
    ``python -m analytic_schema …``).

    The function will:

    1. Build the dynamic argument parser.
    2. Parse *cli_args*.
    3. Validate the resulting dictionary.
    4. Print canonicalised JSON to ``stdout`` if successful; otherwise emit
       errors to ``stderr`` and exit with a non-zero code.
    """
    parser = _build_arg_parser()

    # Early exit for explicit “–help”
    if not cli_args or any(flag in cli_args for flag in ("-h", "--help")):
        parser.print_help(sys.stdout)
        sys.exit(0)

    print("Attempting to parse and validate input…", file=sys.stderr)
    try:
        raw = parse_input(cli_args)
        print(f"Raw parsed params: {raw}", file=sys.stderr)

        canonical = validate_input(raw)

        print("\n✓ Input document is valid and conforms to the schema.")
        print("\nCanonicalised Input:")
        print(json.dumps(canonical, indent=2, sort_keys=True))
        sys.exit(0)

    except (SchemaError, FileNotFoundError, json.JSONDecodeError,
            ValueError, TypeError) as exc:
        print(f"\n✗ Validation Error: {exc}", file=sys.stderr)
        sys.exit(1)
    except SystemExit as exc:
        # Propagate exit codes from argparse
        sys.exit(exc.code if exc.code is not None else 1)
    except Exception as exc:
        import traceback

        print(f"\n✗ Unexpected error: {exc}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(2)

# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    _main(sys.argv[1:])
