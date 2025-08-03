"""
parser.py - generic command-line / JSON / mapping input loader
================================================================

This helper replaces the previous *dual* versions that were bound to
`analytic_schema` and `model_schema`.  It works with **any** schema that
follows the same JSON-Schema-lite pattern:

Public API
----------
`build_arg_parser(schema: Mapping) -> argparse.ArgumentParser`
    Construct an `argparse` instance with flags derived from *schema*.

`parse_input(source=None, *, schema) -> dict`
    Convert user-supplied *source* (CLI string / Path / JSON literal / Mapping)
    into a plain `dict` following the *schema* field names.

Both functions are schema-agnostic; callers must pass in the schema mapping -
you can obtain one via :pymod:`model_schema.loader.load_schema` or any other
means.
"""

from __future__ import annotations

import argparse
import json
import shlex
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

# --------------------------------------------------------------------------- #
# Parser builder                                                              #
# --------------------------------------------------------------------------- #

def build_arg_parser(schema: Mapping[str, Any]) -> argparse.ArgumentParser:
    """Return an :pyclass:`argparse.ArgumentParser` for *schema*.

    Parameters
    ----------
    schema : Mapping[str, Any]
        A parsed JSON contract containing at least keys ``description`` and
        ``fields``.  The function inspects every top-level field and exposes a
        ``--<field-name>`` CLI flag with the appropriate ``type`` / ``choices``.
    """

    p = argparse.ArgumentParser(
        description=schema.get("description", ""),
        fromfile_prefix_chars="@",
        add_help=False,
    )

    # standard meta flags ----------------------------------------------------
    p.add_argument("-h", "--help", action="help", help="Show this help message and exit.")

    if "version" in schema:
        p.add_argument(
            "--version",
            action="version",
            version=f"{schema.get('title', 'schema')} : {schema['version']}",
            help="Print schema version and exit.",
        )

    p.add_argument(
        "--config",
        metavar="FILE",
        help="JSON file containing full input object; overrides all other flags.",
    )

    for name, spec in schema.get("fields", {}).items():
        flag   = f"--{name.replace('_', '-')}"
        types  = spec.get("type", "string")
        types  = types if isinstance(types, list) else [types]
        kwargs: dict[str, Any] = {
            "dest": name,
            "help": spec.get("description", "")
        }

        if "boolean" in types:
            kwargs["action"]  = "store_true"
            kwargs["default"] = argparse.SUPPRESS
        else:
            if "integer" in types:
                kwargs["type"] = int
            elif "number" in types:
                kwargs["type"] = float
            else:
                kwargs["type"] = str
            kwargs["default"] = argparse.SUPPRESS

        if "enum" in spec:
            kwargs["choices"] = spec["enum"]

        p.add_argument(flag, **kwargs)

    return p

# --------------------------------------------------------------------------- #
# Input parsing utility                                                       #
# --------------------------------------------------------------------------- #

def parse_input(
    source: None | str | Path | Sequence[str] | Mapping[str, Any] = None,
    *,
    schema: Mapping[str, Any],
) -> dict[str, Any]:
    """Convert *source* to a *raw* ``dict`` (no validation).

    Parameters
    ----------
    source
        Supported variants:
        * ``Mapping`` - copied directly.
        * ``Path`` - JSON file on disk.
        * ``str``  - interpreted as: existing file path → load; else JSON literal → load; else CLI string.
        * ``Sequence[str]`` - treated as CLI tokens.
        * ``None`` - default to ``sys.argv[1:]``.
    schema
        The schema to drive CLI flag generation when *source* is CLI-style.

    Returns
    -------
    dict
        Raw key-value mapping with only the options provided by the user.  If
        ``--config`` is used the returned dict is exactly that file’s content.
    """

    # Mapping - already dict-like ------------------------------------------
    if isinstance(source, Mapping):
        return dict(source)

    # Path - read JSON file -------------------------------------------------
    if isinstance(source, Path):
        return json.loads(source.read_text(encoding="utf-8"))

    # Decide how to treat *source* -----------------------------------------
    argv: list[str]
    if isinstance(source, str):
        p = Path(source)
        if p.is_file():
            return json.loads(p.read_text(encoding="utf-8"))
        try:
            return json.loads(source)
        except json.JSONDecodeError:
            argv = shlex.split(source)
    elif source is None:
        argv = sys.argv[1:]
    elif isinstance(source, Sequence) and not isinstance(source, (str, bytes)):
        argv = list(source)
    else:
        raise TypeError(f"Unsupported type for parse_input: {type(source)}")

    # CLI style - use argparse ---------------------------------------------
    parser = build_arg_parser(schema)
    namespace, unknown = parser.parse_known_args(argv)
    if unknown:
        raise ValueError(f"Unknown argument(s): {unknown}. Use --help.")
    ns_dict      = vars(namespace)

    # --config overrides everything else -----------------------------------
    if config_file := ns_dict.pop("config", None):
        cfg_path = Path(config_file)
        if not cfg_path.is_file():
            raise FileNotFoundError(cfg_path)
        return json.loads(cfg_path.read_text(encoding="utf-8"))

    return ns_dict
