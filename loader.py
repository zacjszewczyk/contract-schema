"""
loader.py – single source-of-truth for the packaged JSON contract.

Public API
----------
SCHEMA_PATH      : pathlib.Path   – absolute path to the active schema file
SCHEMA_VERSION   : str            – semantic version inside the JSON
OUTPUT_SCHEMA    : dict           – deep copy of the 'output' definition
load_schema()    : function       – helper to obtain a fresh copy
reload()         : function       – hot-reload constants (mainly for tests)
"""

from __future__ import annotations

import copy
import json
import os
from pathlib import Path
from typing import Any, Mapping

# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #

def _read(path: Path) -> Mapping[str, Any]:
    """Read & parse a JSON schema, raising crisp errors on failure."""
    try:
        with path.open(encoding="utf-8") as fd:
            return json.load(fd)
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"Model-schema contract not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {path}: {exc}") from exc


def _resolve_default_path() -> Path:
    """Return the default schema path shipped with the package."""
    return Path(__file__).with_name("model_schema.json")


# --------------------------------------------------------------------------- #
# Initial load                                                                #
# --------------------------------------------------------------------------- #

# Allow overrides via env var – handy in CI or experimentation.
_SCHEMAPATH_ENV = os.getenv("MODEL_SCHEMA_PATH")
SCHEMA_PATH: Path = Path(_SCHEMAPATH_ENV).expanduser().resolve() if _SCHEMAPATH_ENV else _resolve_default_path()

# Internal cache of the parsed schema JSON.
_SCHEMA_RAW: Mapping[str, Any] = _read(SCHEMA_PATH)

SCHEMA_VERSION: str = _SCHEMA_RAW.get("version", "UNKNOWN")

# Provide a deep copy so callers can mutate safely.
OUTPUT_SCHEMA: dict = copy.deepcopy(_SCHEMA_RAW.get("output", {}))


# --------------------------------------------------------------------------- #
# Public utilities                                                            #
# --------------------------------------------------------------------------- #

def load_schema(path: str | Path | None = None) -> dict:
    """
    Return a **deep copy** of the requested schema (defaults to the active one).

    Parameters
    ----------
    path : Path | str | None
        If provided, read and return that file instead of the packaged default.

    Returns
    -------
    dict
        Parsed JSON schema (deep-copied) so callers cannot mutate globals.
    """
    if path is None:
        return copy.deepcopy(_SCHEMA_RAW)
    path = Path(path).expanduser().resolve()
    return copy.deepcopy(_read(path))


def reload() -> None:
    """
    Re-parse the schema file and refresh module-level constants in place.

    Mainly useful for unit tests that write a temporary contract to disk and
    want the loader to reflect it without spawning a fresh interpreter.
    """
    global _SCHEMA_RAW, SCHEMA_VERSION, OUTPUT_SCHEMA  # noqa: PLW0603

    _SCHEMA_RAW = _read(SCHEMA_PATH)
    SCHEMA_VERSION = _SCHEMA_RAW.get("version", "UNKNOWN")
    OUTPUT_SCHEMA = copy.deepcopy(_SCHEMA_RAW.get("output", {}))


# --------------------------------------------------------------------------- #
# Dunder helpers                                                              #
# --------------------------------------------------------------------------- #

def __repr__() -> str:  # pragma: no cover
    return f"<loader SCHEMA_VERSION={SCHEMA_VERSION!s} path={str(SCHEMA_PATH)!r}>"
