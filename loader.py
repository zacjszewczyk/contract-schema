"""
loader.py – single source-of-truth for the packaged JSON contract.

Public API
----------
load_schema()    : function       – helper to obtain a fresh copy
"""

from __future__ import annotations

import copy
import json
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
        raise FileNotFoundError(f"Contract not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {path}: {exc}") from exc


# --------------------------------------------------------------------------- #
# Public utilities                                                            #
# --------------------------------------------------------------------------- #

def load_schema(path: str | Path) -> dict:
    """
    Return a **deep copy** of the requested schema.

    Parameters
    ----------
    path : Path | str
        Path to the schema file to read.

    Returns
    -------
    dict
        Parsed JSON schema (deep-copied) so callers cannot mutate globals.
    """
    resolved_path = Path(path).expanduser().resolve()
    return copy.deepcopy(_read(resolved_path))