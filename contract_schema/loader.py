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
from importlib import resources

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
    p = Path(path)
    if p.is_file():
        with p.open(encoding="utf-8") as fd:
            return copy.deepcopy(json.load(fd))

    # fallback: treat `path` as a resource inside contract_schema.schemas
    try:
        text = resources.files("contract_schema.schemas").joinpath(path).read_text(encoding="utf-8")
        return copy.deepcopy(json.loads(text))
    except FileNotFoundError:
        raise FileNotFoundError(f"Schema '{path}' not found on disk or in package data")
