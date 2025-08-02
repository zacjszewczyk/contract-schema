"""
utils.py – shared, low-level helpers for the contract-schema package.

This module contains common, dependency-free utilities for:
- Hashing files and Python objects.
- Handling timestamps and date-time validation.
- Capturing execution environment details (hardware, library versions).
"""

from __future__ import annotations

import datetime as _dt
import getpass
import hashlib
import importlib.metadata as _im
import json
import os
import platform
import re
import socket
from pathlib import Path
from typing import Any, Dict

__all__ = [
    "is_datetime",
    "sha256_file",
    "hash_object",
    "now_iso",
    "get_library_versions",
    "get_hardware_specs",
]

# --------------------------------------------------------------------------- #
# Date-Time Helpers                                                           #
# --------------------------------------------------------------------------- #

_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+\-]\d{2}:\d{2})$")

def is_datetime(value: Any) -> bool:
    """Return True iff *value* is a valid ISO-8601 date-time string."""
    if not isinstance(value, str) or not _DT_RE.fullmatch(value):
        return False
    from datetime import datetime

    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False

def now_iso() -> str:
    """Return the current UTC timestamp in ISO-8601 format (second precision)."""
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")


# --------------------------------------------------------------------------- #
# Hashing Helpers                                                             #
# --------------------------------------------------------------------------- #

def sha256_file(path: Path) -> str:
    """Return the SHA-256 hash for the file at the given path."""
    h = hashlib.sha256()
    with path.open("rb") as fd:
        for chunk in iter(lambda: fd.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _json_safe_recursor(x: Any) -> Any:
    """Recursively prepare an object for deterministic JSON hashing."""
    # Defer pandas import until it's needed
    try:
        import pandas as pd
        if isinstance(x, pd.DataFrame):
            js = x.to_json(orient="split", date_unit="ns")
            return {"__dataframe_sha256__": hashlib.sha256(js.encode()).hexdigest()}
    except ImportError:
        pass # pandas not installed, will not handle DataFrame type

    if isinstance(x, dict):
        return {k: _json_safe_recursor(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [_json_safe_recursor(v) for v in x]
    return x


def hash_object(obj: Any) -> str:
    """Return a deterministic SHA-256 hash for a Python object.

    Handles nested dicts, lists, and pandas DataFrames.
    """
    safe_obj = _json_safe_recursor(obj)
    encoded = json.dumps(safe_obj, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


# --------------------------------------------------------------------------- #
# Environment Capture Helpers                                                 #
# --------------------------------------------------------------------------- #

def get_library_versions() -> Dict[str, str]:
    """Capture versions of key ML/data science libraries."""
    wanted = {"scikit-learn", "pandas", "numpy", "tensorflow", "torch", "xgboost"}
    versions: Dict[str, str] = {}
    for dist in _im.distributions():
        name = dist.metadata.get("Name") or ""
        if name.lower() in wanted:
            versions[name] = dist.version
    return versions


def get_hardware_specs() -> Dict[str, str]:
    """Gather basic hardware specs (CPU, RAM, GPU)."""
    cpu = platform.processor() or platform.machine()
    ram = ""
    # Defer psutil import as it's an optional dependency
    try:
        import psutil  # type: ignore

        ram = f"{round(psutil.virtual_memory().total / 2**30)} GB"
    except Exception:
        pass # psutil not installed or failed
    gpu = os.getenv("NVIDIA_VISIBLE_DEVICES", "")
    return {"cpu": cpu, "gpu": gpu, "ram": ram}