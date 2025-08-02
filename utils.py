"""
utils.py – shared, low-level utilities for the contract-schema package.

This module consolidates common helpers for:
- Hashing (files and JSON-safe Python objects)
- Timestamps (ISO-8601 format)
- Environment capture (hardware, library versions)
- Type checking (datetime strings)
- Display helpers (notebook-aware printing)
"""

from __future__ import annotations

import datetime as _dt
import getpass
import hashlib
import json
import os
import platform
import socket
import re
from pathlib import Path
from typing import Any, Dict, Union, Tuple

# --------------------------------------------------------------------------- #
# Display Helper                                                              #
# --------------------------------------------------------------------------- #

def _display(obj: Any, **print_kwargs) -> None:
    """Pretty-print that degrades gracefully outside Jupyter."""
    try:
        # "type: ignore[misc]" because IPython may not be present
        get_ipython  # type: ignore  # noqa: F401
        from IPython.display import display  # type: ignore

        display(obj)
    except Exception:  # fall back to plain text in any environment
        print(obj, **print_kwargs)

# --------------------------------------------------------------------------- #
# Timestamp & Hashing Utilities                                               #
# --------------------------------------------------------------------------- #

def _now_iso() -> str:
    """Current UTC timestamp in ISO-8601 (second precision)."""
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")


def _sha256(path: Path) -> str:
    """Return SHA-256 hash for the file at the given path."""
    h = hashlib.sha256()
    with path.open("rb") as fd:
        for chunk in iter(lambda: fd.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _json_safe(x: Any) -> Any:
    """Recursively prepare an object for deterministic JSON hashing."""
    if isinstance(x, dict):
        return {k: _json_safe(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [_json_safe(v) for v in x]
    
    # Handle optional pandas DataFrame support
    try:
        import pandas as pd
        if isinstance(x, pd.DataFrame):
            js = x.to_json(orient="split", date_unit="ns")
            return {"__dataframe_sha256__": hashlib.sha256(js.encode()).hexdigest()}
    except ImportError:
        pass # pandas not installed, continue

    return x


def _hash(obj: Any) -> str:
    """Return a deterministic SHA-256 hash for a Python object."""
    safe_obj = _json_safe(obj)
    encoded = json.dumps(safe_obj, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


# --------------------------------------------------------------------------- #
# Type Checking & Validation Helpers                                          #
# --------------------------------------------------------------------------- #

_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+\-]\d{2}:\d{2})$")

def _is_datetime(value: Any) -> bool:
    """Return True iff *value* is a valid ISO-8601 date-time string."""
    if not isinstance(value, str) or not _DT_RE.fullmatch(value):
        return False
    from datetime import datetime

    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


_TYPE_MAP: dict[str, Union[type, Tuple[type, ...]]] = {
    "string": str,
    "integer": int,
    "number": (int, float),
    "boolean": bool,
    "object": dict,
    "list": list,
}

# Add optional pandas support
try:
    import pandas as pd
    _TYPE_MAP["dataframe"] = pd.DataFrame
except ImportError:
    pass


# --------------------------------------------------------------------------- #
# Environment Capture                                                         #
# --------------------------------------------------------------------------- #

def _library_versions() -> Dict[str, str]:
    """Capture versions of key ML/data science libraries."""
    # Use importlib.metadata which is standard in Python 3.8+
    import importlib.metadata as _im
    
    wanted = {"scikit-learn", "pandas", "numpy", "tensorflow", "torch", "xgboost"}
    versions: Dict[str, str] = {}
    for dist in _im.distributions():
        name = dist.metadata.get("Name") or ""
        if name.lower() in wanted:
            versions[name] = dist.version
    return versions


def _hardware_specs() -> Dict[str, str]:
    """Gather basic hardware specifications (CPU, RAM, GPU)."""
    cpu = platform.processor() or platform.machine()
    ram = ""
    # Use optional psutil for reliable RAM info
    try:
        import psutil  # type: ignore
        ram = f"{round(psutil.virtual_memory().total / 2**30)} GB"
    except Exception:
        pass
    gpu = os.getenv("NVIDIA_VISIBLE_DEVICES", "")
    return {"cpu": cpu, "gpu": gpu, "ram": ram}