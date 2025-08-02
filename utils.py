"""
utils.py – shared, low-level utilities for hashing, validation, and I/O.
"""
from __future__ import annotations

import datetime as _dt
import hashlib
import json
import re
from pathlib import Path
from typing import Any

# Optional pandas import for DataFrame hashing
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


def is_iso8601_datetime(value: Any) -> bool:
    """Return True iff *value* is a valid ISO-8601 date-time string."""
    dt_re = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+\-]\d{2}:\d{2})$")
    if not isinstance(value, str) or not dt_re.fullmatch(value):
        return False
    try:
        _dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def get_file_sha256(path: Path) -> str:
    """Return the SHA-256 hash for the file at the given path."""
    h = hashlib.sha256()
    with path.open("rb") as fd:
        for chunk in iter(lambda: fd.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _to_json_safe_object(x: Any) -> Any:
    """Recursively prepare an object for deterministic JSON hashing."""
    if PANDAS_AVAILABLE and isinstance(x, pd.DataFrame):
        # Represent DataFrame by a hash of its content
        js = x.to_json(orient="split", date_unit="ns")
        return {"__dataframe_sha256__": hashlib.sha256(js.encode()).hexdigest()}
    if isinstance(x, dict):
        return {k: _to_json_safe_object(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [_to_json_safe_object(v) for v in x]
    return x


def hash_object(obj: Any) -> str:
    """Return a deterministic SHA-256 hash for a nested Python object."""
    safe_obj = _to_json_safe_object(obj)
    # sort_keys and no separators create a canonical representation
    encoded = json.dumps(safe_obj, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()