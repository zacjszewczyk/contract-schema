"""Shared helpers for the contract-schema test-suite (std-lib only)."""
from __future__ import annotations
import json, tempfile, copy, contextlib
from pathlib import Path
from typing import Any

import contract_schema as cs

# ---- paths -----------------------------------------------------------------
ROOT         = Path(__file__).resolve().parents[1]
SCHEMA_DIR   = ROOT / "schemas"
ANALYTIC_J   = SCHEMA_DIR / "analytic_schema.json"
MODEL_J      = SCHEMA_DIR / "model_schema.json"

ANALYTIC_C   = cs.Contract.load(ANALYTIC_J)
MODEL_C      = cs.Contract.load(MODEL_J)

# ---- tiny helpers ----------------------------------------------------------
def tmp_json(obj: Any) -> Path:
    """Write *obj* to a temp-file --> return the Path (caller must unlink)."""
    fh = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
    Path(fh.name).write_text(json.dumps(obj), encoding="utf-8")
    return Path(fh.name)

@contextlib.contextmanager
def tmp_dir() -> Path:
    """Yield a temporary directory as a Path (auto-cleaned on exit)."""
    td = tempfile.TemporaryDirectory()
    try:
        yield Path(td.name)
    finally:
        td.cleanup()

def with_defaults(base: dict[str, Any], defaults: dict[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(base)
    for k, v in defaults.items():
        out.setdefault(k, copy.deepcopy(v))
    return out
