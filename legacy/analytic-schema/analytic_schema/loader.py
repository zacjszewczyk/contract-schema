import json
import copy
from pathlib import Path

# ─── Locate and load the single source-of-truth JSON schema ───────────────
_SCRIPT_DIR = Path(__file__).parent.resolve()
SCHEMA_PATH = _SCRIPT_DIR / "analytic_schema.json"

try:
    with SCHEMA_PATH.open("r", encoding="utf-8") as f:
        _raw_schema = json.load(f)
except FileNotFoundError as exc:
    raise FileNotFoundError(f"Schema file not found: {SCHEMA_PATH}") from exc
except json.JSONDecodeError as exc:
    raise ValueError(f"Schema file is not valid JSON: {exc}") from exc

#: Top-level version of the contract
SCHEMA_VERSION = _raw_schema.get("version")

#: Shallow‐copy of the input‐schema section
INPUT_SCHEMA = copy.deepcopy(_raw_schema["input"])    # must exist

#: Shallow‐copy of the output‐schema section
OUTPUT_SCHEMA = copy.deepcopy(_raw_schema["output"])  # must exist

# ─── Embed the INPUT_SCHEMA inside OUTPUT_SCHEMA under the "inputs" field  ─
# so that nested validation can recurse into the same contract.
OUTPUT_SCHEMA.setdefault("fields", {})
OUTPUT_SCHEMA["fields"]["inputs"] = copy.deepcopy(INPUT_SCHEMA)