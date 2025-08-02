"""Loads the `model_schema.json` contract from the package data.

This module is responsible for locating and parsing the JSON schema file that
defines the structure and rules for a valid model manifest. By centralizing
the loading logic here, it ensures that all other parts of the package refer
to the same, single source of truth for the schema contract.

This module exposes the following public constants:
- `SCHEMA_PATH`: The absolute path to the schema file.
- `SCHEMA_VERSION`: The version string extracted from the schema.
- `OUTPUT_SCHEMA`: The specific schema definition for the output manifest.
"""

import json
from pathlib import Path
import copy

# Determine the absolute path to the directory containing this script.
# This ensures that the `model_schema.json` file can be reliably located
# relative to this module, regardless of where the calling script is executed.
_SCRIPT_DIR  = Path(__file__).parent.resolve()
SCHEMA_PATH  = _SCRIPT_DIR / "model_schema.json"

# Attempt to open and load the JSON schema file at module import time.
# This approach fails fast, immediately alerting the developer if the schema
# contract is missing or syntactically incorrect.
try:
    with SCHEMA_PATH.open(encoding="utf-8") as fd:
        _SCHEMA_RAW = json.load(fd)
except FileNotFoundError as exc:
    # Raise a specific error if the schema file cannot be found.
    raise FileNotFoundError(f"Contract not found: {SCHEMA_PATH}") from exc
except json.JSONDecodeError as exc:
    # Raise an error if the file is found but contains invalid JSON.
    raise ValueError(f"Invalid JSON in {SCHEMA_PATH}: {exc}") from exc

# Extract key information from the loaded schema for direct use by other
# modules within the package.
SCHEMA_VERSION = _SCHEMA_RAW.get("version", "UNKNOWN")

# A deep copy of the output schema is created to prevent any downstream
# modifications from mutating the original, globally-loaded schema object.
OUTPUT_SCHEMA  = copy.deepcopy(_SCHEMA_RAW["output"])