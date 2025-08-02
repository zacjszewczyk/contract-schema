"""
contract.py - High-level API for interacting with schema contracts.
"""

from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Mapping

from . import loader
from . import parser
from . import validator
from .document import Document

class Contract:
    """A high-level interface for a loaded input/output schema contract."""

    def __init__(self, title: str, description: str, version: str, input_schema: dict, output_schema: dict):
        """Initializes the Contract with parsed schema definitions."""
        self.title = title
        self.description = description
        self.version = version
        self.input_schema = input_schema
        self.output_schema = output_schema

    @classmethod
    def load(cls, path: str | Path) -> "Contract":
        """Loads a contract schema from a JSON file and returns a Contract instance."""
        schema_data = loader.load_schema(path)

        # Contract with explicit keys
        if (all(key in schema_data for key in ("title", "description", "version", "input", "output"))):
            # Pass all required arguments to the constructor
            return cls(
                title=schema_data.get("title"),
                description=schema_data.get("description"),
                version=schema_data.get("version"),
                input_schema=schema_data.get("input"),
                output_schema=schema_data.get("output"),
            )
        # Otherwise, raise an error
            raise ValueError(f"Schema at '{path}' is not a valid contract schema. Required keys: 'title', 'description', 'version', 'input', 'output'.")

    def parse_and_validate_input(self, source: Any | None = None) -> dict[str, Any]:
        """
        End-to-end helper used by the tests.
        1. Convert *source* into a plain `dict` (CLI / JSON / Mapping).
        2. Dereference JSON-file strings.
        3. Inject schema-defined defaults.
        4. Deep-validate the result.
        5. Return the validated mapping.
        """
        if not self.input_schema:
            raise NotImplementedError("This contract does not define an input schema for parsing.")
        # (1) ───────────────────────────────────────────────────────────────
        if source is None:                         # avoid swallowing the test
            source = []                            # runner’s CLI args
        raw: dict[str, Any] = parser.parse_input(source, schema=self.input_schema)

        # (2) JSON / file dereference ───────────────────────────────────────
        for k, v in list(raw.items()):
            if isinstance(v, str):
                p = Path(v)
                try:
                    raw[k] = json.loads(p.read_text()) if p.is_file() else json.loads(v)
                except (json.JSONDecodeError, FileNotFoundError):
                    pass  # leave untouched
        # (3) default injection ─────────────────────────────────────────────
        for name, spec in self.input_schema.get("fields", {}).items():
            if name not in raw and "default" in spec:
                raw[name] = spec["default"]
        # (4) validate ──────────────────────────────────────────────────────
        validator.validate(raw, schema=self.input_schema)
        # (5) pass object back to caller
        return raw

    def create_document(self, **kwargs) -> Document:
        """Creates a new Document instance tied to this contract's output schema."""
        return Document(schema=self.output_schema, **kwargs)