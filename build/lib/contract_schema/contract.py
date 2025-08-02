"""
contract.py – High-level API for interacting with schema contracts.
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

    def __init__(self, input_schema: dict | None, output_schema: dict, defaults: dict | None = None):
        """Initializes the Contract with parsed schema definitions."""
        self.input_schema = input_schema
        self.output_schema = output_schema
        self.defaults = defaults or {}

    @classmethod
    def load(cls, path: str | Path) -> "Contract":
        """Loads a schema from a JSON file and returns a Contract instance."""
        schema_data = loader.load_schema(path)
        
        # Case 1: Analytic-style with explicit input/output/defaults keys
        if "input" in schema_data and "output" in schema_data:
            return cls(
                input_schema=schema_data.get("input"),
                output_schema=schema_data.get("output"),
                defaults=schema_data.get("defaults")
            )
        # Case 2: Model-style where the whole file is the output schema
        else:
            return cls(input_schema=None, output_schema=schema_data, defaults=None)

    def parse_and_validate_input(self, source: Any) -> dict[str, Any]:
        """Parses and validates an input source against the input schema."""
        if not self.input_schema:
            raise NotImplementedError("This contract does not define an input schema for parsing.")
        
        raw_input = parser.parse_input(source, schema=self.input_schema)
        
        return validator.validate_with_defaults(
            raw_input,
            schema=self.input_schema,
            defaults=self.defaults,
            deref_json_files=True,
        )

    def create_document(self, **kwargs) -> Document:
        """Creates a new Document instance tied to this contract's output schema."""
        return Document(schema=self.output_schema, **kwargs)