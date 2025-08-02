"""
contract_schema – A toolkit for schema-driven document generation and validation.
"""
from .contract import Contract
from .document import Document
from .validator import SchemaError
from .parser import parse_input

__all__ = [
    "Contract",
    "Document",
    "SchemaError",
    "parse_input",
]