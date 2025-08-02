"""
document.py - A generic, schema-aware document builder.
"""
from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path
from typing import Any, Mapping

from . import utils
from . import validator

class Document(dict):
    """A generic builder for a document that conforms to a given schema."""

    def __init__(self, *, schema: Mapping[str, Any], **kwargs: Any):
        super().__init__(**kwargs)
        self.__schema = schema
        self.__finalised = False
        self["initialization_dtg"] = utils._now_iso()

    def add_message(self, level: str, text: str) -> None:
        """Adds a timestamped log message to the document, if schema supports it."""
        if "messages" not in self.__schema.get("fields", {}):
            raise NotImplementedError("This document's schema does not support 'messages'.")
        
        if "messages" not in self:
            self["messages"] = []
        
        self["messages"].append({
            "timestamp": utils._now_iso(),
            "level": level.upper(),
            "text": text,
        })

    def finalise(self) -> None:
        """Calculates final fields and validates the document against its schema."""
        if self.__finalised:
            return

        self["finalization_dtg"] = utils._now_iso()
        init_dt = _dt.datetime.fromisoformat(self["initialization_dtg"].replace("Z", "+00:00"))
        end_dt = _dt.datetime.fromisoformat(self["finalization_dtg"].replace("Z", "+00:00"))
        self["total_runtime_seconds"] = int((end_dt - init_dt).total_seconds())
        
        # Auto-populate execution environment if the schema expects it
        if "execution_environment" in self.__schema.get("fields", {}):
            self.setdefault("execution_environment", {
                "python_version": utils.platform.python_version(),
                "library_dependencies": utils._library_versions(),
                "operating_system": f"{utils.platform.system()} {utils.platform.release()}",
                "username": utils.getpass.getuser(),
                "hardware_specs": utils._hardware_specs(),
            })

        validator.validate(self, schema=self.__schema)
        self.__finalised = True

    def save(self, path: Path | str, *, indent: int = 2) -> None:
        """Saves the finalized document to a JSON file."""
        if not self.__finalised:
            raise RuntimeError("Document must be finalised() before saving.")
        
        Path(path).write_text(
            json.dumps(self, indent=indent, ensure_ascii=False), 
            encoding="utf-8"
        )