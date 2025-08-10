"""
document.py - A generic, schema-aware document builder.
"""
from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path
from typing import Any, Mapping
import uuid, getpass

from . import utils
from . import validator

class Document(dict):
    """A generic builder for a document that conforms to a given schema."""

    def __init__(self, *, schema: Mapping[str, Any], **kwargs: Any):
        super().__init__(**kwargs)
        self.__schema = schema
        self.__finalised = False
        if "initialization_dtg" in self.__schema.get("fields", {}):
            self["initialization_dtg"] = utils._now_iso()

    def add_message(self, level: str, text: str) -> None:
        """Add a timestamped log message to the document, if supported.

        The method is a no-op once :meth:`finalise` has been called and the
        document is immutable.  When the underlying schema lacks a ``messages``
        field a :class:`NotImplementedError` is raised to signal that the
        feature isn't available for the current contract.
        """

        if self.__finalised:
            return

        if "messages" not in self.__schema.get("fields", {}):
            raise NotImplementedError(
                "This document's schema does not support 'messages'."
            )

        if "messages" not in self:
            self["messages"] = []

        self["messages"].append(
            {
                "timestamp": utils._now_iso(),
                "level": level.upper(),
                "text": text,
            }
        )

    def finalise(self) -> None:
        """Populate select meta fields, then validate."""
        if self.__finalised:
            return

        # Record the current time
        now_iso = utils._now_iso()

        if "finalization_dtg" in self.__schema.get("fields", {}):
            self["finalization_dtg"] = now_iso
        
        if "total_runtime_seconds" in self.__schema.get("fields", {}):
            init_dt = _dt.datetime.fromisoformat(self["initialization_dtg"].replace("Z", "+00:00"))
            end_dt = _dt.datetime.fromisoformat(self["finalization_dtg"].replace("Z", "+00:00"))
            self["total_runtime_seconds"] = int((end_dt - init_dt).total_seconds())
        
        if "run_id" in self.__schema.get("fields", {}):
            self["run_id"] = str(uuid.uuid4())

        # Hashes
        if "inputs" in self and "input_hash" in self.__schema.get("fields", {}):
            self["input_hash"] = utils._hash(self["inputs"])

        if "findings" in self and "findings_hash" in self.__schema.get("fields", {}):
            self["findings_hash"] = utils._hash(self["findings"])

        if "model_file_path" in self and "model_file_hash" in self.__schema.get("fields", {}):
            path_str = self.get("model_file_path") or self.get("model_path")
            self["model_file_hash"] = (
                utils._sha256(Path(path_str)) if path_str else "0" * 64
            )

        # Execution environment
        if "execution_environment" in self.__schema.get("fields", {}):
            self.setdefault("execution_environment", {
                "python_version":       utils.platform.python_version(),
                "library_dependencies": utils._library_versions(),
                "operating_system":     f"{utils.platform.system()} {utils.platform.release()}",
                "username":             getpass.getuser(),
                "hardware_specs":       utils._hardware_specs(),
            })

        # Final validation
        validator.validate(self, schema=self.__schema)
        self.__finalised = True

    def save(self, path: Path | str, *, indent: int = 2) -> None:
        """Saves the finalized document to a JSON file."""
        if not self.__finalised:
            raise RuntimeError("Document must be finalised() before saving.")
        
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self, indent=indent, ensure_ascii=False), 
            encoding="utf-8"
        )