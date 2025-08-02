"""
document.py - A generic, schema-aware document builder.
"""
from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path
from typing import Any, Mapping
import uuid, socket, getpass

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
        """Populate any schema-required meta-fields, then validate."""
        if self.__finalised:           # idempotent
            return

        # ------------------------------------------------------------------ #
        # Core timing                                                        #
        # ------------------------------------------------------------------ #
        now_iso                       = utils._now_iso()
        self["finalization_dtg"]      = now_iso
        init_dt                       = _dt.datetime.fromisoformat(
                                           self["initialization_dtg"].replace("Z", "+00:00"))
        end_dt                        = _dt.datetime.fromisoformat(
                                           now_iso.replace("Z", "+00:00"))
        self["total_runtime_seconds"] = int((end_dt - init_dt).total_seconds())

        # Helper – write a value **only if** the schema expects the field and
        # the caller hasn’t supplied one already.
        def _maybe(field: str, value: Any) -> None:
            if field in self.__schema.get("fields", {}) and field not in self:
                self[field] = value
        
        _maybe("run_id",               str(uuid.uuid4()))
        _maybe("run_user",             getpass.getuser())
        _maybe("run_host",             socket.gethostname())
        _maybe("run_start_dtg",        self["initialization_dtg"])
        _maybe("run_end_dtg",          self["finalization_dtg"])
        _maybe("run_duration_seconds", self["total_runtime_seconds"])

        # Required analytic identifiers – fall back to placeholders
        _maybe("analytic_id",      "UNKNOWN")
        _maybe("analytic_name",    "UNKNOWN")
        _maybe("analytic_version", "UNKNOWN")

        # Schema versions
        inputs = self.get("inputs", {})
        if isinstance(inputs, dict):
            _maybe("input_schema_version",  inputs.get("input_schema_version", "UNKNOWN"))
        else:
            _maybe("input_schema_version",  "UNKNOWN")
        _maybe("output_schema_version",     "UNKNOWN")

        # Hashes (if their schema fields exist)
        if "inputs" in self and "input_hash" in self.__schema.get("fields", {}):
            self["input_hash"] = utils._hash(self["inputs"])

        if "findings" in self and "findings_hash" in self.__schema.get("fields", {}):
            self["findings_hash"] = utils._hash(self["findings"])

        # ------------------------------------------------------------------ #
        # Model-schema specific                                              #
        # ------------------------------------------------------------------ #
        if ("model_file_hash" in self.__schema.get("fields", {})
                and "model_file_hash" not in self):
            from pathlib import Path
            path_str = self.get("model_file_path") or self.get("model_path")
            try:
                self["model_file_hash"] = (
                    utils._sha256(Path(path_str)) if path_str else "0" * 64
                )
            except Exception:
                self["model_file_hash"] = "0" * 64  # placeholder

        # ------------------------------------------------------------------ #
        # Execution environment (common)                                     #
        # ------------------------------------------------------------------ #
        if "execution_environment" in self.__schema.get("fields", {}):
            self.setdefault("execution_environment", {
                "python_version":       utils.platform.python_version(),
                "library_dependencies": utils._library_versions(),
                "operating_system":     f"{utils.platform.system()} {utils.platform.release()}",
                "username":             getpass.getuser(),
                "hardware_specs":       utils._hardware_specs(),
            })

        # ------------------------------------------------------------------ #
        # Final validation                                                   #
        # ------------------------------------------------------------------ #
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