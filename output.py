"""
output.py – unified high‑level helpers for both analytic *outputs* and ML *manifests*.

Key classes
-----------
OutputDoc
    Lightweight builder for analytic results that validates against the
    packaged OUTPUT_SCHEMA (typically the `findings` contract).

ModelManifest
    Provenance‑rich description of a saved machine‑learning artefact.  Handles
    execution‑environment capture, file hashing, and schema validation.

Utility functions
-----------------
save_model(model, *, manifest, directory=".")
    One‑shot helper that pickles ``model`` and writes a validated
    ``ModelManifest`` alongside it.

loaders / validators from this package
-------------------------------------
Both classes depend on :pymod:`model_schema.loader` for the JSON contract and
:pymod:`model_schema.validator` for runtime validation.
"""

from __future__ import annotations

import copy
import datetime as _dt
import enum
import getpass
import json
import pickle
import platform
import socket
import uuid
from pathlib import Path
from typing import Any, Dict, Tuple, Union

from . import utils
from .loader import OUTPUT_SCHEMA
from .validator import validate, SchemaError

# --------------------------------------------------------------------------- #
# Enum helper                                                                 #
# --------------------------------------------------------------------------- #

class _Level(enum.Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"
    FATAL = "FATAL"

# --------------------------------------------------------------------------- #
# OutputDoc – for analytic pipelines                                          #
# --------------------------------------------------------------------------- #

class OutputDoc(dict):
    """Build an output document that conforms to ``OUTPUT_SCHEMA``.

    *Valid* once :py:meth:`finalise` succeeds.
    """

    def __init__(self, *, input_data_hash: str, **kwargs: Any):
        super().__init__(**kwargs)
        if not isinstance(input_data_hash, str):
            raise TypeError("input_data_hash must be a hex string")
        self["input_data_hash"] = input_data_hash
        self.__start = _dt.datetime.now(_dt.timezone.utc)
        self.setdefault("messages", [])
        self.add_message(_Level.INFO, "Output document created")

    # ------------------------------ logging ---------------------------------
    def add_message(self, level: Union[str, _Level], text: str) -> None:
        if isinstance(level, str):
            try:
                level = _Level[level.upper()]
            except KeyError as exc:
                allowed = ", ".join(_Level.__members__)
                raise ValueError(f"Invalid log level '{level}'. Allowed: {allowed}") from exc
        elif not isinstance(level, _Level):
            raise TypeError("level must be str or _Level")
        if not isinstance(text, str):
            raise TypeError("text must be str")

        self["messages"].append({
            "timestamp": utils._now_iso(),
            "level": level.value,
            "text": text,
        })

    # ------------------------------ lifecycle ------------------------------
    def finalise(self) -> None:
        if "inputs" not in self or not isinstance(self["inputs"], dict):
            raise SchemaError("missing or invalid 'inputs' before finalise()")

        end = _dt.datetime.now(_dt.timezone.utc)
        self.setdefault("run_id", str(uuid.uuid4()))
        self.setdefault("run_user", getpass.getuser() if hasattr(getpass, "getuser") else "unknown_user")
        self.setdefault("run_host", socket.gethostname() if hasattr(socket, "gethostname") else "unknown_host")
        self.setdefault("run_start_dtg", self.__start.isoformat(timespec="seconds"))
        self.setdefault("run_end_dtg", end.isoformat(timespec="seconds"))
        self["run_duration_seconds"] = round((end - self.__start).total_seconds(), 6)

        # compute and store hashes
        self["input_hash"] = utils._hash(self["inputs"])
        self.setdefault("findings", [])
        if not isinstance(self["findings"], list):
            raise SchemaError("'findings' must be a list")

        finding_schema = OUTPUT_SCHEMA["fields"]["findings"]["items"]
        for idx, finding in enumerate(self["findings"]):
            validate(finding, schema=finding_schema, path=f"OutputDoc.findings[{idx}]")

        self["findings_hash"] = utils._hash(self["findings"])

        # sensible defaults for optional meta
        defaults = {
            "input_schema_version": self["inputs"].get("input_schema_version", "UNKNOWN"),
            "output_schema_version": "UNKNOWN",
            "analytic_id": "UNKNOWN",
            "analytic_name": "UNKNOWN",
            "analytic_version": "UNKNOWN",
            "status": "UNKNOWN",
            "exit_code": -1,
            "records_processed": 0,
        }
        for k, v in defaults.items():
            self.setdefault(k, v)

        self.add_message(_Level.INFO, "Output document finalised")
        validate(self, schema=OUTPUT_SCHEMA, path="OutputDoc")

    def save(self, path: Union[str, Path], *, indent: int = 2, quiet: bool = False) -> None:
        if "run_id" not in self:
            raise RuntimeError("save() called before finalise()")
        p = Path(path)
        p.write_text(json.dumps(self, indent=indent, ensure_ascii=False), encoding="utf-8")
        if not quiet:
            utils._display(f"Output saved to {p.resolve()}")


# --------------------------------------------------------------------------- #
# ModelManifest – for ML artefacts                                            #
# --------------------------------------------------------------------------- #

class ModelManifest(dict):
    """Builder for a model manifest that adheres to ``OUTPUT_SCHEMA``."""

    def __init__(
        self,
        *,
        model_type: str,
        learning_task: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self["model_type"] = model_type
        self["learning_task"] = learning_task
        self["initialization_dtg"] = utils._now_iso()
        self._finalised = False

    # ------------------------------ lifecycle ------------------------------
    def finalise(self, model_path: Path) -> None:
        if self._finalised:
            return

        final_dtg = utils._now_iso()
        self["finalization_dtg"] = final_dtg
        init_dt = _dt.datetime.fromisoformat(self["initialization_dtg"].replace("Z", "+00:00"))
        end_dt = _dt.datetime.fromisoformat(final_dtg.replace("Z", "+00:00"))
        self["total_runtime_seconds"] = int((end_dt - init_dt).total_seconds())
        self.setdefault("export_dtg", final_dtg)
        self["model_file_hash"] = utils._sha256(model_path)

        # execution environment (unless user overrode)
        self.setdefault(
            "execution_environment",
            {
                "python_version": platform.python_version(),
                "library_dependencies": utils._library_versions(),
                "operating_system": f"{platform.system()} {platform.release()}",
                "username": getpass.getuser(),
                "hardware_specs": utils._hardware_specs(),
            },
        )

        validate(self, schema=OUTPUT_SCHEMA, path="ModelManifest")
        self._finalised = True

    # ------------------------------- helpers ------------------------------
    def update_field(self, field_name: str, value: Any) -> None:
        self[field_name] = value
        if self._finalised:
            validate(self, schema=OUTPUT_SCHEMA, path="ModelManifest")

    def save(self, path: Union[Path, str], *, indent: int = 2) -> None:
        if not self._finalised:
            raise RuntimeError("Manifest must be finalised() before save()")
        Path(path).write_text(json.dumps(self, indent=indent, ensure_ascii=False), encoding="utf-8")
        utils._display(f"Manifest saved to {Path(path).resolve()}")


# --------------------------------------------------------------------------- #
# Convenience – save model + manifest                                         #
# --------------------------------------------------------------------------- #

def save_model(
    model: Any,
    *,
    manifest: Dict[str, Any],
    directory: Union[str, Path] | None = ".",
    file_prefix: str | None = None,
) -> Tuple[Path, Path]:
    """Pickle *model* & persist a corresponding :class:`ModelManifest`."""

    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)

    dtg = _dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    mtype = manifest.get("model_type") or getattr(model, "__class__", type(model)).__name__
    stem = file_prefix or f"{mtype}_{dtg}"

    model_path = directory / f"{stem}.pkl"
    manifest_path = directory / f"{stem}_manifest.json"

    with model_path.open("wb") as fd:
        pickle.dump(model, fd, protocol=pickle.HIGHEST_PROTOCOL)

    mani = ModelManifest(**manifest)
    mani.finalise(model_path)
    mani.save(manifest_path)

    return model_path, manifest_path