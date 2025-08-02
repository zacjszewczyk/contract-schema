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
import hashlib
import json
import os
import pickle
import platform
import socket
import uuid
from pathlib import Path
from typing import Any, Dict, Tuple, Union

import importlib.metadata as _im
import pandas as pd

from .loader import OUTPUT_SCHEMA
from .validator import validate_manifest, _validate, SchemaError

# --------------------------------------------------------------------------- #
# Helper – notebook‑aware printing                                             #
# --------------------------------------------------------------------------- #

def _display(obj: Any, **print_kwargs) -> None:
    """Pretty‑print that degrades gracefully outside Jupyter."""
    try:
        # "type: ignore[misc]" because IPython may not be present
        get_ipython  # type: ignore  # noqa: F401
        from IPython.display import display  # type: ignore

        display(obj)
    except Exception:  # fall back to plain text in any environment
        print(obj, **print_kwargs)


# --------------------------------------------------------------------------- #
# Enum + misc helpers                                                         #
# --------------------------------------------------------------------------- #

class _Level(enum.Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"
    FATAL = "FATAL"


def _now_iso() -> str:
    """Current UTC timestamp in ISO‑8601 (second precision)."""
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")


def _sha256(path: Path) -> str:
    """Return SHA‑256 hash for *file* at *path*."""
    h = hashlib.sha256()
    with path.open("rb") as fd:
        for chunk in iter(lambda: fd.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


# --------------------------------------------------------------------------- #
# OutputDoc – for analytic pipelines                                          #
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
            "timestamp": _now_iso(),
            "level": level.value,
            "text": text,
        })

    # --------------------------- hashing helpers ---------------------------
    @staticmethod
    def _json_safe(x: Any) -> Any:  # noqa: PLR0911 – early returns aid clarity
        if isinstance(x, pd.DataFrame):
            js = x.to_json(orient="split", date_unit="ns")
            return {"__dataframe_sha256__": hashlib.sha256(js.encode()).hexdigest()}
        if isinstance(x, dict):
            return {k: OutputDoc._json_safe(v) for k, v in x.items()}
        if isinstance(x, (list, tuple)):
            return [OutputDoc._json_safe(v) for v in x]
        return x

    @classmethod
    def _hash(cls, obj: Any) -> str:
        safe = cls._json_safe(obj)
        b = json.dumps(safe, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(b).hexdigest()

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
        self["input_hash"] = OutputDoc._hash(self["inputs"])
        self.setdefault("findings", [])
        if not isinstance(self["findings"], list):
            raise SchemaError("'findings' must be a list")

        finding_schema = OUTPUT_SCHEMA["fields"]["findings"]["items"]
        for idx, finding in enumerate(self["findings"]):
            _validate(finding, finding_schema, path=f"OutputDoc.findings[{idx}]")

        self["findings_hash"] = OutputDoc._hash(self["findings"])

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
        _validate(self, OUTPUT_SCHEMA, path="OutputDoc")

    def save(self, path: Union[str, Path], *, indent: int = 2, quiet: bool = False) -> None:
        if "run_id" not in self:
            raise RuntimeError("save() called before finalise()")
        p = Path(path)
        p.write_text(json.dumps(self, indent=indent, ensure_ascii=False), encoding="utf-8")
        if not quiet:
            _display(f"Output saved to {p.resolve()}")


# --------------------------------------------------------------------------- #
# ModelManifest – for ML artefacts                                            #
# --------------------------------------------------------------------------- #


def _library_versions() -> Dict[str, str]:
    wanted = {"scikit-learn", "pandas", "numpy", "tensorflow", "torch", "xgboost"}
    versions: Dict[str, str] = {}
    for dist in _im.distributions():
        name = dist.metadata.get("Name") or ""
        if name.lower() in wanted:
            versions[name] = dist.version
    return versions


def _hardware_specs() -> Dict[str, str]:
    cpu = platform.processor() or platform.machine()
    ram = ""
    try:
        import psutil  # type: ignore

        ram = f"{round(psutil.virtual_memory().total / 2**30)} GB"
    except Exception:
        pass
    gpu = os.getenv("NVIDIA_VISIBLE_DEVICES", "")
    return {"cpu": cpu, "gpu": gpu, "ram": ram}


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
        self["initialization_dtg"] = _now_iso()
        self._finalised = False

    # ------------------------------ lifecycle ------------------------------
    def finalise(self, model_path: Path) -> None:
        if self._finalised:
            return

        final_dtg = _now_iso()
        self["finalization_dtg"] = final_dtg
        init_dt = _dt.datetime.fromisoformat(self["initialization_dtg"].replace("Z", "+00:00"))
        end_dt = _dt.datetime.fromisoformat(final_dtg.replace("Z", "+00:00"))
        self["total_runtime_seconds"] = int((end_dt - init_dt).total_seconds())
        self.setdefault("export_dtg", final_dtg)
        self["model_file_hash"] = _sha256(model_path)

        # execution environment (unless user overrode)
        self.setdefault(
            "execution_environment",
            {
                "python_version": platform.python_version(),
                "library_dependencies": _library_versions(),
                "operating_system": f"{platform.system()} {platform.release()}",
                "username": getpass.getuser(),
                "hardware_specs": _hardware_specs(),
            },
        )

        validate_manifest(self)
        self._finalised = True

    # ------------------------------- helpers ------------------------------
    def update_field(self, field_name: str, value: Any) -> None:
        self[field_name] = value
        if self._finalised:
            validate_manifest(self)

    def save(self, path: Union[Path, str], *, indent: int = 2) -> None:
        if not self._finalised:
            raise RuntimeError("Manifest must be finalised() before save()")
        Path(path).write_text(json.dumps(self, indent=indent, ensure_ascii=False), encoding="utf-8")
        _display(f"Manifest saved to {Path(path).resolve()}")


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
