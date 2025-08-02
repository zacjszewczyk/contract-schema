"""Provides high-level helpers for creating and saving model artifacts.

This module contains the primary user-facing components for generating a
model manifest and persisting it alongside the serialized model. It offers:

- `ModelManifest`: A dictionary subclass that acts as a builder for creating a
  structured, valid manifest. It simplifies the process by automatically
  populating complex fields like the execution environment and file hashes.
- `save_model()`: A convenience function that handles the entire process of
  pickling a model, creating its corresponding manifest, and saving both
  artifacts to disk with consistent naming.

The module relies on standard library features and `importlib.metadata` for
dependency capture, making it portable and suitable for various environments.
"""

from __future__ import annotations
import datetime as _dt
import getpass, hashlib, json, os, pickle, platform, socket, sys
from pathlib import Path
from typing import Any, Dict, Tuple

# `importlib.metadata` is used for inspecting the environment's installed
# packages to document library versions for reproducibility.
import importlib.metadata as _im
import pandas as pd

# Internal package imports for accessing the schema and validation logic.
from .loader import OUTPUT_SCHEMA
from .validator import validate_manifest, SchemaError


def _sha256(path: Path) -> str:
    """Compute the SHA-256 hash of a file.

    This function reads the file in 8KB chunks to efficiently handle large
    files without consuming excessive memory.

    Args:
        path (Path): The file path for which to compute the hash.

    Returns:
        str: The hexadecimal SHA-256 hash string of the file's contents.
    """
    h = hashlib.sha256()
    with path.open("rb") as fd:
        # Read the file in chunks to support large model files.
        for chunk in iter(lambda: fd.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _library_versions() -> Dict[str, str]:
    """Capture the versions of key machine learning libraries.

    This function scans the Python environment for a predefined set of
    critical libraries and records their installed versions. This information
    is essential for reproducing the model training environment.

    Returns:
        Dict[str, str]: A dictionary mapping library names to their version
                        strings.
    """
    # A set of important libraries to check for. The check is case-insensitive.
    wanted = {"scikit-learn", "pandas", "numpy", "tensorflow", "torch", "xgboost"}
    versions = {}
    for dist in _im.distributions():
        n = dist.metadata["Name"]
        if n and n.lower() in wanted:
            versions[n] = dist.version
    return versions


def _hardware_specs() -> Dict[str, str]:
    """Gather basic hardware specifications of the execution environment.

    This function collects information about the CPU, RAM, and available GPUs
    to document the hardware context in which the model was trained.

    Returns:
        Dict[str, str]: A dictionary with keys 'cpu', 'gpu', and 'ram'.
    """
    cpu = platform.processor() or platform.machine()
    ram = ""
    try:
        # `psutil` is an optional, more reliable dependency for getting system
        # information like total RAM. It is imported here to avoid making it
        # a hard requirement for the package.
        import psutil  # noqa: S402 (optional dependency)
        ram = f"{round(psutil.virtual_memory().total / 2**30)} GB"
    except Exception:
        # If `psutil` is not available or fails, RAM info is omitted.
        pass
    # A crude but often effective way to check for GPU allocation is to inspect
    # common environment variables set by schedulers or CUDA.
    gpu = os.getenv("NVIDIA_VISIBLE_DEVICES", "")
    return {"cpu": cpu, "gpu": gpu, "ram": ram}


def _now_iso() -> str:
    """Get the current UTC time as an ISO 8601 string.

    Returns:
        str: The current UTC datetime, formatted to second precision.
    """
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")


class ModelManifest(dict):
    """A dictionary subclass for building and validating a model manifest.

    This class simplifies the creation of a manifest by providing a structured
    way to add metadata. It culminates in a `finalise` step that automatically
    injects computed data (like file hashes and environment details) and
    performs a full validation against the official schema.

    Typical use involves instantiating it with known metadata, calling
    `finalise` after the model is saved, and then using `save` to persist it.
    """

    def __init__(self, *, model_type: str, **kwargs):
        """Initialize the manifest with a model type and other seed data.

        Args:
            model_type (str): The high-level type of the model (e.g.,
                              "RandomForest", "NeuralNetwork"). This is a
                              required field.
            **kwargs: Any other key-value pairs to pre-populate the manifest.
        """
        super().__init__(**kwargs)
        now_iso = _now_iso()

        # Core identifiers & timing
        self["model_type"]          = model_type
        self["initialization_dtg"]  = now_iso
        self["learning_task"]       = learning_task

        self._finalised             = False

    def finalise(self, model_path: Path) -> None:
        """Complete the manifest, inject computed fields, and validate.

        This method should be called after the model has been serialized to
        disk. It performs the final, critical steps of manifest creation:
        1.  Computes the SHA-256 hash of the model file.
        2.  Gathers and injects the full execution environment details.
        3.  Runs a full validation against the schema contract.

        Args:
            model_path (Path): The path to the saved model artifact.

        Raises:
            SchemaError: If the completed manifest fails validation.
        """
        # The method is idempotent; it will not run more than once.
        if self._finalised:
            return

        # Calculate runtime from the manifest's own timestamps.
        init_dt = _dt.datetime.fromisoformat(
            self["initialization_dtg"].replace("Z", "+00:00")
        )
        final_dt = _dt.datetime.fromisoformat(
            self["finalization_dtg"].replace("Z", "+00:00")
        )
        self["total_runtime_seconds"] = int((final_dt - init_dt).total_seconds())

        self["model_file_hash"] = _sha256(model_path)

        # The execution environment is auto-populated only if it has not
        # already been provided by the user during instantiation.
        self.setdefault("execution_environment", {
            "python_version":       platform.python_version(),
            "library_dependencies": _library_versions(),
            "operating_system":     f"{platform.system()} {platform.release()}",
            "username":             getpass.getuser(),
            "hardware_specs":       _hardware_specs(),
        })

        # The final, complete manifest is validated against the schema.
        validate_manifest(self)
        self._finalised = True

    def update_field(self, field_name: str, value: Any) -> None:
        """Create or update a field in the manifest and re-validate.

        This method allows for adding or modifying a field after the initial
        creation of the manifest. If the manifest has already been finalized,
        it will be re-validated against the schema after the update to ensure
        its integrity is maintained.

        Args:
            field_name (str): The name (key) of the field to create or update.
            value (Any): The new value for the field.

        Raises:
            SchemaError: If the update results in a manifest that fails
                         schema validation.
        """
        # Set or update the value for the given field name.
        self[field_name] = value

        # If the manifest has already been through the finalization process,
        # it must be re-validated to ensure the change has not made it
        # non-compliant with the schema.
        if self._finalised:
            validate_manifest(self)

    def save(self, path: Path | str, *, indent: int = 2) -> None:
        """Save the finalized manifest to a JSON file.

        Args:
            path (Path | str): The destination file path for the manifest.
            indent (int): The indentation level for pretty-printing the JSON.

        Raises:
            RuntimeError: If this method is called before `finalise()`.
        """
        # A check ensures that the manifest is complete before it is saved.
        if not self._finalised:
            raise RuntimeError("Manifest must be finalised() before save()")
        Path(path).write_text(json.dumps(self, ensure_ascii=False, indent=indent),
                              encoding="utf-8")
def save_model(
    model: Any,
    *,
    manifest: Dict[str, Any],
    directory: str | Path = ".",
    file_prefix: str | None = None
) -> Tuple[Path, Path]:
    """Save a model and its corresponding manifest in a single operation.

    This function provides a convenient one-shot workflow for persisting a
    trained model. It handles file naming, model serialization (via pickle),
    manifest creation and validation, and saving both artifacts to disk.

    Example:
        model_path, manifest_path = save_model(
            model=my_classifier,
            manifest={"model_version": "1.0", ...}
        )

    Args:
        model (Any): The trained, picklable model object to be saved.
        manifest (Dict[str, Any]): A dictionary containing the user-supplied
                                   metadata for the manifest.
        directory (str | Path): The directory where the model and manifest
                                files will be saved. Defaults to the current
                                directory.
        file_prefix (str | None): An optional prefix for the output filenames.
                                  If not provided, a default is generated
                                  from the model type and a timestamp.

    Returns:
        Tuple[Path, Path]: A tuple containing the `pathlib.Path` objects for
                           the saved model file and the manifest file,
                           respectively.
    """
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)

    # Generate a consistent timestamp and filename stem for both artifacts.
    dtg   = _dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    mtype = manifest.get("model_type", getattr(model, "__class__", type(model)).__name__)
    stem  = file_prefix or f"{mtype}_{dtg}"

    model_path    = directory / f"{stem}.pkl"
    manifest_path = directory / f"{stem}_manifest.json"

    # 1. Serialize the model object using the highest available pickle protocol
    #    for efficiency and compatibility.
    with model_path.open("wb") as fd:
        pickle.dump(model, fd, protocol=pickle.HIGHEST_PROTOCOL)

    # 2. Instantiate the manifest, inject the user-supplied fields,
    #    finalize it by adding computed fields, and save it.
    mani = ModelManifest(**manifest)
    mani.finalise(model_path)
    mani.save(manifest_path)

    return model_path, manifest_path
