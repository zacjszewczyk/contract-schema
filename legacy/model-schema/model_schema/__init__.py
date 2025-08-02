"""
Initializes the model_schema package and exposes its public API.

This module acts as the single entry-point for all public functionality. Sub-modules are imported and re-exported so that users can simply:

    >>> import model_schema as msc
    >>> mani = msc.ModelManifest(model_type="RandomForest")
    >>> msc.validate_manifest(mani)
"""

# Import key components from submodules to create the public package API.
from .loader    import OUTPUT_SCHEMA, SCHEMA_VERSION, SCHEMA_PATH
from .validator import validate_manifest, SchemaError
from .output    import save_model, ModelManifest
from .card      import contract_card, model_card

# The `__all__` variable explicitly declares the public names that should
# be imported when a client uses a wildcard import (e.g., `from
# model_schema import *`). This helps prevent namespace pollution and
# clearly defines the package's public contract.
__all__ = [
    "OUTPUT_SCHEMA", "SCHEMA_VERSION", "SCHEMA_PATH",
    "validate_manifest", "SchemaError",
    "save_model", "ModelManifest",
    "contract_card", "model_card",
]