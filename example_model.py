#!/usr/bin/env python
"""
example_model.py
================
Comprehensive demonstration of the **contract_schema** library using the
packaged **model_schema.json** contract.

This example trains a RandomForest classifier on the Iris dataset and emits
a complete model manifest demonstrating all schema fields and library features.

This example demonstrates:
--------------------------
1. **Contract Loading**: How to load and access contract metadata
2. **Input Parsing & Validation**: Parsing model training inputs
3. **Model Training**: Training with train/test split for metrics
4. **Document Creation**: Setting all required and optional output fields
5. **Message Logging**: Using add_message() for structured execution logging
6. **Finalisation**: Auto-computed fields (hashes, timestamps, environment)
7. **Export**: Saving manifests as JSON and Markdown reports

Schema Fields Demonstrated
--------------------------
**Input Schema (all fields exercised):**
- log_path (optional): Where to write execution logs
- output (optional): Destination for manifest output
- verbosity (optional): Log level threshold

**Output Schema (all fields exercised):**
Required fields:
- input_schema_version, output_schema_version, run_id
- initialization_dtg, finalization_dtg, total_runtime_seconds
- execution_environment (auto-populated)
- author, author_organization, contact, license, documentation_link
- status, exit_code
- dataset_description, dataset_size, dataset_hash, data_schema, feature_names
- inputs, input_hash (auto-computed)
- model_id, model_type, model_architecture, model_version, model_description
- learning_task, intended_use, limitations
- feature_engineering_pipeline, model_parameters
- train_size, train_hash, random_seed
- metrics, training_duration_seconds
- export_dtg, model_file_path, model_file_hash (auto-computed)

Optional fields:
- contributors: List of project contributors
- messages: Structured log entries via add_message()
- target_feature: Name of the label column
- max_number_trials: Maximum hyperparameter search trials
- actual_number_trials: Actual trials run
- train_splits: Cross-validation splits
- validation_size, validation_hash, validation_duration_seconds
- test_size, test_hash, test_duration_seconds
- additional_model_properties: Model-specific metadata
- additional_run_properties: Run-specific metadata
"""
from __future__ import annotations

from pathlib import Path
import time
import logging

import joblib
from sklearn.datasets import load_iris
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from sklearn.model_selection import train_test_split

from contract_schema import Contract, utils, to_markdown_card
from contract_schema.validator import SchemaError

# --------------------------------------------------------------------------- #
# Logging Configuration                                                       #
# --------------------------------------------------------------------------- #
# Standard Python logging for script-level messages (separate from the
# Document's structured messages field)
logging.basicConfig(
    level="INFO",
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("contract_schema.examples")

# --------------------------------------------------------------------------- #
# Step 1: Load the Contract                                                   #
# --------------------------------------------------------------------------- #
# The Contract class provides the high-level API for schema-driven workflows.
# Loading validates the schema against the meta-schema and extracts:
#   - title, description, version: Contract metadata
#   - input_schema: Schema defining valid input parameters
#   - output_schema: Schema defining valid output document structure

log.info("Loading the model contract schema...")
C = Contract.load("model_schema.json")

# Access contract metadata for documentation or versioning purposes
log.info("Contract: %s (v%s)", C.title, C.version)
log.info("Description: %s", C.description)

# --------------------------------------------------------------------------- #
# Step 2: Build & Validate Model Inputs                                       #
# --------------------------------------------------------------------------- #
# The parse_and_validate_input() method supports multiple input formats:
#   - dict/Mapping: Direct Python dictionary
#   - Path: JSON file on disk
#   - str: File path, JSON literal, or CLI argument string
#   - Sequence[str]: CLI tokens (simulating sys.argv)
#   - None: Defaults to sys.argv[1:]

log.info("Preparing model training input parameters...")

# Demonstrate setting ALL input fields (all are optional with defaults)
inputs = C.parse_and_validate_input(
    {
        # --- Optional fields (with defaults shown) -----------------------------
        "log_path": "stdout",  # (optional) Where to write execution logs; default: "stdout"
        "output": "stdout",  # (optional) Destination for manifest; default: "stdout"
        "verbosity": "INFO",  # (optional) Log level: DEBUG/INFO/WARN/ERROR/FATAL; default: "INFO"
    }
)
log.info("Input validation successful. Fields: %s", list(inputs.keys()))

# --------------------------------------------------------------------------- #
# Step 3: Train the Model                                                     #
# --------------------------------------------------------------------------- #
# This section demonstrates a complete training workflow with:
#   - Train/test split (we'll also track validation separately)
#   - Multiple performance metrics
#   - Timing for training, validation, and test phases

log.info("Loading and preparing dataset...")
start_ts = time.time()

iris = load_iris(as_frame=True)
X, y = iris.data, iris.target
feature_names = list(X.columns)
target_name = "target"

# Create train/validation/test splits (60/20/20)
log.info("Splitting dataset into train/validation/test sets...")
X_temp, X_test, y_temp, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)
X_train, X_val, y_train, y_val = train_test_split(
    X_temp, y_temp, test_size=0.25, random_state=42, stratify=y_temp  # 0.25 of 0.8 = 0.2
)

log.info("Dataset splits: train=%d, validation=%d, test=%d", len(X_train), len(X_val), len(X_test))

# Train the model
log.info("Training RandomForest classifier...")
train_start = time.time()
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)
training_duration = time.time() - train_start
log.info("Training completed in %.2f seconds", training_duration)

# Validate
log.info("Running validation...")
val_start = time.time()
y_val_pred = model.predict(X_val)
val_acc = accuracy_score(y_val, y_val_pred)
validation_duration = time.time() - val_start
log.info("Validation accuracy: %.4f (%.2fs)", val_acc, validation_duration)

# Test
log.info("Running test evaluation...")
test_start = time.time()
y_test_pred = model.predict(X_test)
test_acc = accuracy_score(y_test, y_test_pred)
test_precision = precision_score(y_test, y_test_pred, average="weighted")
test_recall = recall_score(y_test, y_test_pred, average="weighted")
test_f1 = f1_score(y_test, y_test_pred, average="weighted")
test_duration = time.time() - test_start
log.info("Test metrics: accuracy=%.4f, precision=%.4f, recall=%.4f, f1=%.4f (%.2fs)",
         test_acc, test_precision, test_recall, test_f1, test_duration)

total_train_time = time.time() - start_ts

# Save the model
model_path = Path("iris_rf.joblib")
joblib.dump(model, model_path)
log.info("Model saved to %s", model_path.resolve())

# --------------------------------------------------------------------------- #
# Step 4: Build the Model Output Document (Manifest)                          #
# --------------------------------------------------------------------------- #
# The Document class is a dict subclass that:
#   - Tracks the output schema for validation
#   - Auto-populates initialization_dtg on creation
#   - Provides add_message() for structured logging
#   - Computes hashes and environment info on finalise()

log.info("Creating model manifest document...")
doc = C.create_document(
    # =========================================================================
    # PROVENANCE & AUTHORSHIP
    # =========================================================================
    # Version of the input contract used for this run
    input_schema_version=C.version,
    # Version of the output contract (typically matches C.version)
    output_schema_version=C.version,
    # Primary author's name
    author="Zac Szewczyk",
    # Organization at time of execution
    author_organization="Example Org",
    # Contact information (email or URL)
    contact="zac@example.com",
    # Software license for code/outputs
    license="Apache-2.0",
    # URL to documentation, paper, or blog post
    documentation_link=(
        "https://scikit-learn.org/stable/auto_examples/"
        "datasets/plot_iris_dataset.html"
    ),

    # =========================================================================
    # OPTIONAL: Contributors (list of individuals who contributed)
    # =========================================================================
    contributors={  # (optional) Project contributors; default: not included
        "Alice Smith": "Feature engineering and data preprocessing",
        "Bob Jones": "Model selection and hyperparameter tuning",
        "Carol White": "Model validation and testing",
    },

    # =========================================================================
    # EXECUTION STATUS
    # =========================================================================
    # High-level outcome: success, fail, warning, UNKNOWN
    status="success",
    # Process exit code (0 = success, non-zero = failure)
    exit_code=0,

    # =========================================================================
    # DATASET METADATA
    # =========================================================================
    # Human-readable description of the dataset
    dataset_description=(
        "Fisher's Iris flower dataset (1936), a classic multiclass classification "
        "benchmark containing 150 samples of iris flowers with 4 numeric features: "
        "sepal length, sepal width, petal length, and petal width. The target "
        "variable has 3 classes (setosa, versicolor, virginica) with 50 samples each."
    ),
    # Total number of rows in the full dataset
    dataset_size=len(X),
    # SHA-256 hash of the full dataset for integrity verification
    dataset_hash=utils._hash(X),
    # Schema mapping feature names to their data types
    data_schema={c: "number" for c in X.columns},
    # Ordered list of feature names used in training
    feature_names=feature_names,

    # =========================================================================
    # INPUT PARAMETERS
    # =========================================================================
    # Validated input parameters (copy for auditability)
    inputs=inputs,

    # =========================================================================
    # MODEL IDENTIFICATION
    # =========================================================================
    # Canonical identifier of the model
    model_id=str(model_path),
    # High-level model family
    model_type="RandomForest",
    # Detailed architecture description
    model_architecture="RandomForestClassifier with n_estimators=100, criterion=gini, max_features=sqrt",
    # Semantic version of the exported model
    model_version="0.1.0",
    # Brief description of the model's purpose
    model_description=(
        "RandomForest classifier trained on the Iris dataset for species "
        "classification. This model predicts iris species (setosa, versicolor, "
        "virginica) from sepal and petal measurements."
    ),

    # =========================================================================
    # LEARNING TASK & USAGE
    # =========================================================================
    # High-level learning paradigm: classification, regression, clustering,
    # dimensionality_reduction, anomaly_detection, reinforcement_learning
    learning_task="classification",
    # Clear statement of the model's designed application
    intended_use=(
        "Demonstration of the contract_schema library for ML model documentation. "
        "Suitable for educational purposes showing proper model manifest creation. "
        "Not intended for production botanical classification."
    ),
    # OPTIONAL: Name of the label/target column
    target_feature=target_name,  # (optional) Target column name; default: not included
    # Known limitations and scenarios where the model may underperform
    limitations=(
        "1. Trained on a small, balanced dataset (150 samples)\n"
        "2. Only handles the 3 species in the Iris dataset\n"
        "3. No handling of missing values or outliers\n"
        "4. Feature scaling not applied (tree-based models don't require it)\n"
        "5. Hyperparameters not optimized for production use"
    ),

    # =========================================================================
    # PREPROCESSING & PARAMETERS
    # =========================================================================
    # Ordered list of preprocessing steps applied to the data
    feature_engineering_pipeline=[
        "1. Load raw Iris dataset from scikit-learn",
        "2. Split into train/validation/test sets (60/20/20)",
        "3. No feature scaling applied (not required for tree-based models)",
        "4. No feature selection applied (all 4 features used)",
    ],
    # Key-value mapping of model hyperparameters
    model_parameters=model.get_params(),

    # =========================================================================
    # OPTIONAL: Hyperparameter Search Metadata
    # =========================================================================
    max_number_trials=1,  # (optional) Maximum hyperparameter trials; default: not included
    actual_number_trials=1,  # (optional) Actual trials run; default: not included

    # =========================================================================
    # DATA SPLIT SIZES
    # =========================================================================
    # Number of rows in the training set
    train_size=len(X_train),
    # OPTIONAL: Number of cross-validation splits on training data
    train_splits=5,  # (optional) CV splits; default: not included
    # OPTIONAL: Number of rows in the validation set
    validation_size=len(X_val),  # (optional) Validation set size; default: not included
    # OPTIONAL: Number of rows in the test set
    test_size=len(X_test),  # (optional) Test set size; default: not included

    # =========================================================================
    # DATA SPLIT HASHES
    # =========================================================================
    # SHA-256 hash of the training set
    train_hash=utils._hash(X_train),
    # OPTIONAL: SHA-256 hash of the validation set
    validation_hash=utils._hash(X_val),  # (optional) Validation hash; default: not included
    # OPTIONAL: SHA-256 hash of the test set
    test_hash=utils._hash(X_test),  # (optional) Test hash; default: not included

    # =========================================================================
    # REPRODUCIBILITY
    # =========================================================================
    # Random seed for reproducibility
    random_seed=42,

    # =========================================================================
    # PERFORMANCE METRICS
    # =========================================================================
    # Key-value map of metrics (supports any metrics relevant to the task)
    metrics={
        "validation_accuracy": round(val_acc, 4),
        "test_accuracy": round(test_acc, 4),
        "test_precision_weighted": round(test_precision, 4),
        "test_recall_weighted": round(test_recall, 4),
        "test_f1_weighted": round(test_f1, 4),
    },

    # =========================================================================
    # TIMING
    # =========================================================================
    # Total time in seconds for training
    training_duration_seconds=round(training_duration, 2),
    # OPTIONAL: Time for validation
    validation_duration_seconds=round(validation_duration, 2),  # (optional); default: not included
    # OPTIONAL: Time for test evaluation
    test_duration_seconds=round(test_duration, 2),  # (optional); default: not included

    # =========================================================================
    # MODEL FILE
    # =========================================================================
    # UTC timestamp when the model was saved
    export_dtg=utils._now_iso(),
    # Filesystem path to the serialized model
    model_file_path=str(model_path),
    # Note: model_file_hash is auto-computed by finalise() from model_file_path

    # =========================================================================
    # OPTIONAL: Additional Model Properties (model-specific metadata)
    # =========================================================================
    additional_model_properties={  # (optional) Model-specific metadata; default: not included
        "n_classes": 3,
        "class_names": list(iris.target_names),
        "feature_importances": dict(zip(feature_names, [round(x, 4) for x in model.feature_importances_])),
        "oob_score_enabled": False,
    },

    # =========================================================================
    # OPTIONAL: Additional Run Properties (run-specific metadata)
    # =========================================================================
    additional_run_properties={  # (optional) Run-specific metadata; default: not included
        "ci_job_url": "https://example.com/ci/job/67890",
        "git_commit": "def789abc012",
        "ticket_id": "JIRA-5678",
        "experiment_name": "iris_baseline_v1",
    },
)

# --------------------------------------------------------------------------- #
# Step 4b: Add Structured Log Messages (Optional)                             #
# --------------------------------------------------------------------------- #
# The add_message() method appends timestamped, leveled log entries to the
# document's 'messages' field. This provides a structured audit trail of
# execution events separate from console logging.
#
# Supported levels: DEBUG, INFO, WARN, ERROR, FATAL

log.info("Adding structured messages to document...")
doc.add_message("INFO", "Model training pipeline started")
doc.add_message("DEBUG", f"Loaded {len(X)} samples with {len(feature_names)} features")
doc.add_message("INFO", f"Dataset split: train={len(X_train)}, val={len(X_val)}, test={len(X_test)}")
doc.add_message("INFO", f"Training RandomForest with {model.n_estimators} estimators")
doc.add_message("DEBUG", f"Training completed in {training_duration:.2f}s")
doc.add_message("INFO", f"Validation accuracy: {val_acc:.4f}")
doc.add_message("INFO", f"Test accuracy: {test_acc:.4f}")
doc.add_message("INFO", f"Model saved to {model_path}")
doc.add_message("INFO", "Model training pipeline completed successfully")

# --------------------------------------------------------------------------- #
# Step 5: Finalise & Persist                                                  #
# --------------------------------------------------------------------------- #
# finalise() performs several important operations:
#   1. Records finalization_dtg (current UTC timestamp)
#   2. Computes total_runtime_seconds from init to finalization
#   3. Generates a unique run_id (UUID v4)
#   4. Computes input_hash from the inputs dict
#   5. Computes model_file_hash from model_file_path (SHA-256 of the file)
#   6. Captures execution_environment (Python version, libraries, OS, hardware)
#   7. Validates the complete document against the output schema
#   8. Marks the document as immutable (no further add_message() calls)

log.info("Finalising document (computing hashes, validating schema)...")
try:
    doc.finalise()
except SchemaError as exc:
    # SchemaError is raised if validation fails
    raise SystemExit(f"Manifest failed validation:\n{exc}") from exc

# After finalise(), these auto-computed fields are available:
log.info("Auto-computed fields:")
log.info("  - run_id: %s", doc["run_id"])
log.info("  - initialization_dtg: %s", doc["initialization_dtg"])
log.info("  - finalization_dtg: %s", doc["finalization_dtg"])
log.info("  - total_runtime_seconds: %d", doc["total_runtime_seconds"])
log.info("  - input_hash: %s", doc["input_hash"][:16] + "...")
log.info("  - model_file_hash: %s", doc["model_file_hash"][:16] + "...")

# Save to JSON file
json_path = Path("iris_model_manifest.json")
doc.save(json_path)
log.info("JSON manifest written to %s", json_path.resolve())

# Generate and save Markdown report using the card helper
# to_markdown_card() converts the document to a human-readable format
md_path = Path("iris_model_report.md")
md_path.write_text(to_markdown_card(doc), encoding="utf-8")
log.info("Markdown report written to %s", md_path.resolve())

# --------------------------------------------------------------------------- #
# Summary                                                                     #
# --------------------------------------------------------------------------- #
log.info("=" * 70)
log.info("Example completed successfully!")
log.info("This example demonstrated:")
log.info("  1. Loading and inspecting a contract schema")
log.info("  2. Parsing and validating inputs")
log.info("  3. Training a model with train/validation/test splits")
log.info("  4. Building a manifest with ALL required and optional fields")
log.info("  5. Adding structured log messages")
log.info("  6. Finalising with auto-computed hashes and metadata")
log.info("  7. Exporting to JSON and Markdown formats")
log.info("=" * 70)
