#!/usr/bin/env python
"""
example_analytic.py
===================
Comprehensive demonstration of the **contract_schema** library using the
packaged **analytic_schema.json** contract.

This example demonstrates:
--------------------------
1. **Contract Loading**: How to load and access contract metadata
2. **Input Parsing & Validation**: Multiple ways to parse inputs (dict, CLI args)
3. **Document Creation**: Setting all required and optional output fields
4. **Message Logging**: Using add_message() for structured execution logging
5. **Finding Generation**: Creating findings with all required fields
6. **Finalisation**: Auto-computed fields (hashes, timestamps, environment)
7. **Export**: Saving documents as JSON and Markdown reports

Schema Fields Demonstrated
--------------------------
**Input Schema (all fields exercised):**
- start_dtg (required): UTC timestamp marking data window start
- end_dtg (required): UTC timestamp marking data window end
- data_source_type (required): Transport mechanism (file/IONIC/api)
- data_source (required): Path, identifier, or URL for data
- log_path (optional): Where to write execution logs
- output (optional): Destination for findings output
- analytic_parameters (optional): Analytic-specific tuning knobs
- data_map (optional): Field mapping for non-SchemaONE data
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
- analytic_id, analytic_name, analytic_version, analytic_description
- findings, findings_hash (auto-computed)

Optional fields:
- contributors: List of project contributors
- messages: Structured log entries via add_message()
- additional_run_properties: Custom metadata for the run
"""
from __future__ import annotations

from pathlib import Path
import uuid
import logging

import pandas as pd
from sklearn.datasets import load_iris

from contract_schema import Contract, utils, to_markdown_card

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

log.info("Loading the analytic contract schema...")
C = Contract.load("analytic_schema.json")

# Access contract metadata for documentation or versioning purposes
log.info("Contract: %s (v%s)", C.title, C.version)
log.info("Description: %s", C.description)

# --------------------------------------------------------------------------- #
# Step 2: Build & Validate Analytic Inputs                                    #
# --------------------------------------------------------------------------- #
# The parse_and_validate_input() method supports multiple input formats:
#   - dict/Mapping: Direct Python dictionary
#   - Path: JSON file on disk
#   - str: File path, JSON literal, or CLI argument string
#   - Sequence[str]: CLI tokens (simulating sys.argv)
#   - None: Defaults to sys.argv[1:]
#
# It performs:
#   1. Parsing the source into a dict
#   2. Dereferencing JSON file paths in values
#   3. Injecting schema-defined defaults
#   4. Deep validation against the input schema

log.info("Preparing analytic input parameters...")
now_iso = utils._now_iso()
iris = load_iris(as_frame=True)
df: pd.DataFrame = iris.frame

# Demonstrate setting ALL input fields (required + optional)
inputs = C.parse_and_validate_input(
    {
        # --- Required fields ---------------------------------------------------
        "start_dtg": now_iso,  # Inclusive UTC timestamp (ISO 8601) for data window start
        "end_dtg": now_iso,  # Exclusive UTC timestamp (ISO 8601) for data window end
        "data_source_type": "file",  # Transport mechanism: "file", "IONIC", or "api"
        "data_source": "iris.frame",  # Path/identifier/URL for the dataset

        # --- Optional fields (with defaults shown) -----------------------------
        "log_path": "stdout",  # (optional) Where to write execution logs; default: "stdout"
        "output": "stdout",  # (optional) Destination for findings; default: "stdout"
        "analytic_parameters": {  # (optional) Analytic-specific tuning knobs; default: {}
            "min_class_count": 10,  # Example custom parameter
            "include_summary": True,  # Example custom parameter
        },
        "data_map": {},  # (optional) Field mapping for non-SchemaONE data; default: {}
        "verbosity": "INFO",  # (optional) Log level: DEBUG/INFO/WARN/ERROR/FATAL; default: "INFO"
    }
)
log.info("Input validation successful. Fields: %s", list(inputs.keys()))

# --------------------------------------------------------------------------- #
# Step 3: Derive Findings                                                     #
# --------------------------------------------------------------------------- #
# Each finding must include all required fields as defined in the schema.
# Findings represent the analytic's output observations/detections.

log.info("Generating findings from dataset analysis...")
class_counts = df["target"].value_counts().to_dict()

# Create a finding with ALL required fields documented
findings = [
    {
        # Unique identifier for this finding (UUID v4 recommended)
        "finding_id": str(uuid.uuid4()),
        # Concise summary of what was detected
        "title": "Class distribution summary",
        # Detailed explanation of the detection and its significance
        "description": (
            f"The dataset contains {len(df)} samples distributed across "
            f"{len(class_counts)} classes. Counts per class: {class_counts}. "
            "Classes are balanced with 50 samples each."
        ),
        # UTC timestamp of the primary event triggering this finding
        "event_dtg": utils._now_iso(),
        # Impact level: low, medium, high, critical
        "severity": "low",
        # Probability the finding is valid: percentage or qualitative scale
        "confidence": "high",
        # Concrete artifacts associated with the finding (IPs, hashes, etc.)
        "observables": list(map(str, class_counts.keys())),
        # MITRE ATT&CK tactic identifiers (empty for non-security analytics)
        "mitre_attack_tactics": [],
        # MITRE ATT&CK technique identifiers (empty for non-security analytics)
        "mitre_attack_techniques": [],
        # Guidance for response or investigation
        "recommended_actions": "None – informational only.",
        # Suggested data sources or tools for additional context
        "recommended_pivots": "N/A",
        # Data handling classification: U (Unclassified), CUI, etc.
        "classification": "U",
    }
]
log.info("Generated %d finding(s)", len(findings))

# --------------------------------------------------------------------------- #
# Step 4: Build the Analytic Output Document                                  #
# --------------------------------------------------------------------------- #
# The Document class is a dict subclass that:
#   - Tracks the output schema for validation
#   - Auto-populates initialization_dtg on creation
#   - Provides add_message() for structured logging
#   - Computes hashes and environment info on finalise()

log.info("Creating output document...")
doc = C.create_document(
    # =========================================================================
    # PROVENANCE & AUTHORSHIP
    # =========================================================================
    # Version of the input contract used for this run
    input_schema_version="1.0.1",
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
        "Alice Smith": "Data preprocessing and validation",
        "Bob Jones": "Code review and testing",
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
        "Fisher's Iris flower data set (1936) containing 150 samples of iris "
        "flowers with 4 features each. Treated as row-level events for this "
        "demonstration analytic. Features include sepal length/width and "
        "petal length/width measurements."
    ),
    # Total number of rows in the dataset
    dataset_size=len(df),
    # SHA-256 hash of the dataset for integrity verification
    dataset_hash=utils._hash(df),
    # Schema mapping feature names to their data types
    data_schema={**{c: "number" for c in iris.feature_names}, "target": "integer"},
    # Ordered list of feature names used in analysis
    feature_names=iris.feature_names,

    # =========================================================================
    # ANALYTIC METADATA
    # =========================================================================
    # Validated input parameters (copy for auditability)
    inputs=inputs,
    # Canonical identifier or path of the analytic script
    analytic_id=str(Path(__file__).resolve()),
    # Human-readable name of the analytic
    analytic_name="Iris class distribution",
    # Semantic version of this analytic
    analytic_version="0.1.0",
    # Brief description of the analytic's purpose
    analytic_description=(
        "Demonstration analytic that summarises the distribution of species "
        "in the Iris data set. It computes class counts and verifies class "
        "balance for the downstream classification task."
    ),

    # =========================================================================
    # FINDINGS
    # =========================================================================
    # List of findings generated by the analytic
    findings=findings,

    # =========================================================================
    # OPTIONAL: Additional Run Properties (custom metadata for this run)
    # =========================================================================
    additional_run_properties={  # (optional) Custom key-value metadata; default: not included
        "class_counts": class_counts,
        "ci_job_url": "https://example.com/ci/job/12345",
        "git_commit": "abc123def456",
        "ticket_id": "JIRA-1234",
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
doc.add_message("INFO", "Analytic execution started")
doc.add_message("DEBUG", f"Loaded {len(df)} records from iris.frame")
doc.add_message("INFO", f"Analysing {len(class_counts)} distinct classes")
doc.add_message("INFO", f"Generated {len(findings)} finding(s)")
doc.add_message("INFO", "Analytic execution completed successfully")

# --------------------------------------------------------------------------- #
# Step 5: Finalise & Persist                                                  #
# --------------------------------------------------------------------------- #
# finalise() performs several important operations:
#   1. Records finalization_dtg (current UTC timestamp)
#   2. Computes total_runtime_seconds from init to finalization
#   3. Generates a unique run_id (UUID v4)
#   4. Computes input_hash from the inputs dict
#   5. Computes findings_hash from the findings list
#   6. Captures execution_environment (Python version, libraries, OS, hardware)
#   7. Validates the complete document against the output schema
#   8. Marks the document as immutable (no further add_message() calls)

log.info("Finalising document (computing hashes, validating schema)...")
doc.finalise()

# After finalise(), these auto-computed fields are available:
log.info("Auto-computed fields:")
log.info("  - run_id: %s", doc["run_id"])
log.info("  - initialization_dtg: %s", doc["initialization_dtg"])
log.info("  - finalization_dtg: %s", doc["finalization_dtg"])
log.info("  - total_runtime_seconds: %d", doc["total_runtime_seconds"])
log.info("  - input_hash: %s", doc["input_hash"][:16] + "...")
log.info("  - findings_hash: %s", doc["findings_hash"][:16] + "...")

# Save to JSON file
json_path = Path("iris_analytic_report.json")
doc.save(json_path)
log.info("JSON output written to %s", json_path.resolve())

# Generate and save Markdown report using the card helper
# to_markdown_card() converts the document to a human-readable format
md_path = Path("iris_analytic_report.md")
md_path.write_text(to_markdown_card(doc), encoding="utf-8")
log.info("Markdown report written to %s", md_path.resolve())

# --------------------------------------------------------------------------- #
# Summary                                                                     #
# --------------------------------------------------------------------------- #
log.info("=" * 70)
log.info("Example completed successfully!")
log.info("This example demonstrated:")
log.info("  1. Loading and inspecting a contract schema")
log.info("  2. Parsing and validating inputs with all fields")
log.info("  3. Creating findings with all required fields")
log.info("  4. Building a document with required and optional fields")
log.info("  5. Adding structured log messages")
log.info("  6. Finalising with auto-computed hashes and metadata")
log.info("  7. Exporting to JSON and Markdown formats")
log.info("=" * 70)
