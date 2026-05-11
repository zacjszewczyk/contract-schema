# Contract Schema

Contract Schema is a lightweight Python package for schema-driven, structured inputs and outputs.

The contract language is intentionally compact, but it now supports first-class validation for:

* regex-constrained scalar values via `pattern`
* list cardinality via `minItems`
* object cardinality via `minProperties`
* dynamic object maps via `additionalProperties` schemas and `propertyNamesPattern`
* strict ISO calendar dates via `format: date`
* MITRE-style timestamps with either `T` or space separators via `format: mitre-date-time`

## Table of Contents

* [**Description**](#description)
* [**Dependencies**](#dependencies)
* [**Installation**](#installation)
* [**Usage**](#usage)
* [**Project structure**](#project-structure)
* [**Background and Motivation**](#background-and-motivation)
* [**Contributing**](#contributing)
* [**Contributors**](#contributors)
* [**License**](#license)

## Description

Contract Schema unifies disparate I/O layers under one authoritative contract. A single, versioned JSON file defines both the permissible inputs and the required outputs; the library takes care of CLI generation, default handling, deep validation, and rich, provenance-aware output documents.

* **Input parsing:** Every field in the contract automatically becomes an `argparse` flag, a JSON key, and a CLI flag, with type enforcement, enums, regex checks, and sensible defaults injected.  
* **Output construction:** The same contract defines the structure of your result document. A helper class injects run metadata, hashes inputs and findings for auditability, and captures execution environment details.  
* **Meta-schema validation:** A bundled _meta-schema_ ensures every contract you write contains the minimal top-level keys (`title`, `version`, `description`, `input`, `output`) and that both `input` and `output` sections themselves declare a `fields` block. Contract mistakes are caught at load time.  
* **No heavy dependencies:** Only the Python standard library (>= 3.8) is required. `pandas` is optional and used only when you pass a DataFrame directly as an input.  

The result is a single source of truth for I/O that works anywhere Python runs: CI pipelines, air-gapped analysis workstations, or serverless functions.

## Dependencies

* **Required:** Python >= 3.8  

No other external packages are needed.

## Installation

```
pip install contract-schema
```

## Usage

Contract Schema supports two primary workflows: **analytics** (security/data analysis pipelines) and **models** (machine learning model training manifests). Both follow the same pattern:

1. Load a contract from a bundled JSON schema
2. Parse and validate inputs against the contract
3. Build an output document that conforms to the contract
4. Finalize and save the document

### Quick Start

```python
from contract_schema import Contract

# Load the analytic contract (bundled with the package)
contract = Contract.load("analytic_schema.json")

# Parse and validate inputs (from dict, JSON file, or CLI args)
inputs = contract.parse_and_validate_input({
    "start_dtg": "2025-01-01T00:00:00Z",
    "end_dtg": "2025-01-02T00:00:00Z",
    "data_source_type": "file",
    "data_source": "/path/to/data.csv",
})

# Create and populate an output document
doc = contract.create_document(
    input_schema_version=contract.version,
    output_schema_version=contract.version,
    author="Your Name",
    author_organization="Your Org",
    contact="you@example.com",
    license="MIT",
    documentation_link="https://example.com",
    status="success",
    exit_code=0,
    inputs=inputs,
    # ... additional required fields from the contract
)

# Finalize (validates output, computes hashes, captures environment)
doc.finalise()

# Save to file
doc.save("output_report.json")
```

### Input Sources

Inputs can be provided in multiple formats:

```python
# From a Python dict
inputs = contract.parse_and_validate_input({"key": "value"})

# From a JSON file path
inputs = contract.parse_and_validate_input("/path/to/config.json")

# From a JSON string
inputs = contract.parse_and_validate_input('{"key": "value"}')

# From CLI arguments
inputs = contract.parse_and_validate_input([
    "--start-dtg", "2025-01-01T00:00:00Z",
    "--end-dtg", "2025-01-02T00:00:00Z",
])

# From sys.argv (default when None is passed)
inputs = contract.parse_and_validate_input(None)
```

### Bundled Contracts

The package includes three production-ready contracts:

- **`analytic_schema.json`** - For security analytics and data analysis pipelines. Includes fields for findings, MITRE ATT&CK mappings, and observables.
- **`model_schema.json`** - For ML model training manifests. Includes fields for metrics, hyperparameters, and model artifacts.
- **`analytic_results_cim.json`** - Common Information Model for analytic *results* in tabular storage. The output document carries a `results` list whose row grain is `analytic_run_id + result_id + observation_window + primary_entity + tactic_id + technique_id`. One analytic mapped to three techniques emits three rows. The schema spans host, network, cloud, identity, OT, container, and hybrid analytics under one entity model (`primary_entity_*` plus typed supporting entity fields), keeps **score**, **likelihood**, **severity**, and **confidence** as separate concerns, and stores bounded evidence (counts, references, JSON-encoded feature blobs) rather than raw events. Documents finalised against this contract auto-emit a `results_hash` for tamper-evidence.

The package also includes **`analytic_plans.json`** for validating analytic plan JSON arrays such as those stored under a `techniques/` directory. It is intended for contract-driven validation of plan content rather than execution manifests.

All bundled contracts share the same contract language and validator, including support for dynamic MITRE ID maps (`AN####`, `DET####`), regex-constrained scalars (e.g. `^TA[0-9]{4}$` for tactic IDs and `^T[0-9]{4}(\.[0-9]{3})?$` for technique IDs in the CIM), list/object cardinality checks, and flexible MITRE timestamp parsing where needed.

### Analytic Results CIM (`analytic_results_cim.json`)

The CIM standardises how analytic results are written into tabular storage so that host, network, cloud, identity, OT, container, and hybrid analytics can share one table (`analytic_results_cim`) and one set of dashboards.

**Row grain.** One row = one analytic finding for one analytic run, one observation window, one primary subject, and one MITRE tactic-technique relationship. If an analytic maps to three techniques, emit three rows; if a finding involves multiple entities, choose a `primary_entity_*` and place the rest in supporting entity fields (`src_*`, `dst_*`, `cloud_*`, `ot_*`, `container_*`, `kubernetes_*`).

**Field families.**

- *Provenance:* `analytic_id`, `analytic_name`, `analytic_version`, `analytic_path`, `analytic_url`, `analytic_repo_url`, `analytic_branch`, `analytic_commit_hash`, `analytic_config_hash`, `analytic_engine`, `input_dataset_names` (ARRAY VARCHAR of source table names).
- *MITRE mapping (row-level, not analytic-level):* `mitre_attack_domain`, `mitre_tactic_id` (regex `^TA[0-9]{4}$`), `mitre_tactic_name`, `mitre_technique_id` (regex `^T[0-9]{4}(\.[0-9]{3})?$`, store the most specific form), `mitre_subtechnique_*`.
- *Assessment (kept separate on purpose):* `result_score` + `result_score_type` + `result_threshold` are model-native; `result_severity` is analyst-assigned impact; `behavior_likelihood` is "how likely malicious"; `analytic_confidence` is "how sufficient the evidence is". A result can be highly suspicious but low confidence because logs are incomplete, or moderate likelihood but high confidence because the evidence is deterministic.
- *Environment / data source:* `environment_domain`, `site_name`, `datasource_vendor`, `datasource_product`, `log_source_type`. `primary_entity_type` names which sub-bucket under `results` is the subject.
- *Typed observation buckets (NEW in 1.2.0):* every row carries a `results` object with optional sub-objects `host`, `net`, `cloud`, `ot`. Each sub-object groups the entity / observation fields for that domain. The `host` bucket carries hostname / IP / user **plus** the process information the analytic produced (`process_name`, `process_path`, `process_hash`, `process_id`, `process_guid`, `process_command_line`, `parent_process_*`, `target_process_*`). Buckets that do not apply are omitted. `results_count` is an integer 0-4 reporting how many domains the finding actually touched.
- *Evidence (bounded, not raw logs):* `result_count` (number of result rows emitted by the analytic for this run), `evidence_first_seen_utc`, `evidence_last_seen_utc`, `evidence_query`.

**Wrapper document.** Each finalised document carries `cim_schema_name`, `cim_schema_version`, run metadata, the `results` list, and a `results_hash` (auto-computed by `Document.finalise()` from the canonicalised `results` array, mirroring how `findings_hash` works on `analytic_schema.json`).

**Quick example.**

```python
from contract_schema import Contract

C = Contract.load("analytic_results_cim.json")
inputs = C.parse_and_validate_input({
    "start_dtg": "2026-05-11T13:00:00Z",
    "end_dtg":   "2026-05-11T14:00:00Z",
    "data_source_type": "file",
    "data_source": "/data/sysmon.csv",
})

row = {
    "result_id": "ar_9f03c",
    "analytic_run_id": "run_20260511T141522Z_7ac9",
    "dtg_utc": "2026-05-11T14:15:22Z",
    "observation_start_utc": "2026-05-11T13:00:00Z",
    "observation_end_utc":   "2026-05-11T14:00:00Z",
    "run_username": "zachary.szewczyk",
    "analytic_id": "scheduled_task_creation",
    "analytic_name": "Suspicious Scheduled Task Creation",
    "analytic_description": "Identifies suspicious scheduled task creation with anomalous command content.",
    "analytic_version": "1.0.0",
    "mitre_attack_domain": "enterprise",
    "mitre_tactic_id": "TA0002",
    "mitre_tactic_name": "Execution",
    "mitre_technique_id": "T1053.005",
    "mitre_technique_name": "Scheduled Task/Job: Scheduled Task",
    "result_severity": "high",
    "behavior_likelihood": "likely",
    "analytic_confidence": "moderate",
    "environment_domain": "host",
    "primary_entity_type": "host",
    "results": {
        "host": {
            "hostname": "WS-1042",
            "user": "CORP\\jsmith",
            "process_name": "schtasks.exe",
            "process_path": "C:\\Windows\\System32\\schtasks.exe",
            "process_command_line": "schtasks /create /tn ... /tr powershell -enc ...",
        },
    },
    "results_count": 1,
    "result_score": 0.91,
    "result_score_type": "rule_score",
    "result_count": 1,
}

doc = C.create_document(
    cim_schema_name="analytic_results_cim",
    cim_schema_version="1.0.0",
    input_schema_version=C.version,
    output_schema_version=C.version,
    inputs=inputs,
    results=[row],
    status="success", exit_code=0,
    author="Your Name", author_organization="Your Org",
    contact="you@example.com", license="MIT",
    documentation_link="https://example.com",
)
doc.finalise()                  # validates + computes results_hash
doc.save("cim_results.json")
```

### Creating Custom Contracts

Custom contracts must conform to the bundled meta-schema (`contract_meta_schema.json`), which requires:

- `title`, `version`, `description` at the top level
- `input` and `output` objects, each containing a `fields` object

See `example_analytic.py` and `example_model.py` for complete working examples. See `example_analytic.ipynb` for an interactive example of the analytic schema in action.

## Project structure

```
contract-schema/        # Repository root
|__ README.md            # This file
|
|__ contract_schema/     # Python package
|   |__ __init__.py
|   |__ contract.py      # High-level Contract class
|   |__ document.py      # Schema-aware Document builder
|   |__ loader.py        # JSON loader with resource fallback
|   |__ parser.py        # CLI / JSON / Mapping input parser
|   |__ validator.py     # Lightweight JSON-schema validator
|   |__ utils.py         # Shared helpers (hashing, timestamps, etc.)
|   |__ schemas/         # Bundled contracts
|       |__ analytic_schema.json
|       |__ analytic_results_cim.json
|       |__ analytic_plans.json
|       |__ analytic_plans_d3fend.json
|       |__ model_schema.json
|       |__ contract_meta_schema.json
|
|__ example_analytic.py  # End-to-end demo script for the analytic contract
|__ example_analytic.ipynb # End-to-end demo notebook for the analytic contract
|__ example_model.py  # End-to-end demo script for the model contract
|
|__ tests/               # Unit tests
|   |__ analytic/
|   |__ model/
|   |__ meta/
|
|__ makefile             # Project makefile
|__ LICENSE.md           # License
|__ pyproject.toml       # Build metadata
```

## Background and Motivation

Security analytics, ML pipelines, and data engineering jobs often reinvent the wheel for argument parsing and result emission. Over time, field names diverge, validation drifts, and downstream systems break.

Contract Schema solves this by treating the contract itself as code--version-controlled, validated, and consumed at runtime.

* Uniformity - All tools speak the same language defined by the contract.
* Reliability - Inputs and outputs are validated deeply; failures happen fast and loudly.
* Traceability - Documents include run IDs, environment snapshots, and SHA-256 hashes of both inputs and outputs.
* Extensibility - Write new contracts (e.g., for data ingestion) and they instantly get CLI generation, validation, and output helpers-no new code needed.

## Contributing

Contributions are welcome from all, regardless of rank or position.

There are no system requirements for contributing to this project. To contribute via the web:

1. Click GitLab's "Web IDE" button to open the online editor.
2. Make your changes. **Note:** limit your changes to one part of one file per commit; for example, edit only the "Description" section here in the first commit, then the "Background and Motivation" section in a separate commit.
3. Once finished, click the blue "Commit..." button.
4. Write a detailed description of the changes you made in the "Commit Message" box.
5. Select the "Create a new branch" radio button if you do not already have your own branch; otherwise, select your branch. The recommended naming convention for new branches is ``first.middle.last``.
6. Click the green "Commit" button.

You may also contribute to this project using your local machine by cloning this repository to your workstation, creating a new branch, committing and pushing your changes, and creating a merge request.

## Contributors

This section lists project contributors. When you submit a merge request, remember to append your name to the bottom of the list below. You may also include a brief list of the sections to which you contributed.

* **Creator:** Zachary Szewczyk

## License

This project is licensed under the [Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License](https://creativecommons.org/licenses/by-nc-sa/4.0/). You can view the full text of the license in [LICENSE.md](./LICENSE.md). Read more about the license [at the original author's website](https://zacs.site/disclaimers.html). Generally speaking, this license allows individuals to remix this work provided they release their adaptation under the same license and cite this project as the original, and prevents anyone from turning this work or its derivatives into a commercial product.
