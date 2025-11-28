# Contract Schema

Contract Schema is a lightweight Python package for schema-driven, structured inputs and outputs.

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

* **Input parsing:** Every field in the contract automatically becomes an `argparse` flag, a JSON key, and a CLI flag, with type enforcement, enums, and sensible defaults injected.  
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

The package includes two production-ready contracts:

- **`analytic_schema.json`** - For security analytics and data analysis pipelines. Includes fields for findings, MITRE ATT&CK mappings, and observables.
- **`model_schema.json`** - For ML model training manifests. Includes fields for metrics, hyperparameters, and model artifacts.

Both contracts share common metadata fields like execution environment, timestamps, and provenance hashes.

### Creating Custom Contracts

Custom contracts must conform to the bundled meta-schema (`contract_meta_schema.json`), which requires:

- `title`, `version`, `description` at the top level
- `input` and `output` objects, each containing a `fields` object

See `example_analytic.py` and `example_model.py` for complete working examples.

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
|       |__ model_schema.json
|       |__ contract_meta_schema.json
|
|__ example_analytic.py  # End-to-end demo script for the analytic contract
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

To contribute to this project:

1. Fork the repository on GitHub.
2. Create a new branch for your changes (recommended naming: `feature/description` or `fix/description`).
3. Make your changes. **Note:** limit your changes to one part of one file per commit for easier review.
4. Write a detailed description of the changes you made in the commit message.
5. Push your branch and create a Pull Request.

You may also contribute by opening issues to report bugs or request features.

## Contributors

This section lists project contributors. When you submit a Pull Request, remember to append your name to the bottom of the list below. You may also include a brief list of the sections to which you contributed.

* **Creator:** Zachary Szewczyk

## License

This project is licensed under the [Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License](https://creativecommons.org/licenses/by-nc-sa/4.0/). You can view the full text of the license in [LICENSE.md](./LICENSE.md). Read more about the license [at the original author's website](https://zacs.site/disclaimers.html). Generally speaking, this license allows individuals to remix this work provided they release their adaptation under the same license and cite this project as the original, and prevents anyone from turning this work or its derivatives into a commercial product.
