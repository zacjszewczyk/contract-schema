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

Check out `example_analytic.py` and `example_model.py` for detailed examples of Contract Schema in action.

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

**Required**. This section should include a boilerplate summary of the license under which the project is published. For Information Defense company projects, this should be the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License; use the paragraph below:

This project is licensed under the [Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License](https://creativecommons.org/licenses/by-nc-sa/4.0/). You can view the full text of the license in [LICENSE.md](./LICENSE.md). Read more about the license [at the original author's website](https://zacs.site/disclaimers.html). Generally speaking, this license allows individuals to remix this work provided they release their adaptation under the same license and cite this project as the original, and prevents anyone from turning this work or its derivatives into a commercial product.
