# Analytic Schema

Analytic Schema is a lightweight Python package for loading, validating, and building standardized I/O documents for analytics notebooks based on a single, versioned JSON contract that drives both input parsing and output construction.

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

Analytic Schema centralizes your notebook or script I/O definitions into one authoritative JSON file. From that contract it automatically:

- Generates a complete command-line interface (via `argparse`) for every input field.  
- Parses parameters provided as CLI flags, JSON dictionaries, or JSON files.  
- Injects sensible defaults for all optional inputs.  
- Performs deep schema validation (type checks, enums, date-time formats, oneOf branches, and no extra fields).  
- Builds structured output documents with embedded metadata, SHA-256 hashes for auditability, and a built-in logging mechanism.  

With zero dependencies beyond the standard library and `pandas`, Analytic Schema is ideal for air-gapped notebooks, CI pipelines, or any environment where you need a robust, self-contained I/O layer for cybersecurity analytics.

## Dependencies

This project depends only on the Python standard library (>=3.8) and `pandas` (>=1.0).

## Installation

```
pip install analytic-schema
```

In your code:

```
import analytic_schema
```

## Usage

Check out [`example_usage.py`](./example_usage.py) for a guided walkthrough. That script demonstrates how Analytic Schema handles all the I/O boilerplate, to include CLI parsing, default injection, validation, metadata, logging, hashing, and final serialization so you can focus on the core analytic logic.

## Project structure

```
analytic-schema/ # Project repository
├── analytic_schema/ # Package
│   ├── __init__.py
│   ├── loader.py
│   ├── parser.py
│   ├── validator.py
│   ├── output.py
│   └── analytic_schema.json
│
├── tests/
│   └── test_analytic_schema.py
│
├── example_usage.py
│
├── README.md      # This file
├── LICENSE.md     # Project license
├── Makefile       # Project makefile
└── pyproject.toml
```

## Background and Motivation

In cybersecurity analytics, consistency and auditability are paramount. Analysts and automation pipelines often spin up dozens of scripts and notebooks, each rolling its own argument parsing, validation, and output formatting. This fragmentation leads to subtle bugs, schema drift, and integration headaches.

Analytic Schema addresses these challenges by elevating your I/O contract to one single JSON schema. This contract drives:

- **Uniformity**: All analytics share the same field names, types, and defaults.  
- **Reliability**: Fail-fast validation prevents runtime surprises from missing or mistyped parameters.  
- **Traceability**: Inputs and findings are hashed, and logs are captured inline, enabling full audit trails.  
- **Simplicity**: With only the standard library plus `pandas`, it works in air-gapped environments and keeps your dependencies minimal.  

By abstracting away boilerplate, you can focus on detecting and investigating threats, while ensuring your pipelines remain robust, maintainable, and easily integrated.

## Contributing

Contributions are welcome from all, regardless of rank or position.

There are no system requirements for contributing to this project. To contribute via the web:

1. Click GitLab’s “Web IDE” button to open the online editor.
2. Make your changes. **Note:** limit your changes to one part of one file per commit; for example, edit only the “Description” section here in the first commit, then the “Background and Motivation” section in a separate commit.
3. Once finished, click the blue “Commit...” button.
4. Write a detailed description of the changes you made in the “Commit Message” box.
5. Select the “Create a new branch” radio button if you do not already have your own branch; otherwise, select your branch. The recommended naming convention for new branches is `first.middle.last`.
6. Click the green “Commit” button.

You may also contribute to this project using your local machine by cloning this repository to your workstation, creating a new branch, committing and pushing your changes, and creating a merge request.

## Contributors

This section lists project contributors. When you submit a merge request, remember to append your name to the bottom of the list below. You may also include a brief list of the sections to which you contributed.

* **Creator:** Zachary Szewczyk

## License

This project is licensed under the [Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License](https://creativecommons.org/licenses/by-nc-sa/4.0/). You can view the full text of the license in [LICENSE.md](./LICENSE.md). Read more about the license [at the original author’s website](https://zacs.site/disclaimers.html). Generally speaking, this license allows individuals to remix this work provided they release their adaptation under the same license and cite this project as the original, and prevents anyone from turning this work or its derivatives into a commercial product.