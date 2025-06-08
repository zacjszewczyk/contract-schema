# Analytic Schema

Analytic Schema is a lightweight Python package for loading, validating, and building standardized I/O documents for analytics notebooks based on a single JSON contract.

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

Analytic Schema provides a simple, dependency-light framework for cybersecurity analytics: you define a single JSON contract that specifies input and output fields, types, defaults, and constraints, and the library generates CLI flags, parses inputs (CLI, JSON dict, or file), injects defaults, performs deep validation, and builds validated output documents with structured logging and hashing. No third-party dependencies beyond `pandas`, perfect for air-gapped notebooks and pipelines.

## Dependencies

This project depends only on the Python standard library (>=3.8) and pandas (>=1.0).

## Installation

\`\`\`bash
pip install analytic-schema
\`\`\`

In your code:

\`\`\`python
import analytic_schema
\`\`\`

## Usage

\`\`\`python
from analytic_schema import parse_input, validate_input, OutputDoc

# 1) Parse raw inputs (CLI, JSON string, dict, or file)
raw = parse_input(“—input-schema-version 1.0.0 “
                  “—start-dtg 2025-06-01T00:00:00Z “
                  “—end-dtg   2025-06-02T00:00:00Z “
                  “—data-source-type file “
                  “—data-source /tmp/log.csv”)

# 2) Validate and inject defaults
params = validate_input(raw)

# 3) Run your analytic logic...
#    (omitted)

# 4) Build structured output
out = OutputDoc(input_data_hash=“abcd1234...”, inputs=params)
out.add_message(“INFO”, “Analysis complete”)
out[“records_processed”] = 123
out[“findings”] = [...]
out.finalise()
out.save(“output.json”)
\`\`\`

## Project structure

\`\`\`
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
\`\`\`

## Background and Motivation

In cybersecurity analytics and data processing, there’s a growing need for consistent, auditable, and interoperable I/O across diverse scripts and notebooks. By encoding your contract in a single JSON schema, Analytic Schema automates CLI generation, default handling, validation, and output construction—ensuring your pipelines are robust, maintainable, and easy to integrate without pulling in heavy dependencies.

## Contributing

Contributions are welcome from all, regardless of rank or position.

There are no system requirements for contributing to this project. To contribute via the web:

1. Click GitLab’s “Web IDE” button to open the online editor.
2. Make your changes. **Note:** limit your changes to one part of one file per commit; for example, edit only the “Description” section here in the first commit, then the “Background and Motivation” section in a separate commit.
3. Once finished, click the blue “Commit...” button.
4. Write a detailed description of the changes you made in the “Commit Message” box.
5. Select the “Create a new branch” radio button if you do not already have your own branch; otherwise, select your branch. The recommended naming convention for new branches is `first.middle.last`.
6. Click the green “Commit” button.

You may also contribute to this project using your local machine by cloning this repository to your workstation, creating a new branch, commiting and pushing your changes, and creating a merge request.

## Contributors

This section lists project contributors. When you submit a merge request, remember to append your name to the bottom of the list below. You may also include a brief list of the sections to which you contributed.

* **Creator:** Zachary Szewczyk

## License

This project is licensed under the [Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License](https://creativecommons.org/licenses/by-nc-sa/4.0/). You can view the full text of the license in [LICENSE.md](./LICENSE.md). Read more about the license [at the original author’s website](https://zacs.site/disclaimers.html). Generally speaking, this license allows individuals to remix this work provided they release their adaptation under the same license and cite this project as the original, and prevents anyone from turning this work or its derivatives into a commercial product.