# Model Schema

Model Schema is a lightweight Python package that supports machine learning model provenance and reproducibility by helping to construct descriptive model manifests based on a single, versioned JSON contract.  

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

Based on the Model Card concept proposed in 2018 in Mitchell et. al’s [*Model Cards for Model Reporting*](https://arxiv.org/abs/1810.03993) paper, Model Schema centralizes your model artifact's metadata in one authoritative JSON file. From that contract it automatically:

- Builds an incremental manifest so you can attach metrics, dataset hashes, and hyper-parameters as they become available.  
- Collects the complete execution environment to include Python version, installed libraries, OS, hardware specs, user, and host.  
- Computes SHA-256 hashes for the full dataset, each split, and the final model file for tamper-evidence and reproducibility.  
- Performs deep schema validation (type checks, enums, ISO-8601 date-times, oneOf branches, and no extra fields).  
- Serializes the finished manifest—or a collection of manifests from hyper-parameter sweeps—to pretty-printed JSON ready for CI pipelines, model registries, or downstream consumers.
- Serializes any picklable model under a predictable filename (`<model_type>_<dtg>.pkl`).  

With zero dependencies beyond the standard library, Model Schema is ideal for air-gapped training clusters, on-prem machine learning operations (MLOps) pipelines, or any environment that needs a robust, self-contained provenance layer for models.

## Dependencies

This project depends only on the Python standard library. This project requires Python 3.8 or higher.

## Installation

```
pip install model-schema
```

In your code:

```
import model_schema as msc
```

## Usage

Check out `example.py` for a guided walkthrough. That script trains several models, serializes each one, and emits a single JSON file containing every manifest.

## Project structure

```
model-schema/            # Project repository
├── model_schema/        # Package
│   ├── __init__.py
│   ├── model_schema.py  # Loader, validator, ModelManifest helper
│   ├── output.py        # Incremental save helpers (optional extras)
│   └── model_schema.json# Single source-of-truth contract
│
├── tests/
│   └── test_model_schema.py
│
├── example.py
│
├── README.md            # This file
├── LICENSE.md           # Project license
├── Makefile             # Build & packaging helpers
└── pyproject.toml
```

## Background and Motivation

In machine learning, reproducibility and provenance are paramount. Months after a model is deployed you may need to:

* Reproduce that model.
* Replicate certain aspects of that model generation process with improvements in select areas.
* Re-train on new data with identical hyper-parameters.
* Replicate the environment that produced the original model.
* Compare current performance with historic baselines.

Without detailed documentation, these tasks become guess-work. Model Schema solves that by elevating model metadata to a first-class, contract-driven artefact:

* Every model export shares the same field names, types, and defaults.
* Validation prevents missing or malformed metadata.
* Hashes across inputs, splits, and the model file itself enable full audit trails.
* Pure stdlib design keeps dependencies minimal and works in restricted environments.

By abstracting away boilerplate, you can focus on building better models while ensuring your pipeline remains robust, maintainable, and easy to integrate.

## Contributing

Contributions are welcome from all, regardless of rank or position.

There are no system requirements for contributing to this project. To contribute via the web:

1. Click GitLab’s “Web IDE” button to open the online editor.
2. Make your changes. **Note:** limit your changes to one part of one file per commit; for example, edit only the “Description” section here in the first commit, then the “Background and Motivation” section in a separate commit.
3. Once finished, click the blue “Commit...” button.
4. Write a detailed description of the changes you made in the “Commit Message” box.
5. Select the “Create a new branch” radio button if you do not already have your own branch; otherwise, select your branch. The recommended naming convention for new branches is ``first.middle.last``.
6. Click the green “Commit” button.

You may also contribute to this project using your local machine by cloning this repository to your workstation, creating a new branch, commiting and pushing your changes, and creating a merge request.

## Contributors

This section lists project contributors. When you submit a merge request, remember to append your name to the bottom of the list below. You may also include a brief list of the sections to which you contributed.

* **Creator:** Zachary Szewczyk

## License

This project is licensed under the [Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License](https://creativecommons.org/licenses/by-nc-sa/4.0/). You can view the full text of the license in [LICENSE.md](./LICENSE.md). Read more about the license [at the original author’s website](https://zacs.site/disclaimers.html). Generally speaking, this license allows individuals to remix this work provided they release their adaptation under the same license and cite this project as the original, and prevents anyone from turning this work or its derivatives into a commercial product.