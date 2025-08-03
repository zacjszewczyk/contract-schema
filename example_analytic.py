#!/usr/bin/env python
"""
example_analytic.py
Demonstrates how to use *contract_schema* with the packaged **analytic** contract.

Steps
-----
1. Load the contract.
2. Build & validate a minimal analytic *input* object.
3. Derive one toy finding (class-distribution summary).
4. Build the analytic *output* document.
5. Finalise (inject hashes, timing, env) & write JSON + Markdown reports.
"""
from __future__ import annotations

from pathlib import Path
import uuid
import logging

import pandas as pd
from sklearn.datasets import load_iris

from contract_schema import Contract, utils, to_markdown_card

# --------------------------------------------------------------------------- #
# Logging                                                                     #
# --------------------------------------------------------------------------- #
logging.basicConfig(
    level="INFO",
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("contract_schema.examples")

# --------------------------------------------------------------------------- #
# 1.  Load contract                                                           #
# --------------------------------------------------------------------------- #
C = Contract.load("analytic_schema.json")

# --------------------------------------------------------------------------- #
# 2.  Build & validate analytic **inputs**                                    #
# --------------------------------------------------------------------------- #
now_iso = utils._now_iso()
iris = load_iris(as_frame=True)
df: pd.DataFrame = iris.frame

inputs = C.parse_and_validate_input(
    {
        "start_dtg":        now_iso,
        "end_dtg":          now_iso,
        "data_source_type": "file",
        "data_source":      "iris.frame",
    }
)

# --------------------------------------------------------------------------- #
# 3.  Derive a single finding                                                 #
# --------------------------------------------------------------------------- #
class_counts = df["target"].value_counts().to_dict()
findings = [{
    "finding_id":          str(uuid.uuid4()),
    "title":               "Class distribution summary",
    "description":         f"Counts per class: {class_counts}",
    "event_dtg":           utils._now_iso(),
    "severity":            "low",
    "confidence":          "high",
    "observables":         list(map(str, class_counts.keys())),
    "mitre_attack_tactics":     [],
    "mitre_attack_techniques":  [],
    "recommended_actions":      "None – informational only.",
    "recommended_pivots":       "N/A",
    "classification":           "U",
}]

# --------------------------------------------------------------------------- #
# 4.  Build the analytic OUTPUT document                                      #
# --------------------------------------------------------------------------- #
doc = C.create_document(
    # provenance -------------------------------------------------------------
    input_schema_version="1.0.1",
    output_schema_version=C.version,
    author="Zac Szewczyk",
    author_organization="Example Org",
    contact="zac@example.com",
    license="Apache-2.0",
    documentation_link=(
        "https://scikit-learn.org/stable/auto_examples/"
        "datasets/plot_iris_dataset.html"
    ),
    status="success",
    exit_code=0,
    # dataset ----------------------------------------------------------------
    dataset_description=(
        "Fisher's Iris flower data set treated as row-level events for a toy analytic."
    ),
    dataset_size=len(df),
    dataset_hash=utils._hash(df),
    data_schema={**{c: "number" for c in iris.feature_names}, "target": "integer"},
    feature_names=iris.feature_names,
    # analytic metadata ------------------------------------------------------
    inputs=inputs,
    analytic_id=str(Path(__file__).resolve()),
    analytic_name="Iris class distribution",
    analytic_version="0.1.0",
    analytic_description=(
        "Demonstration analytic that summarises the distribution of species in the Iris data set."
    ),
    # findings ---------------------------------------------------------------
    findings=findings,
    additional_run_properties={"class_counts": class_counts},
)

# --------------------------------------------------------------------------- #
# 5.  Finalise & persist                                                      #
# --------------------------------------------------------------------------- #
doc.finalise()

json_path = Path("iris_analytic_report.json")
doc.save(json_path)
log.info("Output document written to %s", json_path.resolve())

md_path = Path("iris_analytic_report.md")
md_path.write_text(to_markdown_card(doc), encoding="utf-8")
log.info("Markdown report written to %s", md_path.resolve())
