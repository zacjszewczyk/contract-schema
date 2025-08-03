#!/usr/bin/env python
"""
example_analytic.py
───────────────────
Demonstrates how to use *contract_schema* with the packaged analytic contract.

The script:

1. Loads the Fisher Iris data set.
2. Builds a minimal, schema-valid analytic **input** object.
3. Passes it through `Contract.parse_and_validate_input()` (so all CLI / JSON /
   default logic is exercised).
4. Computes a trivial “finding” (class-distribution summary).
5. Creates an output `Document`, lets `finalise()` inject hashes, timing,
   run-ID, execution-environment, etc.
6. Saves `iris_analytic_report.json`.
"""
from __future__ import annotations

import sys, uuid, time
from pathlib import Path

import pandas as pd
from sklearn.datasets import load_iris

from contract_schema import Contract, utils

# --------------------------------------------------------------------------- #
# 1.  Load contract                                                           #
# --------------------------------------------------------------------------- #
# The JSON lives in contract_schema/schemas/analytic_schema.json (packaged),
# so the bare filename is enough.
C = Contract.load("analytic_schema.json")

# --------------------------------------------------------------------------- #
# 2.  Build & validate analytic **inputs**                                    #
# --------------------------------------------------------------------------- #
now = utils._now_iso()
iris = load_iris(as_frame=True)
df: pd.DataFrame = iris.frame

inputs = C.parse_and_validate_input({
    "start_dtg":        now,
    "end_dtg":          now,
    "data_source_type": "df",
    "data_source":      df,          # we can pass a DataFrame directly
    # log_path / output / analytic_parameters / data_map / verbosity fall back
    # to schema defaults injected by parse_and_validate_input()
})

# --------------------------------------------------------------------------- #
# 3.  Derive a single “finding”                                               #
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
    "recommended_actions":      "None - this is informational only.",
    "recommended_pivots":       "N/A",
    "classification":           "U",
}]

# --------------------------------------------------------------------------- #
# 4.  Build the analytic OUTPUT document                                      #
# --------------------------------------------------------------------------- #
doc = C.create_document(
    # ─ required provenance --------------------------------------------------
    input_schema_version="1.0.1",
    output_schema_version=C.version,
    author="Zac Szewczyk",
    author_organization="Example Org",
    contact="zac@example.com",
    license="Apache-2.0",
    documentation_link="https://scikit-learn.org/stable/auto_examples/"
                       "datasets/plot_iris_dataset.html",
    status="success",
    exit_code=0,

    # ─ dataset metadata -----------------------------------------------------
    dataset_description="Fisher’s Iris flower data set treated as row-level "
                        "events for a toy analytic.",
    dataset_size=len(df),
    dataset_hash=utils._hash(df),
    data_schema={**{c: "number" for c in iris.feature_names}, "target": "integer"},
    feature_names=iris.feature_names,

    # ─ analytic metadata ----------------------------------------------------
    inputs=inputs,                     # verbatim, for auditability
    analytic_id=str(Path(__file__).resolve()),
    analytic_name="Iris class distribution",
    analytic_version="0.1.0",
    analytic_description="Demonstration analytic that summarises the "
                         "distribution of species in the Iris data set.",

    # ─ findings -------------------------------------------------------------
    findings=findings,
)

# Extra run-wide metadata is optional
doc["additional_run_properties"] = {"class_counts": class_counts}

# --------------------------------------------------------------------------- #
# 5.  Finalise (inject hashes, timing, env) & save                            #
# --------------------------------------------------------------------------- #
doc.finalise()
outfile = Path("iris_analytic_report.json")
doc.save(outfile)
print(f"✅  Analytic report written to {outfile.resolve()}")
