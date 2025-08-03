#!/usr/bin/env python
"""
Train a RandomForest on the Iris data set and emit a model manifest
conforming to *model_schema.json*.

Outputs
-------
iris_rf.joblib - serialized model
iris_model_manifest.json - fully-validated contract document
"""
from __future__ import annotations

import time, uuid, sys
from pathlib import Path

import joblib
from sklearn.datasets import load_iris
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split

from contract_schema import Contract, utils, to_markdown_card    # pip install .
from contract_schema.validator import SchemaError

import logging
log = logging.getLogger("contract_schema.examples")
logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(message)s")

# --------------------------------------------------------------------------- #
# Main                                                                        #
# --------------------------------------------------------------------------- #
def main() -> None:
    contract = Contract.load("model_schema.json")
    
    inputs = contract.parse_and_validate_input({
        "log_path": "stdout",
        "output": "stdout",
        "verbosity": "INFO",
    })

    start_ts = time.time()
    # ------------------------------------------------------------------ #
    # Train model                                                        #
    # ------------------------------------------------------------------ #
    iris = load_iris(as_frame=True)
    X, y = iris.data, iris.target
    feature_names = list(X.columns)

    X_tr, X_te, y_tr, y_te = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    model = RandomForestClassifier(random_state=42)
    model.fit(X_tr, y_tr)
    acc = accuracy_score(y_te, model.predict(X_te))
    train_secs = time.time() - start_ts

    model_path = Path("iris_rf.joblib")
    joblib.dump(model, model_path)

    # ------------------------------------------------------------------ #
    # Build manifest                                                     #
    # ------------------------------------------------------------------ #
    doc = contract.create_document(
        input_schema_version=contract.version,
        output_schema_version=contract.version,
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
        dataset_description="Fisher's Iris flower dataset (1936).",
        dataset_size=len(X),
        dataset_hash=utils._hash(X),
        data_schema={c: "number" for c in X.columns},
        feature_names=feature_names,
        inputs=inputs,
        model_id=str(model_path),
        model_type="RandomForest",
        model_architecture="n_estimators=100 (default)",
        model_version="0.1.0",
        model_description="RandomForest trained on Iris.",
        learning_task="classification",
        intended_use="Library demonstration only.",
        limitations="Toy example; no production guarantees.",
        feature_engineering_pipeline=["None (raw numeric features)"],
        model_parameters=model.get_params(),
        train_size=len(X_tr),
        train_hash=utils._hash(X_tr),
        random_seed=42,
        metrics={"accuracy": round(acc, 4)},
        training_duration_seconds=round(train_secs, 2),
        export_dtg=utils._now_iso(),
        model_file_path=str(model_path),   # lets finalise() hash the file
    )

    try:
        doc.finalise()
    except SchemaError as exc:
        sys.exit(f"Manifest failed validation:\n{exc}")

    doc.save("iris_model_manifest.json")
    log.info("Saved model manifest - iris_model_manifest.json")

    outfile = Path("iris_model_report.md")
    with open(outfile, "w") as f:
        f.write(to_markdown_card(doc))
    log.info(f"Output report written to {outfile.resolve()}")

if __name__ == "__main__":
    main()
