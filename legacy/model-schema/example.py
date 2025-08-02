"""
Example script demonstrating the use of the model_schema package.

This script performs a complete machine learning workflow for multiple models:
1.  Loads the Iris dataset and splits it into training and testing sets.
2.  Defines a list of scikit-learn compatible classifiers to train.
3.  Iterates through each model, performing the following steps:
    a. Trains the model on the training data.
    b. Evaluates the model's performance on both training and test sets.
    c. Serializes the trained model object to a pickle file.
    d. Gathers extensive metadata, including hyperparameters, dataset details,
       performance metrics, and execution environment information.
    e. Populates and validates a ModelManifest object for the model.
4.  Collects all individual manifests into a single run-level manifest.
5.  Performs a final validation pass on the collected manifests.
6.  Saves the complete run manifest to a single, version-controlled JSON file.
"""
# Core Python libraries for file paths, timing, hashing, and data serialization.
from pathlib import Path
from time import perf_counter
import hashlib, json, pickle, random, datetime as dt, math

# Third-party libraries for numerical computing, data manipulation, and ML.
import numpy as np
import pandas as pd
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.metrics import precision_recall_fscore_support, accuracy_score
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from xgboost import XGBClassifier

# The custom package for creating and validating model manifests.
import model_schema as msc

# ---------------------------------------------------------------------------
# Section 1: Data Preparation
# ---------------------------------------------------------------------------
# This block sets up the shared resources that will be used for all model
# training runs. This includes loading the dataset, splitting it into
# standard training and testing sets, and defining a helper function for
# consistent metric calculation.

# Load the Iris dataset from scikit-learn, returning it as a pandas DataFrame
# for convenient access to feature names and data.
iris = load_iris(as_frame=True)

# Split the dataset into training and testing sets.
# The split is stratified by the target variable to ensure that the
# proportion of each flower class is the same in both the training and
# testing sets, which is crucial for representative evaluation.
# A fixed random_state ensures that this split is reproducible.
X_train, X_test, y_train, y_test = train_test_split(
    iris.data, iris.target, test_size=0.25, random_state=42, stratify=iris.target
)

def _metric_dict(y_true, y_pred):
    """Calculate and return a dictionary of common classification metrics.

    This helper function computes weighted precision, recall, and F1-score,
    along with accuracy. The results are rounded to four decimal places for
    consistent and clean reporting in the final manifest.

    Args:
        y_true (array-like): The ground-truth (correct) target values.
        y_pred (array-like): The estimated targets as returned by a classifier.

    Returns:
        dict: A dictionary containing the calculated 'precision', 'recall',
              'f1', and 'accuracy' scores.
    """
    # Calculate precision, recall, and F1-score. The 'weighted' average
    # calculates metrics for each label and finds their average, weighted by
    # support (the number of true instances for each label).
    # `zero_division=0` prevents warnings if a class has no predictions.
    p, r, f, _ = precision_recall_fscore_support(y_true, y_pred, average="weighted", zero_division=0)
    return {
        "precision": round(p, 4),
        "recall":    round(r, 4),
        "f1":        round(f, 4),
        "accuracy":  round(accuracy_score(y_true, y_pred), 4),
    }

# Initialize variables for run-level bookkeeping. These values are constant
# across the entire script execution and serve as metadata for the run.
#
# A single timestamp (`run_dtg`) ensures that all artifacts from this run
# can be easily grouped and identified.
#
# A fixed random seed (`run_seed`) is used to initialize the random number
# generators for Python, NumPy, and the models themselves, ensuring that
# the training process is deterministic and reproducible.
run_dtg   = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
run_seed  = 42
random.seed(run_seed); np.random.seed(run_seed)

# This dictionary will serve as the top-level container for the entire run's
# output. Individual model manifests will be appended to the 'models' list.
run_manifest = {
    "run_dtg": run_dtg,
    "random_seed": run_seed,
    "models": []
}

# Define the output directory for all generated artifacts. A subdirectory
# named with the run's unique timestamp (`run_dtg`) is created to
# neatly organize all outputs from this specific execution.
# `pathlib.Path` is used for robust, cross-platform path handling.
# `mkdir(parents=True, exist_ok=True)` ensures the full path is created
# without raising an error if it already exists.
export_dir = Path("model_exports") / run_dtg
export_dir.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Section 2: Model Training and Manifest Generation Loop
# ---------------------------------------------------------------------------
# This section defines the set of models to be trained and then iterates
# through them, performing the full train, evaluate, and document workflow
# for each one.

# A list of tuples defines the models for this run. Each tuple contains a
# human-readable name for the model type and an instantiated classifier
# object with its specific hyperparameters and the shared random_state.
MODELS = [
    ("RandomForest",       RandomForestClassifier(n_estimators=200, random_state=run_seed)),
    ("GradientBoosting",   GradientBoostingClassifier(random_state=run_seed)),
    ("XGBoost",            XGBClassifier(n_estimators=150, eval_metric="logloss", random_state=run_seed)),
]

# The main loop iterates through each model defined in the MODELS list.
for model_type, model in MODELS:
    # Train the model and measure the duration of the training process.
    # `perf_counter` provides a high-resolution monotonic clock suitable
    # for timing short-duration intervals.
    tic = perf_counter()
    model.fit(X_train, y_train)
    train_dur = perf_counter() - tic

    # Generate predictions on both the training and test sets to evaluate
    # for performance and potential overfitting.
    y_hat_tr = model.predict(X_train)
    y_hat_te = model.predict(X_test)

    # Calculate performance metrics for both sets using the helper function.
    metrics = {
        "training":  _metric_dict(y_train, y_hat_tr),
        "test":      _metric_dict(y_test,  y_hat_te),
    }

    # Serialize the trained model object to a pickle file.
    # The filename is constructed from the model type and the run's unique
    # timestamp to ensure a unique and descriptive name.
    fn_stem = f"{model_type}_{run_dtg}"
    model_path = export_dir / f"{fn_stem}.pkl"
    with model_path.open("wb") as fd:
        pickle.dump(model, fd, pickle.HIGHEST_PROTOCOL)

    # Instantiate the ModelManifest object, populating it with all known
    # metadata about the model, data, and training process. This serves as
    # the structured documentation for the model artifact.
    mani = msc.ModelManifest(
        author_organization  = "Cybersecurity",
        contact              = "zacjszewczyk@gmail.com",
        documentation_link   = "https://github.com/zacjszewczyk/model-schema",
        license              = "Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License",
        model_type           = model_type,
        model_architecture   = model_type,
        model_version        = "1.0.0",
        model_description    = f"{model_type} for iris classification",
        intended_use         = "Demo – flower species prediction",
        limitations          = "Toy dataset; not for production",
        data_description     = "UCI iris dataset",
        data_schema          = {c: str(t) for c, t in zip(iris.data.columns, ["float"]*4)},
        feature_names        = iris.data.columns.tolist(),
        target_variable      = "species",
        feature_engineering_pipeline = ["None – raw numeric features"],
        # The hyperparameters are retrieved from the model. Any `NaN` float
        # values are converted to `None` to ensure valid JSON serialization,
        # as `NaN` is not part of the JSON standard.
        hyperparameters      = {
                                 k: (None if isinstance(v, float) and math.isnan(v) else v)
                                 for k, v in model.get_params().items()
                             },
        number_trials        = 1,
        dataset_size         = len(iris.data),
        # Hashes of the datasets are computed to ensure data integrity and
        # provide a verifiable fingerprint of the exact data used.
        dataset_hash         = hashlib.sha256(iris.data.to_json().encode()).hexdigest(),
        train_size           = len(X_train),
        test_size            = len(X_test),
        train_hash           = hashlib.sha256(X_train.to_json().encode()).hexdigest(),
        test_hash            = hashlib.sha256(X_test.to_json().encode()).hexdigest(),
        random_seed          = run_seed,
        metrics              = metrics,
        training_duration_seconds = round(train_dur, 3)
    )

    # Finalize the manifest. This crucial step computes the hash of the
    # saved model file, gathers execution environment details (like OS,
    # Python version, and library versions), and runs a deep validation
    # against the schema to ensure correctness and completeness.
    mani.finalise(model_path)

    # Append the completed and validated manifest for the current model
    # to the run-level list of models.
    run_manifest["models"].append(mani)

# ---------------------------------------------------------------------------
# Section 3: Final Model Schema Validation
# ---------------------------------------------------------------------------
# This optional block provides a final sanity check. It iterates through the
# collection of newly created manifests and validates each one against the
# schema again. This helps catch any issues that might have occurred during
# the manifest assembly process before persisting the final output.
for idx, m in enumerate(run_manifest["models"]):
    try:
        msc.validate_manifest(m)
    except msc.SchemaError as e:
        # If any manifest fails validation, raise a RuntimeError with a
        # detailed message, stopping the script.
        raise RuntimeError(f"Manifest #{idx} failed validation: {e}")

# ---------------------------------------------------------------------------
# Section 4: Persist Final Output
# ---------------------------------------------------------------------------
# This final step saves the entire collection of model manifests to a single,
# human-readable JSON file. This file serves as the complete, self-contained
# record of the training run.

# Construct the final output path within the designated export directory.
joined_path = export_dir / f"all_manifests_{run_dtg}.json"
# Serialize the `run_manifest` dictionary to a JSON formatted string and
# write it to the file.
# `indent=4` ensures the JSON is pretty-printed and human-readable.
# `ensure_ascii=False` allows for non-ASCII characters.
# `encoding="utf-8"` is explicitly set to prevent encoding errors on
# different operating systems.
joined_path.write_text(json.dumps(run_manifest, indent=4, ensure_ascii=False), encoding="utf-8")
print("Complete – exported:", joined_path)