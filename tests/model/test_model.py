import unittest, json, pickle, tempfile
from pathlib import Path
import sklearn.dummy
import pandas as pd
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split

from tests._util import MODEL_C as C, tmp_dir

iris   = load_iris(as_frame=True)
X_tr, X_te, y_tr, y_te = train_test_split(
    iris.data, iris.target, test_size=0.2, random_state=0, stratify=iris.target
)

class ModelContractTests(unittest.TestCase):

    def _basic_manifest(self):
        return dict(
            author_organization="Demo",
            contact="demo@example.com",
            documentation_link="https://example.com",
            license="MIT",
            model_type="Dummy",
            model_architecture="MostFrequentClassifier",
            model_version="0.1.0",
            model_description="Baseline",
            intended_use="Unit test",
            limitations="None",
            data_description="Iris",
            data_schema={c: "float" for c in iris.feature_names},
            feature_names=iris.feature_names,
            target_variable="species",
            feature_engineering_pipeline=[],
            model_parameters={},
            dataset_size=len(iris.data),
            dataset_hash="0"*64,
            train_size=len(X_tr),
            test_size=len(X_te),
            train_hash="0"*64,
            test_hash="0"*64,
            random_seed=0,
            learning_task="classification",
            metrics={
                "training": {"accuracy": 1.0},
                "test":     {"accuracy": 0.9},
            },
            training_duration_seconds=0.0,
        )

    def test_manifest_roundtrip(self):
        clf = sklearn.dummy.DummyClassifier(strategy="most_frequent")
        clf.fit(X_tr, y_tr)

        with tmp_dir() as td:
            model_path = Path(td) / "m.pkl"
            with model_path.open("wb") as fd:
                pickle.dump(clf, fd)

            mani = C.create_document(**self._basic_manifest())
            mani["export_dtg"] = mani["initialization_dtg"]  # minimal extra
            mani.finalise()

            json_path = Path(td) / "manifest.json"
            mani.save(json_path)
            self.assertTrue(json_path.is_file())
            doc = json.loads(json_path.read_text())
            self.assertEqual(doc["model_type"], "Dummy")
