import json
import tempfile
import unittest
from pathlib import Path

from contract_schema import loader

class LoaderTests(unittest.TestCase):
    def test_load_schema_from_file(self):
        data = {
            "title": "T",
            "version": "1",
            "description": "d",
            "input": {"fields": {}},
            "output": {"fields": {}},
        }
        with tempfile.NamedTemporaryFile("w+", delete=False) as tmp:
            json.dump(data, tmp)
            tmp.flush()
            path = Path(tmp.name)

        try:
            loaded = loader.load_schema(path)
            self.assertEqual(loaded, data)
        finally:
            path.unlink(missing_ok=True)

    def test_load_schema_from_package_resource(self):
        schema = loader.load_schema("analytic_schema.json")
        self.assertEqual(schema["title"], "Analytic Schema")

    def test_load_schema_not_found(self):
        with self.assertRaises(FileNotFoundError):
            loader.load_schema("does_not_exist.json")

    def test_invalid_json_raises_value_error(self):
        with tempfile.NamedTemporaryFile("w+", delete=False) as tmp:
            tmp.write("{not json")  # malformed
            tmp.flush()
            p = Path(tmp.name)

        try:
            with self.assertRaises(ValueError):
                loader.load_schema(p)
        finally:
            p.unlink(missing_ok=True)

    def test_empty_file_raises_value_error(self):
        with tempfile.NamedTemporaryFile("w+", delete=False) as tmp:
            p = Path(tmp.name) # File is created but empty

        try:
            with self.assertRaises(ValueError):
                loader.load_schema(p)
        finally:
            p.unlink(missing_ok=True)