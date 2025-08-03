import unittest
from contract_schema import utils
import re
from pathlib import Path
from tests._util import tmp_bytes_file, sha256_bytes

class UtilsTests(unittest.TestCase):
    def test_hash_is_deterministic_and_unique(self):
        obj1 = {"a": 1, "b": [2, 3]}
        obj2 = {"a": 1}
        obj3 = {"b": [2, 3], "a": 1} # Order shouldn't matter
        obj4 = {"a": 1, "b": [2, 3, {"c": 4}]} # Deeper object

        self.assertEqual(utils._hash(obj1), utils._hash(obj1))
        self.assertEqual(utils._hash(obj1), utils._hash(obj3))
        self.assertNotEqual(utils._hash(obj1), utils._hash(obj2))
        self.assertNotEqual(utils._hash(obj1), utils._hash(obj4))

    def test_hash_dataframe(self):
        try:
            import pandas as pd
            df1 = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
            df2 = pd.DataFrame({'col1': [1, 2], 'col2': [3, 5]}) # Different data
            df_hash1 = utils._hash(df1)
            df_hash1_again = utils._hash(df1.copy())
            df_hash2 = utils._hash(df2)
            self.assertEqual(df_hash1, df_hash1_again)
            self.assertNotEqual(df_hash1, df_hash2)
            self.assertTrue(re.fullmatch(r"[0-9a-f]{64}", df_hash1))
        except ImportError:
            self.skipTest("pandas is not installed, skipping DataFrame hash test")

    def test_is_datetime_various_inputs(self):
        good = [
            "2025-08-03T12:00:00Z",
            "2023-01-02T03:04:05+00:00",
            "2024-12-31T23:59:59-05:00",
        ]
        bad = ["not-dt", "2025-13-01T00:00:00Z", 42, "2025-08-03T12:00:00"] # Missing timezone

        for g in good:
            self.assertTrue(utils._is_datetime(g), g)

        for b in bad:
            self.assertFalse(utils._is_datetime(b), str(b))

    def test_sha256_file_matches_manual_digest(self):
        buf = b"contract-schema-test"
        f = tmp_bytes_file(buf)
        try:
            self.assertEqual(utils._sha256(f), sha256_bytes(buf))
            self.assertTrue(re.fullmatch(r"[0-9a-f]{64}", utils._sha256(f)))
        finally:
            f.unlink(missing_ok=True)

    def test_sha256_empty_file(self):
        f = tmp_bytes_file(b"")
        try:
            self.assertEqual(utils._sha256(f), sha256_bytes(b""))
        finally:
            f.unlink(missing_ok=True)

    def test_environment_capture_functions(self):
        libs = utils._library_versions()
        self.assertIsInstance(libs, dict)
        
        hardware = utils._hardware_specs()
        self.assertIsInstance(hardware, dict)
        self.assertIn("cpu", hardware)
        self.assertIn("ram", hardware)
        self.assertIn("gpu", hardware)