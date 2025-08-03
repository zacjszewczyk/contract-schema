import unittest
from contract_schema.utils import _hash, _is_datetime, _sha256
import re
from pathlib import Path
from tests._util import tmp_bytes_file, sha256_bytes

class UtilsTests(unittest.TestCase):
    def test_hash_is_deterministic_and_unique(self):
        obj1 = {"a": 1, "b": [2, 3]}
        obj2 = {"a": 1}

        self.assertEqual(_hash(obj1), _hash(obj1))
        self.assertNotEqual(_hash(obj1), _hash(obj2))

    def test_is_datetime_various_inputs(self):
        good = [
            "2025-08-03T12:00:00Z",
            "2023-01-02T03:04:05+00:00",
            "2024-12-31T23:59:59-05:00",
        ]
        bad = ["not-dt", "2025-13-01T00:00:00Z", 42]

        for g in good:
            self.assertTrue(_is_datetime(g), g)

        for b in bad:
            self.assertFalse(_is_datetime(b), str(b))

    def test_sha256_file_matches_manual_digest(self):
        buf = b"contract-schema-test"
        f = tmp_bytes_file(buf)
        try:
            self.assertEqual(_sha256(f), sha256_bytes(buf))
            self.assertTrue(re.fullmatch(r"[0-9a-f]{64}", _sha256(f)))
        finally:
            f.unlink(missing_ok=True)