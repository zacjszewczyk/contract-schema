import unittest

from contract_schema.utils import _hash

class UtilsTests(unittest.TestCase):
    def test_hash_is_deterministic_and_unique(self):
        obj1 = {"a": 1, "b": [2, 3]}
        obj2 = {"a": 1}

        self.assertEqual(_hash(obj1), _hash(obj1))
        self.assertNotEqual(_hash(obj1), _hash(obj2))
