import tempfile
import time
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path

from contract_schema.document import Document
from contract_schema import utils

class DocumentTests(unittest.TestCase):
    def setUp(self):
        self.schema = {
            "title": "Unit-Test Output",
            "type": "object",
            "fields": {
                "run_id": {"type": ["string"]},
                "execution_environment": {"type": ["object"]},
                "inputs": {"type": ["object"]},
                "input_hash": {"type": ["string"]},
                "initialization_dtg": {"type": ["string"], "format": "date-time", "required": True},
                "finalization_dtg":   {"type": ["string"], "format": "date-time", "required": True},
                "total_runtime_seconds": {"type": ["integer"], "required": True},
                "messages": {
                    "type": ["list"],
                    "items": {
                        "fields": {
                            "timestamp": {"type": ["string"], "format": "date-time", "required": True},
                            "level":     {"type": ["string"], "enum": ["INFO"], "required": True},
                            "text":      {"type": ["string"], "required": True},
                        },
                        "additionalProperties": False,
                    },
                    "required": False,
                },
            },
            "additionalProperties": False,
        }
        self.no_msg_schema = {
            "title": "NoMsg",
            "type":  "object",
            "fields": {
                "initialization_dtg": {"type": ["string"], "format": "date-time", "required": True},
                "finalization_dtg":   {"type": ["string"], "format": "date-time", "required": True},
                "total_runtime_seconds": {"type": ["integer"], "required": True},
            },
            "additionalProperties": False,
        }

    def test_document_full_lifecycle(self):
        doc = Document(schema=self.schema)

        # Auto-populated field
        self.assertIn("initialization_dtg", doc)
        init = datetime.fromisoformat(doc["initialization_dtg"].replace("Z", "+00:00"))
        self.assertLess(abs((datetime.now(timezone.utc) - init).total_seconds()), 5)

        # Message handling
        doc.add_message("info", "hello world")
        self.assertEqual(len(doc["messages"]), 1)
        self.assertEqual(doc["messages"][0]["level"], "INFO")

        # Finalization & runtime calculation
        time.sleep(1)
        doc.finalise()
        self.assertIn("finalization_dtg", doc)
        self.assertGreaterEqual(doc["total_runtime_seconds"], 1)

        # Persistence
        with tempfile.NamedTemporaryFile("r+b", delete=False) as tmp:
            path = Path(tmp.name)
        try:
            doc.save(path)
            self.assertGreater(len(path.read_text()), 0)
        finally:
            path.unlink(missing_ok=True)

    def test_save_without_finalize_raises(self):
        doc = Document(schema=self.schema)
        with tempfile.NamedTemporaryFile("r+b", delete=False) as tmp:
            path = Path(tmp.name)
        try:
            with self.assertRaises(RuntimeError):
                doc.save(path)
        finally:
            path.unlink(missing_ok=True)

    def test_add_message_after_finalize_is_ignored(self):
        doc = Document(schema=self.schema)
        doc.add_message("INFO", "first message")
        self.assertEqual(len(doc["messages"]), 1)
        doc.finalise()
        # This should be a no-op
        doc.add_message("INFO", "too late")
        self.assertEqual(len(doc["messages"]), 1)

    def test_add_message_not_supported_raises(self):
        doc = Document(schema=self.no_msg_schema)
        with self.assertRaises(NotImplementedError):
            doc.add_message("INFO", "boom")

    def test_finalize_is_idempotent(self):
        doc = Document(schema=self.no_msg_schema)
        doc.finalise()
        # Call again – should silently noop, not raise
        doc.finalise()

        with tempfile.NamedTemporaryFile("w+", delete=False) as tmp:
            p = Path(tmp.name)

        try:
            doc.save(p)  # still allowed
        finally:
            p.unlink(missing_ok=True)

    # ------------------------------------------------------------------ #
    # Core metadata assertions                                           #
    # ------------------------------------------------------------------ #
    def test_finalise_injects_run_id_env_and_hash(self):
        inputs = {"param": "value"}
        doc = Document(schema=self.schema, inputs=inputs)

        init_dt = datetime.fromisoformat(doc["initialization_dtg"].replace("Z", "+00:00"))
        self.assertLess(abs((datetime.now(timezone.utc) - init_dt).total_seconds()), 5)

        doc.finalise()

        # --- run_id ------------------------------------------------------
        self.assertIn("run_id", doc)
        # Valid UUID?
        uuid_obj = uuid.UUID(doc["run_id"])
        self.assertEqual(str(uuid_obj), doc["run_id"])

        # --- execution_environment --------------------------------------
        env = doc.get("execution_environment")
        self.assertIsInstance(env, dict)
        self.assertIn("python_version", env)
        self.assertIn("operating_system", env)

        # --- input_hash --------------------------------------------------
        expected_hash = utils._hash(inputs)
        self.assertEqual(doc["input_hash"], expected_hash)

        # --- runtime seconds --------------------------------------------
        self.assertGreaterEqual(doc["total_runtime_seconds"], 0)

    def test_finalise_missing_initialization_dtg_raises(self):
        schema = {
            "title": "MissingInit",
            "type": "object",
            "fields": {
                "finalization_dtg": {"type": ["string"], "format": "date-time"},
                "total_runtime_seconds": {"type": ["integer"]},
            },
            "additionalProperties": False,
        }
        doc = Document(schema=schema)
        with self.assertRaises(KeyError):
            doc.finalise()

    def test_finalise_missing_finalization_dtg_raises(self):
        schema = {
            "title": "MissingFinal",
            "type": "object",
            "fields": {
                "initialization_dtg": {"type": ["string"], "format": "date-time"},
                "total_runtime_seconds": {"type": ["integer"]},
            },
            "additionalProperties": False,
        }
        doc = Document(schema=schema)
        with self.assertRaises(KeyError):
            doc.finalise()

    def test_finalise_missing_both_dtg_raises(self):
        schema = {
            "title": "MissingBoth",
            "type": "object",
            "fields": {
                "total_runtime_seconds": {"type": ["integer"]},
            },
            "additionalProperties": False,
        }
        doc = Document(schema=schema)
        with self.assertRaises(KeyError):
            doc.finalise()