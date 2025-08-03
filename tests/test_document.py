import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path

from contract_schema.document import Document


class DocumentTests(unittest.TestCase):
    def setUp(self):
        self.schema = {
            "title": "Unit-Test Output",
            "type": "object",
            "fields": {
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

    def test_save_to_nonexistent_dir_raises(self):
        doc = Document(schema=self.no_msg_schema)
        doc.finalise()
        bad_path = Path(tempfile.gettempdir()) / "non_existent_dir" / "doc.json"
        self.assertFalse(bad_path.parent.exists())
        with self.assertRaises(FileNotFoundError):
            doc.save(bad_path)

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