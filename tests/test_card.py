import unittest

from contract_schema.card import to_markdown_card


class CardTests(unittest.TestCase):
    def test_to_markdown_card_basic(self):
        data = {
            "title":   "Example",
            "values":  [1, 2, 3],
            "details": {"a": 1, "b": True},
        }
        md = to_markdown_card(data)
        self.assertIn("## Title", md)
        self.assertIn("- 1", md)               # list item
        self.assertIn("**a**: 1", md)          # nested mapping rendered
