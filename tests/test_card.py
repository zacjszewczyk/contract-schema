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
        self.assertIn("Example", md)
        self.assertIn("- 1", md)              # list item
        self.assertIn("- **a**: 1", md)        # nested mapping rendered

    def test_to_markdown_card_with_empty_values(self):
        data = {
            "title":       "Empty Test",
            "empty_list":  [],
            "empty_dict":  {},
            "none_value":  None,
        }
        md = to_markdown_card(data)
        self.assertIn("## Title", md)
        self.assertIn("Empty Test", md)
        # Asserts that keys for empty collections become headers
        self.assertIn("## Empty List", md)
        self.assertIn("## Empty Dict", md)
        self.assertIn("## None Value\nnull", md)

    def test_to_markdown_card_with_nested_structures(self):
        data = {
            "title": "Nested Example",
            "nested_list": [
                {"item": 1, "status": "A"},
                {"item": 2, "status": "B"},
            ],
        }
        md = to_markdown_card(data)
        self.assertIn("## Title", md)
        self.assertIn("Nested Example", md)
        # Asserts that nested objects in a list are stringified
        self.assertIn("- {'item': 1, 'status': 'A'}", md)
        self.assertIn("- {'item': 2, 'status': 'B'}", md)

    def test_to_markdown_card_empty_input(self):
        md = to_markdown_card({})
        self.assertEqual(md.strip(), "")

    def test_to_markdown_card_heading_level(self):
        data = {"Test": "Value"}
        md = to_markdown_card(data, heading_level=4)
        self.assertIn("#### Test", md)

    def test_to_markdown_card_nested_list_flattening(self):
        data = {"nested": [["a", "b"], ["c"]]}
        md = to_markdown_card(data)
        self.assertIn("- a, b", md)
        self.assertIn("- c", md)