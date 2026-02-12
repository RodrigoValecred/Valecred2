import unittest
import ast
import sys
import os

# Path to the notebook file
NOTEBOOK_PATH = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Prepara_Tabela_Operacoes.Notebook/notebook-content.py"

try:
    from tests.notebook_utils import extract_function_from_file
except ImportError:
    # Try importing directly if running from within tests directory
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from notebook_utils import extract_function_from_file

class TestDecodeHtmlEntities(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print(f"Extracting decode_html_entities from {NOTEBOOK_PATH}")
        func_source = extract_function_from_file(NOTEBOOK_PATH, "decode_html_entities")

        if func_source:
            # Execute the function definition in the class scope
            local_scope = {}
            # We need to make sure imports inside the function work.
            # If function has 'import html' inside, it's fine.
            exec(func_source, globals(), local_scope)
            cls.decode_html_entities = staticmethod(local_scope["decode_html_entities"])
        else:
            cls.decode_html_entities = None
            print("WARNING: decode_html_entities function not found in file.")

    def test_function_exists(self):
        """Test that the function was successfully extracted."""
        self.assertIsNotNone(self.decode_html_entities, "Function decode_html_entities not found in notebook file.")

    def test_basic_decoding(self):
        """Test decoding of basic HTML entities."""
        if not self.decode_html_entities:
            self.skipTest("Function not found")
        self.assertEqual(self.decode_html_entities("&amp;"), "&")
        self.assertEqual(self.decode_html_entities("&lt;"), "<")
        self.assertEqual(self.decode_html_entities("&gt;"), ">")
        self.assertEqual(self.decode_html_entities("&quot;"), '"')
        self.assertEqual(self.decode_html_entities("&#39;"), "'")

    def test_none_input(self):
        """Test handling of None input."""
        if not self.decode_html_entities:
            self.skipTest("Function not found")
        self.assertIsNone(self.decode_html_entities(None))

    def test_empty_string(self):
        """Test handling of empty string."""
        if not self.decode_html_entities:
            self.skipTest("Function not found")
        # Assuming implementation: if text and isinstance(text, str): ... return text
        # if text is "", if text is false, returns text ("").
        self.assertEqual(self.decode_html_entities(""), "")

    def test_no_entities(self):
        """Test string with no entities."""
        if not self.decode_html_entities:
            self.skipTest("Function not found")
        text = "Hello World"
        self.assertEqual(self.decode_html_entities(text), text)

    def test_non_string_input(self):
        """Test handling of non-string input (should return as is)."""
        if not self.decode_html_entities:
            self.skipTest("Function not found")
        val = 123
        self.assertEqual(self.decode_html_entities(val), val)
        val = 3.14
        self.assertEqual(self.decode_html_entities(val), val)

class TestTacVariations(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print(f"Extracting get_tac_variations from {NOTEBOOK_PATH}")
        func_source = extract_function_from_file(NOTEBOOK_PATH, "get_tac_variations")

        if func_source:
            local_scope = {}
            exec(func_source, globals(), local_scope)
            cls.get_tac_variations = staticmethod(local_scope["get_tac_variations"])
        else:
            cls.get_tac_variations = None
            print("WARNING: get_tac_variations function not found in file.")

    def test_function_exists(self):
        """Test that the function was successfully extracted."""
        self.assertIsNotNone(self.get_tac_variations, "Function get_tac_variations not found in notebook file.")

    def test_variations_list_content(self):
        """Test that the variations list contains expected values."""
        if not self.get_tac_variations:
            self.skipTest("Function not found")

        variations = self.get_tac_variations()
        self.assertIsInstance(variations, list)

        expected_items = ["TAC  M", "TAC MOP", "TAC M.", "TACM", "TACA M", "TAC M 300,00", "TAC"]
        for item in expected_items:
            self.assertIn(item, variations)

        # Verify no unexpected items if the list is intended to be exact
        self.assertEqual(len(variations), len(expected_items))
        self.assertEqual(set(variations), set(expected_items))

    def test_variations_are_strings(self):
        """Test that all items in the list are strings."""
        if not self.get_tac_variations:
            self.skipTest("Function not found")

        variations = self.get_tac_variations()
        for item in variations:
            self.assertIsInstance(item, str)

if __name__ == '__main__':
    unittest.main()
