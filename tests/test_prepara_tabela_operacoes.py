import unittest
import ast
import sys
import os

# Path to the notebook file
NOTEBOOK_PATH = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Prepara_Tabela_Operacoes.Notebook/notebook-content.py"

def extract_function_from_file(filepath, function_name):
    """
    Extracts the source code of a function from a python file using AST.
    """
    if not os.path.exists(filepath):
        # Allow running from root or tests dir
        filepath = os.path.join("..", filepath)
        if not os.path.exists(filepath):
             # Try absolute path based on current script dir
             pass

    # Reset to original if not found via relative
    if not os.path.exists(filepath):
         # Just use the original path, maybe we are at root
         filepath = NOTEBOOK_PATH

    if not os.path.exists(filepath):
        print(f"File not found at: {filepath}")
        return None

    with open(filepath, 'r', encoding='utf-8') as f:
        source = f.read()

    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        print(f"Syntax error parsing {filepath}: {e}")
        return None

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            if hasattr(ast, 'get_source_segment'):
                return ast.get_source_segment(source, node)
            else:
                lines = source.splitlines()
                start = node.lineno - 1
                end = node.end_lineno
                return "\n".join(lines[start:end])
    return None

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

if __name__ == '__main__':
    unittest.main()
