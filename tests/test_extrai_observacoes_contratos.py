import unittest
import sys
import os

try:
    from tests.notebook_utils import extract_function_from_file
except ImportError:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from notebook_utils import extract_function_from_file

NOTEBOOK_PATH = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Extrai_Observacoes_Contratos.Notebook/notebook-content.py"

class MockColumn:
    def __init__(self, name=None, expr=None):
        self.name = name
        self.expr = expr

    def cast(self, type_name):
        return MockColumn(expr=f"cast({self.expr or self.name} as {type_name})")

    def __repr__(self):
        return self.expr if self.expr else f"col({self.name})"

    def __eq__(self, other):
        return str(self) == str(other)

def mock_col(name):
    return MockColumn(name=name)

def mock_regexp_replace(col_obj, pattern, replacement):
    return MockColumn(expr=f"regexp_replace({col_obj}, '{pattern}', '{replacement}')")

class TestConverterMoedaBr(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print(f"Extracting converter_moeda_br from {NOTEBOOK_PATH}")
        func_source = extract_function_from_file(NOTEBOOK_PATH, "converter_moeda_br")

        if func_source:
            # Create a context with mock functions to be used as globals during function execution
            context = {
                "col": mock_col,
                "regexp_replace": mock_regexp_replace,
                # 'lit' is imported in the notebook but not used in this specific function,
                # but good to have if future changes use it.
                "lit": lambda x: MockColumn(expr=f"lit({x})")
            }
            local_scope = {}
            # Execute the function definition. The function body is NOT executed here.
            # The function object is created and bound to 'converter_moeda_br' in local_scope.
            # It captures 'context' as its global scope because we passed it as 'globals'.
            exec(func_source, context, local_scope)
            cls.converter_moeda_br = staticmethod(local_scope["converter_moeda_br"])
        else:
            cls.converter_moeda_br = None
            print("WARNING: converter_moeda_br function not found in file.")

    def test_function_exists(self):
        self.assertIsNotNone(self.converter_moeda_br, "Function converter_moeda_br not found in notebook file.")

    def test_converter_moeda_br_structure(self):
        if not self.converter_moeda_br: self.skipTest("Function not found")

        result = self.converter_moeda_br("valor_br")

        # The function logic:
        # 1. regexp_replace(col(col_name), "\.", "")  -> remove thousands separator
        # 2. regexp_replace(..., ",", ".")            -> replace decimal comma with dot
        # 3. .cast("double")                          -> cast to double

        # Expected string representation from our mocks:
        # cast(regexp_replace(regexp_replace(col(valor_br), '\.', ''), ',', '.') as double)

        # Note on backslashes:
        # In the source code: "\." (backslash + dot).
        # In our mock: it becomes the string literal '\.'.
        # In expected string below: we need to escape the backslash for the python string literal.
        expected_expr = "cast(regexp_replace(regexp_replace(col(valor_br), '\\.', ''), ',', '.') as double)"

        self.assertIsInstance(result, MockColumn)
        self.assertEqual(str(result), expected_expr)

if __name__ == '__main__':
    unittest.main()
