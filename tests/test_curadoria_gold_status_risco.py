import unittest
import sys
import os
from unittest.mock import MagicMock, call

# Ensure the tests directory is in the path to import notebook_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from notebook_utils import extract_function_from_file

# Define the path to the notebook file relative to the repository root
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOK_PATH = os.path.join(
    REPO_ROOT,
    "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Curadoria_Gold.Notebook/notebook-content.py"
)

class TestStatusRiscoExpr(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print(f"Extracting get_status_risco_expr from {NOTEBOOK_PATH}")
        cls.func_source = extract_function_from_file(NOTEBOOK_PATH, "get_status_risco_expr")
        if not cls.func_source:
             raise ValueError("Function get_status_risco_expr not found in notebook.")

    def test_status_risco_logic(self):
        # Mocks
        col_mocks = {}
        def col_side_effect(name):
            if name not in col_mocks:
                m = MagicMock(name=f"col({name})")
                m.__lt__ = MagicMock(name=f"lt_mock") # Mock < operator
                m.__eq__ = MagicMock(name=f"eq_mock") # Mock == operator
                m.__and__ = MagicMock(name=f"and_mock") # Mock & operator
                col_mocks[name] = m
            return col_mocks[name]

        mock_col = MagicMock(side_effect=col_side_effect)
        mock_when = MagicMock(name="when")
        mock_current_date = MagicMock(name="current_date")

        # To chain when().when().otherwise()
        mock_when_ret = MagicMock(name="when_ret")
        mock_when.return_value = mock_when_ret
        mock_when_ret.when.return_value = mock_when_ret # Chain
        mock_when_ret.otherwise.return_value = "RESULT_COLUMN"

        # Execution globals
        exec_globals = {
            'col': mock_col,
            'when': mock_when,
            'current_date': mock_current_date,
        }

        # Load function
        local_scope = {}
        exec(self.func_source, exec_globals, local_scope)
        get_status_risco_expr = local_scope['get_status_risco_expr']

        # Test Case 1: Default args
        result = get_status_risco_expr()

        # Verify result
        self.assertEqual(result, "RESULT_COLUMN")

        # Verify calls
        self.assertTrue(mock_when.called)
        self.assertTrue(mock_current_date.called) # Should be called as default arg

        # Test Case 2: Custom args and explicit date
        mock_date_col = MagicMock(name="custom_date")
        get_status_risco_expr("my_tto", "my_venc", mock_date_col)

        # Verify col calls
        self.assertIn("my_tto", col_mocks)
        self.assertIn("my_venc", col_mocks)

        # Verify comparison: my_venc < mock_date_col
        # Note: In the function: col(col_vencimento) < current_date_col
        # So we expect col_mocks["my_venc"].__lt__ to be called with mock_date_col
        col_mocks["my_venc"].__lt__.assert_called_with(mock_date_col)

        # Verify comparison: my_tto == "RN"
        col_mocks["my_tto"].__eq__.assert_called_with("RN")

if __name__ == '__main__':
    unittest.main()
