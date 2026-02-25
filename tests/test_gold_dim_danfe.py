import unittest
import sys
import os
from unittest.mock import MagicMock

# Ensure the tests directory is in the path to import notebook_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from notebook_utils import extract_function_from_file

# Define the path to the notebook file relative to the repository root
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOK_PATH = os.path.join(
    REPO_ROOT,
    "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Gold_Dim_Danfe.Notebook/notebook-content.py"
)

class TestParseDanfe(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print(f"Extracting parse_danfe from {NOTEBOOK_PATH}")
        cls.func_source = extract_function_from_file(NOTEBOOK_PATH, "parse_danfe")
        if not cls.func_source:
             raise ValueError("Function parse_danfe not found in notebook.")

    def test_parse_danfe_calls(self):
        # Mocks for PySpark functions
        mock_col = MagicMock(name="col")
        mock_substring = MagicMock(name="substring")

        # Mock Column behavior
        def col_side_effect(name):
            m = MagicMock(name=f"col('{name}')")
            return m

        mock_col.side_effect = col_side_effect

        # Mock DataFrame
        mock_df = MagicMock(name="df")

        # Chainable withColumn
        mock_df.withColumn.return_value = mock_df
        # Chainable withColumnRenamed
        mock_df.withColumnRenamed.return_value = mock_df

        # Execution context
        exec_globals = {
            'col': mock_col,
            'substring': mock_substring,
        }

        # Execute the function definition
        local_scope = {}
        exec(self.func_source, exec_globals, local_scope)
        parse_danfe = local_scope['parse_danfe']

        # Call the function
        result_df = parse_danfe(mock_df)

        # Assertions

        # 1. Verify expected columns were added
        expected_cols = {
            "uf": (1, 2),
            "aamm": (3, 4),
            "cnpj": (7, 14),
            "modelo": (21, 2),
            "serie": (23, 3),
            "numero_nf": (26, 9),
            "codigo_nf": (35, 9),
            "dv": (44, 1)
        }

        # Get all calls to withColumn
        self.assertEqual(mock_df.withColumn.call_count, len(expected_cols))

        calls = mock_df.withColumn.call_args_list

        # Collect columns added
        added_cols = [c[0][0] for c in calls]
        for col_name in expected_cols:
            self.assertIn(col_name, added_cols)

        # 2. Verify substring calls
        self.assertEqual(mock_substring.call_count, len(expected_cols))

        substring_calls = mock_substring.call_args_list

        # Check that we have a substring call corresponding to each expected column param
        # We can't easily link substring call to withColumn call without more complex mocking,
        # but we can verify the set of substring calls matches our expectations.

        expected_params = list(expected_cols.values()) # List of (start, len)

        found_params = []
        for call_args in substring_calls:
            args, _ = call_args
            # args[0] is col object, args[1] is start, args[2] is length
            found_params.append((args[1], args[2]))

        # Sort both lists to compare
        expected_params.sort()
        found_params.sort()

        self.assertEqual(expected_params, found_params, "Mismatch in substring parameters")

        # 3. Verify withColumnRenamed
        mock_df.withColumnRenamed.assert_called_once_with("CHAVEDANFE", "chave_danfe")

        # 4. Verify col was called with CHAVEDANFE at least once
        # In reality it's called for every substring call.
        col_calls = mock_col.call_args_list
        for call_args in col_calls:
            self.assertEqual(call_args[0][0], "CHAVEDANFE")

if __name__ == '__main__':
    unittest.main()
