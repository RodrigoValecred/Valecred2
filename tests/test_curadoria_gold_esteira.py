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

class TestTransformEsteiraDates(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print(f"Extracting transform_esteira_dates from {NOTEBOOK_PATH}")
        cls.func_source = extract_function_from_file(NOTEBOOK_PATH, "transform_esteira_dates")
        if not cls.func_source:
             raise ValueError("Function transform_esteira_dates not found in notebook.")

    def test_single_pass_pivot(self):
        # Mocks for Spark functions
        col_mocks = {}
        def col_side_effect(name):
            if name not in col_mocks:
                m = MagicMock(name=f"col({name})")
                # When alias is called, return a new mock but keep track?
                # Actually alias usually returns a Column object.
                m.alias = MagicMock(return_value=MagicMock(name=f"col({name}).alias"))
                col_mocks[name] = m
            return col_mocks[name]

        mock_col = MagicMock(side_effect=col_side_effect)
        mock_max = MagicMock(return_value=MagicMock(name="max_mock"))
        mock_min = MagicMock(return_value=MagicMock(name="min_mock"))

        # Setup aliases for max/min mocks so .alias("max") works
        mock_max.return_value.alias = MagicMock(return_value="max_aliased")
        mock_min.return_value.alias = MagicMock(return_value="min_aliased")

        # DataFrame Mocks
        mock_df_esteira = MagicMock()
        mock_grouped = MagicMock()
        mock_pivoted = MagicMock()
        mock_combined = MagicMock()

        # Chain
        mock_df_esteira.groupBy.return_value = mock_grouped
        mock_grouped.pivot.return_value = mock_pivoted
        mock_pivoted.agg.return_value = mock_combined

        # Select returns different mocks for max and min dfs
        mock_df_max = MagicMock(name="df_max")
        mock_df_min = MagicMock(name="df_min")
        mock_combined.select.side_effect = [mock_df_max, mock_df_min]

        # Input Data
        status_mapping = {
            "PROPOSTA": "proposta",
            "DIR COMERCIAL": "dir_comercial"
        }

        # Execution Context
        exec_globals = {
            'col': mock_col,
            'max': mock_max,
            'min': mock_min,
        }

        # Execute the function definition
        local_scope = {}
        exec(self.func_source, exec_globals, local_scope)
        transform_esteira_dates = local_scope['transform_esteira_dates']

        # Run the function
        df_combined_res = transform_esteira_dates(mock_df_esteira, status_mapping)

        # Assertions

        # 1. Verify GroupBy and Pivot
        mock_df_esteira.groupBy.assert_called_with("cod_cliente")
        mock_grouped.pivot.assert_called()
        args, _ = mock_grouped.pivot.call_args
        self.assertEqual(args[0], "status_do_cliente")
        self.assertEqual(set(args[1]), set(status_mapping.keys())) # Verify list content

        # 2. Verify Aggregation (Single Pass)
        mock_pivoted.agg.assert_called_once()
        agg_args = mock_pivoted.agg.call_args[0]
        # Should contain aliased max and min
        self.assertEqual(agg_args[0], "max_aliased")
        self.assertEqual(agg_args[1], "min_aliased")

        # 3. Verify Selection of Combined DataFrame
        self.assertEqual(mock_combined.select.call_count, 1)

        # Since we mocked col(), we need to verify the mocks
        self.assertIn("cod_cliente", col_mocks)
        self.assertIn("PROPOSTA_max", col_mocks)
        self.assertIn("DIR COMERCIAL_max", col_mocks)
        self.assertIn("PROPOSTA_min", col_mocks)
        self.assertIn("DIR COMERCIAL_min", col_mocks)

        # Verify alias calls on the mocks
        col_mocks["PROPOSTA_max"].alias.assert_called_with("pivot_proposta")
        col_mocks["DIR COMERCIAL_max"].alias.assert_called_with("pivot_dir_comercial")
        col_mocks["PROPOSTA_min"].alias.assert_called_with("min_proposta")
        col_mocks["DIR COMERCIAL_min"].alias.assert_called_with("min_dir_comercial")

        # 4. Verify Returns
        self.assertEqual(df_combined_res, mock_df_max)

if __name__ == '__main__':
    unittest.main()
