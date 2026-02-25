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
    "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Curadoria_Shared.Notebook/notebook-content.py"
)

class TestDeduplicateClientes(unittest.TestCase):

    def test_deduplication_logic(self):
        func_source = extract_function_from_file(NOTEBOOK_PATH, "deduplicate_clientes_staging")
        if not func_source:
             self.fail("Function deduplicate_clientes_staging not found in notebook.")

        # Mocks
        mock_df = MagicMock(name="df")

        # Mock Column expressions
        mock_col = MagicMock(name="col_fn")
        mock_col_obj = MagicMock(name="col_obj")
        mock_col.return_value = mock_col_obj

        # Mock desc() call
        mock_col_obj.desc.return_value = "DESC_ORDER"

        # Mock __eq__ for filter(col("rn") == 1)
        mock_filter_expr = MagicMock(name="filter_expr")
        mock_col_obj.__eq__.return_value = mock_filter_expr

        # Mock Window
        mock_window = MagicMock(name="Window")
        mock_window_spec = MagicMock(name="WindowSpec")
        mock_window.partitionBy.return_value = mock_window_spec
        mock_window_spec.orderBy.return_value = "WINDOW_SPEC"

        # Mock row_number
        mock_row_number = MagicMock(name="row_number")
        mock_row_number_obj = MagicMock(name="row_number_obj")
        mock_row_number.return_value = mock_row_number_obj
        mock_row_number_obj.over.return_value = "ROW_NUMBER_COL"

        # Execution Context
        exec_globals = {
            'col': mock_col,
            'Window': mock_window,
            'row_number': mock_row_number,
        }

        local_scope = {}
        exec(func_source, exec_globals, local_scope)
        deduplicate_clientes_staging = local_scope['deduplicate_clientes_staging']

        # Setup DataFrame method chains
        # df.withColumn(..).filter(..).drop(..)
        mock_df_with_col = MagicMock(name="df_with_col")
        mock_df.withColumn.return_value = mock_df_with_col

        mock_df_filtered = MagicMock(name="df_filtered")
        mock_df_with_col.filter.return_value = mock_df_filtered

        mock_df_final = MagicMock(name="df_final")
        mock_df_filtered.drop.return_value = mock_df_final

        # Run function
        result = deduplicate_clientes_staging(mock_df)

        # Assertions
        # 1. Window Specification
        mock_window.partitionBy.assert_called_with("cpf_cnpj")
        # Ensure orderBy was called with desc orders
        mock_window_spec.orderBy.assert_called()

        # 2. withColumn "rn"
        mock_df.withColumn.assert_called_with("rn", "ROW_NUMBER_COL")

        # 3. filter
        # filter(col("rn") == 1) -> filter(mock_filter_expr)
        mock_df_with_col.filter.assert_called_with(mock_filter_expr)

        # 4. drop "rn"
        mock_df_filtered.drop.assert_called_with("rn")

        self.assertEqual(result, mock_df_final)

if __name__ == '__main__':
    unittest.main()
