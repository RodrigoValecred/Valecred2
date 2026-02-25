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

class TestVOPMetrics(unittest.TestCase):

    def test_calculate_vop_metrics(self):
        func_source = extract_function_from_file(NOTEBOOK_PATH, "calculate_vop_metrics")
        if not func_source:
             self.fail("Function calculate_vop_metrics not found in notebook.")

        # Mocks
        mock_df_ops = MagicMock(name="df_ops_validas")
        mock_df_grouped = MagicMock(name="df_grouped")
        mock_df_agg = MagicMock(name="df_agg")
        mock_window_spec = MagicMock(name="window_spec")

        # Setup chain for groupBy(...).agg(...)
        mock_df_ops.groupBy.return_value = mock_df_grouped
        mock_df_grouped.agg.return_value = mock_df_agg

        # Setup chain for withColumn(...).filter(...).select(...)
        mock_df_with_rn = MagicMock(name="df_with_rn")
        mock_df_filtered = MagicMock(name="df_filtered")
        mock_df_final = MagicMock(name="df_final")

        mock_df_agg.withColumn.return_value = mock_df_with_rn
        mock_df_with_rn.filter.return_value = mock_df_filtered
        mock_df_filtered.select.return_value = mock_df_final

        # Mock PySpark functions
        mock_col = MagicMock(name="col")
        mock_sum = MagicMock(name="sum")
        mock_row_number = MagicMock(name="row_number")
        mock_window = MagicMock(name="Window")

        # Mock Window.partitionBy(...).orderBy(...)
        mock_window.partitionBy.return_value.orderBy.return_value = mock_window_spec

        # Execution Context
        exec_globals = {
            'col': mock_col,
            'sum': mock_sum,
            'row_number': mock_row_number,
            'Window': mock_window,
        }

        local_scope = {}
        exec(func_source, exec_globals, local_scope)
        calculate_vop_metrics = local_scope['calculate_vop_metrics']

        # Run function
        result_week, result_month = calculate_vop_metrics(mock_df_ops)

        # Assertions
        # 1. Check groupBy calls using existing columns
        # Expected: groupBy("cod_cliente", col("dia_da_semana_da_operacao").alias("dia_semana"))
        # and groupBy("cod_cliente", col("dia_da_operacao").alias("dia_mes"))

        self.assertEqual(mock_df_ops.groupBy.call_count, 2)

        # Inspect args for first groupBy (Day of Week)
        args_week, _ = mock_df_ops.groupBy.call_args_list[0]
        self.assertEqual(args_week[0], "cod_cliente")
        # Verify alias usage
        mock_col.assert_any_call("dia_da_semana_da_operacao")

        # Inspect args for second groupBy (Day of Month)
        args_month, _ = mock_df_ops.groupBy.call_args_list[1]
        self.assertEqual(args_month[0], "cod_cliente")
        mock_col.assert_any_call("dia_da_operacao")

        # 2. Check returns
        self.assertEqual(result_week, mock_df_final)
        self.assertEqual(result_month, mock_df_final)

if __name__ == '__main__':
    unittest.main()
