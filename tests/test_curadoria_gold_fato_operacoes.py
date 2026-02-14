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

class TestCreateFatoOperacoes(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print(f"Extracting create_fato_operacoes from {NOTEBOOK_PATH}")
        cls.func_source = extract_function_from_file(NOTEBOOK_PATH, "create_fato_operacoes")
        if not cls.func_source:
             raise ValueError("Function create_fato_operacoes not found in notebook.")

    def test_sk_produto_join(self):
        # Dictionary to store mocks created by col()
        col_mocks = {}

        def col_side_effect(name):
            if name not in col_mocks:
                m = MagicMock(name=f"col({name})")
                # Support ~col(...)
                m.__invert__ = MagicMock(return_value=MagicMock(name=f"~col({name})"))
                col_mocks[name] = m
            return col_mocks[name]

        mock_col = MagicMock(side_effect=col_side_effect)
        mock_lit = MagicMock(return_value=MagicMock(name="lit_mock"))
        mock_to_date = MagicMock(return_value=MagicMock(name="to_date_mock"))
        mock_date_format = MagicMock(return_value=MagicMock(name="date_format_mock"))
        mock_broadcast = MagicMock(side_effect=lambda x: x)

        # DataFrame Mocks
        mock_df_ops = MagicMock()
        mock_df_prod = MagicMock()

        # Setup method chaining mocks
        mock_df_prep = MagicMock()
        mock_df_ops.withColumn.return_value = mock_df_prep # data_join_calendario

        mock_df_prep_2 = MagicMock()
        mock_df_prep.withColumn.return_value = mock_df_prep_2 # sk_operacao

        mock_df_prep_3 = MagicMock()
        mock_df_prep_2.withColumn.return_value = mock_df_prep_3 # sk_data (Optimization)

        mock_df_joined_1 = MagicMock()
        mock_df_prep_3.join.return_value = mock_df_joined_1 # join with dim_produto

        mock_df_filtered = MagicMock()
        mock_df_joined_1.filter.return_value = mock_df_filtered

        mock_df_selected = MagicMock()
        mock_df_filtered.select.return_value = mock_df_selected

        mock_df_final = MagicMock()
        mock_df_selected.dropDuplicates.return_value = mock_df_final

        # Execution Context (Globals)
        exec_globals = {
            'col': mock_col,
            'lit': mock_lit,
            'to_date': mock_to_date,
            'date_format': mock_date_format,
            'broadcast': mock_broadcast,
        }
        # Add other potential functions as dummy mocks
        for func_name in ['when', 'length', 'regexp_replace', 'collect_list', 'concat_ws', 'upper', 'greatest',
                          'substring', 'year', 'lead', 'date_add', 'lag', 'max', 'coalesce', 'dayofweek',
                          'dayofmonth', 'date_sub', 'trim', 'datediff', 'sum', 'min', 'count', 'round',
                          'floor', 'least', 'current_date', 'split', 'pow', 'xxhash64']:
             exec_globals[func_name] = MagicMock()

        # Execute the function definition
        local_scope = {}
        exec(self.func_source, exec_globals, local_scope)
        create_fato_operacoes = local_scope['create_fato_operacoes']

        # Run the function
        result = create_fato_operacoes(mock_df_ops, mock_df_prod)

        # Assertions

        # 1. Verify that df_dim_produto.select was called with "chave_produto" and "sk_produto"
        mock_df_prod.select.assert_called_with("chave_produto", "sk_produto")

        # 2. Verify join
        mock_df_prep_3.join.assert_called()
        args, kwargs = mock_df_prep_3.join.call_args
        self.assertEqual(args[1], "chave_produto")
        self.assertEqual(args[2], "left")

        # 3. Verify Select includes sk_produto and sk_data
        mock_df_filtered.select.assert_called()
        call_args = mock_df_filtered.select.call_args[0]

        self.assertIn("sk_produto", col_mocks)
        sk_prod_mock = col_mocks["sk_produto"]
        self.assertIn(sk_prod_mock, call_args)

        self.assertIn("sk_operacao", col_mocks)
        sk_op_mock = col_mocks["sk_operacao"]
        self.assertIn(sk_op_mock, call_args)

        self.assertIn("sk_data", col_mocks)
        sk_data_mock = col_mocks["sk_data"]
        self.assertIn(sk_data_mock, call_args)

        # Verify date_format was called
        mock_date_format.assert_called()

if __name__ == '__main__':
    unittest.main()
