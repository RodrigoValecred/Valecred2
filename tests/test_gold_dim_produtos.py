import unittest
import sys
import os
from unittest.mock import MagicMock, patch

# Ensure the tests directory is in the path to import notebook_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from notebook_utils import extract_function_from_file

# Define the path to the notebook file relative to the repository root
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOK_PATH = os.path.join(
    REPO_ROOT,
    "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Gold_Dim_Produtos.Notebook/notebook-content.py"
)

class TestIncorporarProdutosAusentes(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print(f"Extracting incorporar_produtos_ausentes from {NOTEBOOK_PATH}")
        cls.func_source = extract_function_from_file(NOTEBOOK_PATH, "incorporar_produtos_ausentes")
        if not cls.func_source:
             raise ValueError("Function incorporar_produtos_ausentes not found in notebook.")

    def test_incorporar_produtos_ausentes_success(self):
        # Mocks for PySpark functions
        mock_col = MagicMock(name="col")
        mock_trim = MagicMock(name="trim")
        mock_broadcast = MagicMock(name="broadcast")
        mock_coalesce = MagicMock(name="coalesce")
        mock_lit = MagicMock(name="lit")

        # Mock Column behavior
        def col_side_effect(name):
            return MagicMock(name=f"col('{name}')")

        mock_col.side_effect = col_side_effect

        # Mock Spark Session and DataFrames
        mock_spark = MagicMock(name="spark")
        mock_df_calc = MagicMock(name="df_calc")
        mock_df_ausentes = MagicMock(name="df_ausentes")

        mock_spark.read.table.return_value = mock_df_ausentes
        mock_df_ausentes.select.return_value = mock_df_ausentes

        # Chainable methods
        mock_df_calc.join.return_value = mock_df_calc
        mock_df_calc.withColumn.return_value = mock_df_calc
        mock_df_calc.drop.return_value = mock_df_calc

        # Execution context
        exec_globals = {
            'col': mock_col,
            'trim': mock_trim,
            'broadcast': mock_broadcast,
            'coalesce': mock_coalesce,
            'lit': mock_lit
        }

        # Execute the function definition
        local_scope = {}
        exec(self.func_source, exec_globals, local_scope)
        incorporar_produtos_ausentes = local_scope['incorporar_produtos_ausentes']

        # Call the function
        result_df = incorporar_produtos_ausentes(mock_spark, mock_df_calc)

        # Assertions
        mock_spark.read.table.assert_called_with("LH_Silver.sup_produtos_ausentes")
        mock_df_calc.join.assert_called()
        self.assertEqual(result_df, mock_df_calc) # Since we mocked chain calls to return mock_df_calc

    def test_incorporar_produtos_ausentes_failure(self):
        # Mocks
        mock_spark = MagicMock(name="spark")
        mock_df_calc = MagicMock(name="df_calc")

        # Raise exception when reading table
        mock_spark.read.table.side_effect = Exception("Table not found")

        # Execution context (still need imports even if not used in failure path if python parses them)
        exec_globals = {
            'col': MagicMock(),
            'trim': MagicMock(),
            'broadcast': MagicMock(),
            'coalesce': MagicMock(),
            'lit': MagicMock()
        }

        # Execute the function definition
        local_scope = {}
        exec(self.func_source, exec_globals, local_scope)
        incorporar_produtos_ausentes = local_scope['incorporar_produtos_ausentes']

        # Capture stdout to verify print
        with patch('builtins.print') as mock_print:
            result_df = incorporar_produtos_ausentes(mock_spark, mock_df_calc)

            # Assertions
            mock_spark.read.table.assert_called_with("LH_Silver.sup_produtos_ausentes")
            self.assertEqual(result_df, mock_df_calc) # Should return original df

            # Verify warning was printed
            mock_print.assert_called()
            args, _ = mock_print.call_args
            self.assertIn("Aviso: Não foi possível carregar ou utilizar LH_Silver.sup_produtos_ausentes", args[0])

if __name__ == '__main__':
    unittest.main()
