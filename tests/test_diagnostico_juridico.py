import unittest
from unittest.mock import MagicMock, patch
import sys
import os

# Ensure tests package is in path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from tests.notebook_utils import extract_function_from_file
except ImportError:
    from notebook_utils import extract_function_from_file

NOTEBOOK_PATH = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Utilitarios/NB_Diagnostico_Juridico.Notebook/notebook-content.py"

class TestDiagnosticoJuridico(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print(f"Extracting check_silver_titulos from {NOTEBOOK_PATH}")
        cls.func_source = extract_function_from_file(NOTEBOOK_PATH, "check_silver_titulos")
        if not cls.func_source:
             print("WARNING: check_silver_titulos function not found in file.")

    def setUp(self):
        if not self.func_source:
            self.skipTest("Function not found")

        # Prepare scope
        def create_mock_col(name):
            m = MagicMock()
            # We need alias to return something that can be passed to agg
            # agg accepts Column objects.
            # So alias should return a MagicMock too (or a string if agg mocks accept strings)
            # Let's return a string for simplicity in debugging, but since agg is mocked, it doesn't matter what it returns as long as it returns *something*.
            # But wait, alias() is called on the result of max().
            m.alias.return_value = f"{name}_aliased"
            return m

        self.mock_col = MagicMock(side_effect=lambda x: create_mock_col(f"col({x})"))
        self.mock_max = MagicMock(side_effect=lambda x: create_mock_col(f"max({x})"))
        self.mock_count = MagicMock(side_effect=lambda x: create_mock_col(f"count({x})"))
        self.mock_lit = MagicMock(side_effect=lambda x: create_mock_col(f"lit({x})"))

        local_scope = {}
        global_scope = {
            "col": self.mock_col,
            "max": self.mock_max,
            "count": self.mock_count,
            "lit": self.mock_lit
        }

        exec(self.func_source, global_scope, local_scope)
        self.check_silver_titulos = local_scope["check_silver_titulos"]

    def test_check_silver_titulos_success(self):
        """Test happy path where table is read and stats calculated."""
        spark = MagicMock()
        df_titulos = MagicMock()

        # Mock reading table
        spark.read.table.return_value = df_titulos

        # Mock aggregation and collection
        # silver_stats = df_titulos.agg(...).collect()[0]
        # We need agg(...) to return a DF, and collect() to return a list of Rows
        mock_stats_df = MagicMock()
        df_titulos.agg.return_value = mock_stats_df

        mock_row = {'total_titulos': 100, 'max_data_inclusao': '2023-01-01'}
        mock_stats_df.collect.return_value = [mock_row]

        # Run function
        result = self.check_silver_titulos(spark)

        # Verify
        spark.read.table.assert_called_with("LH_Silver.staging_titulos_limpa")
        self.assertEqual(result, df_titulos)

    def test_check_silver_titulos_error(self):
        """Test error handling when table read fails."""
        spark = MagicMock()

        # Mock error
        spark.read.table.side_effect = Exception("Table not found")

        # Run function
        # We expect it to print error and return None
        # To suppress print output in test, we could patch builtins.print, but it's not strictly necessary unless we want to assert on it.
        # Let's assert on it.

        with patch('builtins.print') as mock_print:
            result = self.check_silver_titulos(spark)

            # Verify exception was caught
            self.assertIsNone(result)

            # Verify error message was printed
            # Check if any call contained "ERRO"
            found_error_msg = False
            for call_args in mock_print.call_args_list:
                args, _ = call_args
                if args and "ERRO ao ler Silver Títulos" in str(args[0]):
                    found_error_msg = True
                    break
            self.assertTrue(found_error_msg, "Error message not printed")

if __name__ == '__main__':
    unittest.main()
