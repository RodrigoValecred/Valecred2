import unittest
from unittest.mock import MagicMock
import sys
import os

# Adiciona raiz do projeto ao sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from tests.notebook_utils import extract_function_from_file

class TestResolveColumns(unittest.TestCase):
    def test_empty_string_treated_as_null(self):
        # 1. Extract function source
        notebook_path = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/Relatorios/NB_Gold_Relatorio_Produtos_Mensal.Notebook/notebook-content.py"
        func_source = extract_function_from_file(notebook_path, "resolve_columns")

        self.assertIsNotNone(func_source, "Failed to extract function source")

        # 2. Simula funções PySpark
        mock_col = MagicMock()
        mock_trim = MagicMock()
        mock_when = MagicMock()
        mock_coalesce = MagicMock()
        mock_lit = MagicMock()

        # Define comportamentos de simulação
        mock_col_instance = MagicMock()
        mock_col.return_value = mock_col_instance

        mock_trim_instance = MagicMock()
        mock_trim.return_value = mock_trim_instance

        # trim(col) == "" -> condition
        mock_condition = MagicMock()
        mock_trim_instance.__eq__ = MagicMock(return_value=mock_condition)

        mock_when_instance = MagicMock()
        mock_when.return_value = mock_when_instance
        mock_when_instance.otherwise.return_value = "RESULT_EXPRESSION"

        mock_coalesce.return_value = "FINAL_COALESCE"

        # 3. Cria Simulação de DataFrame
        mock_df = MagicMock()
        mock_df.columns = ["mycol", "mycol_op"]
        mock_df.withColumn.return_value = mock_df

        # 4. Exec globals
        exec_globals = {
            "col": mock_col,
            "trim": mock_trim,
            "when": mock_when,
            "coalesce": mock_coalesce,
            "lit": mock_lit
        }

        # 5. Executa function definition
        try:
            exec(func_source, exec_globals)
        except Exception as e:
            self.fail(f"Failed to execute extracted function source: {e}")

        resolve_columns = exec_globals["resolve_columns"]

        # 6. Call function
        result_df = resolve_columns(mock_df, ["mycol"])

        # 7. Asserções

        # Verifica se trim(col("mycol")) foi chamado
        # mock_col("mycol") -> mock_col_instance
        # mock_trim(mock_col_instance) -> mock_trim_instance
        mock_col.assert_any_call("mycol")
        mock_trim.assert_any_call(mock_col_instance)

        # Verifica condição: trim(...) == ""
        mock_trim_instance.__eq__.assert_called_with("")

        # Verifica when(condição, None)
        # when(mock_condition, None)
        mock_when.assert_any_call(mock_condition, None)

        # Verifica otherwise(col("mycol"))
        mock_when_instance.otherwise.assert_called_with(mock_col_instance)

        # Verifica coalesce(RESULT_EXPRESSION, col("mycol_op"))
        # Precisamos verificar se col("mycol_op") também foi chamado
        mock_col.assert_any_call("mycol_op")

        # Verifica a chamada withColumn com o resultado de coalesce correto
        mock_df.withColumn.assert_called_with("mycol", "FINAL_COALESCE")

if __name__ == "__main__":
    unittest.main()
