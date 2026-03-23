import unittest
import sys
import os
from unittest.mock import MagicMock

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from notebook_utils import extract_function_from_file

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOK_PATH = os.path.join(
    REPO_ROOT,
    "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Curadoria_Gold.Notebook/notebook-content.py"
)

class TestSafeReadTable(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print(f"Extracting safe_read_table from {NOTEBOOK_PATH}")
        cls.func_source = extract_function_from_file(NOTEBOOK_PATH, "safe_read_table")
        if not cls.func_source:
             raise ValueError("Function safe_read_table not found in notebook.")

        # Executa o código da função no namespace atual para que possamos chamá-la
        exec(cls.func_source, globals())

    def setUp(self):
        self.spark = MagicMock()

    def test_safe_read_table_success(self):
        # Configura o mock do spark para retornar um dataframe de sucesso
        expected_df = MagicMock()
        self.spark.read.table.return_value = expected_df

        result = safe_read_table(self.spark, "minha_tabela")

        self.spark.read.table.assert_called_once_with("minha_tabela")
        self.assertEqual(result, expected_df)

    def test_safe_read_table_fallback_df(self):
        # Configura o mock do spark para lançar exceção
        self.spark.read.table.side_effect = Exception("Tabela não encontrada")
        fallback_df = MagicMock()

        result = safe_read_table(self.spark, "minha_tabela", fallback_df=fallback_df)

        self.spark.read.table.assert_called_once_with("minha_tabela")
        self.assertEqual(result, fallback_df)

    def test_safe_read_table_fallback_schema(self):
        # Configura o mock do spark para lançar exceção
        self.spark.read.table.side_effect = Exception("Tabela não encontrada")
        schema = MagicMock()
        empty_df = MagicMock()
        self.spark.createDataFrame.return_value = empty_df

        result = safe_read_table(self.spark, "minha_tabela", schema=schema)

        self.spark.read.table.assert_called_once_with("minha_tabela")
        self.spark.createDataFrame.assert_called_once_with([], schema=schema)
        self.assertEqual(result, empty_df)

    def test_safe_read_table_raises_exception(self):
        # Configura o mock do spark para lançar exceção
        error = Exception("Tabela não encontrada")
        self.spark.read.table.side_effect = error

        with self.assertRaises(Exception) as context:
            safe_read_table(self.spark, "minha_tabela")

        self.spark.read.table.assert_called_once_with("minha_tabela")
        self.assertEqual(str(context.exception), "Tabela não encontrada")

if __name__ == "__main__":
    unittest.main()
