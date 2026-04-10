import unittest
import sys
import os
from unittest.mock import MagicMock, patch

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from notebook_utils import extract_function_from_file

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOK_PATH = os.path.join(
    REPO_ROOT,
    "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Silver_Carteira_PDD.Notebook/notebook-content.py"
)

class TestSafeLoadTable(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print(f"Extracting safe_load_table from {NOTEBOOK_PATH}")
        cls.func_source = extract_function_from_file(NOTEBOOK_PATH, "safe_load_table")
        if not cls.func_source:
             raise ValueError("Function safe_load_table not found in notebook.")

    def setUp(self):
        self.spark = MagicMock()

        # Globais de execução
        exec_globals = {
            'spark': self.spark
        }

        # Carrega a função
        local_scope = {}
        exec(self.__class__.func_source, exec_globals, local_scope)
        self.safe_load_table = local_scope['safe_load_table']

    def test_safe_load_table_success(self):
        expected_df = MagicMock()
        self.spark.read.table.return_value = expected_df

        result = self.safe_load_table("minha_tabela", MagicMock())

        self.spark.read.table.assert_called_once_with("minha_tabela")
        self.assertEqual(result, expected_df)

    @patch('builtins.print')
    def test_safe_load_table_fallback(self, mock_print):
        self.spark.read.table.side_effect = Exception("TABLE_OR_VIEW_NOT_FOUND error")
        schema = MagicMock()
        empty_df = MagicMock()
        self.spark.createDataFrame.return_value = empty_df

        result = self.safe_load_table("minha_tabela", schema)

        self.spark.read.table.assert_called_once_with("minha_tabela")
        self.spark.createDataFrame.assert_called_once_with([], schema)
        self.assertEqual(result, empty_df)
        mock_print.assert_any_call("AVISO: Tabela de dependência 'minha_tabela' não encontrada. Usando um DataFrame vazio.")

    def test_safe_load_table_raises_other_exception(self):
        error = Exception("Other error")
        self.spark.read.table.side_effect = error
        schema = MagicMock()

        with self.assertRaises(Exception) as context:
            self.safe_load_table("minha_tabela", schema)

        self.spark.read.table.assert_called_once_with("minha_tabela")
        self.assertEqual(str(context.exception), "Other error")

if __name__ == "__main__":
    unittest.main()
