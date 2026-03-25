import unittest
import sys
import os
from unittest.mock import MagicMock

# Garante que o diretório tests esteja no path para importar notebook_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from notebook_utils import extract_function_from_file

# Define o caminho para o arquivo do notebook em relação à raiz do repositório
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOK_PATH = os.path.join(
    REPO_ROOT,
    "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Curadoria_Gold.Notebook/notebook-content.py"
)

class TestEscrowLoading(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        func_source = extract_function_from_file(NOTEBOOK_PATH, "get_escrow_data")
        if not func_source:
             raise ValueError("Function get_escrow_data not found in notebook.")

        # Mocks para o ambiente de execução da função
        cls.max_mock = MagicMock()
        cls.struct_type_mock = MagicMock()
        cls.struct_field_mock = MagicMock()
        cls.long_type_mock = MagicMock()
        cls.boolean_type_mock = MagicMock()

        exec_globals = {
            'max': cls.max_mock,
            'StructType': cls.struct_type_mock,
            'StructField': cls.struct_field_mock,
            'LongType': cls.long_type_mock,
            'BooleanType': cls.boolean_type_mock,
        }

        local_scope = {}
        exec(func_source, exec_globals, local_scope)
        cls.get_escrow_data = staticmethod(local_scope['get_escrow_data'])

    def setUp(self):
        self.spark = MagicMock()

    def test_get_escrow_data_success(self):
        # Configura o mock do spark para retornar um dataframe
        mock_df = MagicMock()
        self.spark.read.table.return_value = mock_df

        # O encadeamento de chamadas: .groupBy().agg()
        mock_grouped = MagicMock()
        mock_df.groupBy.return_value = mock_grouped

        expected_df = MagicMock()
        mock_grouped.agg.return_value = expected_df

        # Chamada da função
        result = self.get_escrow_data(self.spark, "tabela_escrow")

        # Verificações
        self.spark.read.table.assert_called_once_with("tabela_escrow")
        mock_df.groupBy.assert_called_once_with("cod_operacao")
        self.assertEqual(result, expected_df)

    def test_get_escrow_data_fallback(self):
        # Configura o mock do spark para lançar exceção na leitura
        self.spark.read.table.side_effect = Exception("Tabela não encontrada")

        # Mock do DataFrame vazio retornado pelo spark.createDataFrame
        empty_df = MagicMock()
        self.spark.createDataFrame.return_value = empty_df

        # Chamada da função
        result = self.get_escrow_data(self.spark, "tabela_inexistente")

        # Verificações
        self.spark.read.table.assert_called_once_with("tabela_inexistente")
        self.spark.createDataFrame.assert_called_once()
        self.assertEqual(result, empty_df)

        # Verifica se o schema foi construído corretamente (usando os mocks injetados em setUpClass)
        self.assertTrue(self.struct_type_mock.called)

if __name__ == '__main__':
    unittest.main()
