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
    "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Prepara_Tabela_Contabil.Notebook/notebook-content.py"
)

class TestPreparaTabelaContabilSelect(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Mock class for PySpark 'col'
        class MockColumn(str):
            def __new__(cls, name, cast_type=None, alias_name=None):
                obj = super().__new__(cls, name)
                obj.name = name
                obj.cast_type = cast_type
                obj.alias_name = alias_name
                return obj

            def alias(self, name):
                return MockColumn(self.name, self.cast_type, name)

            def cast(self, data_type):
                return MockColumn(self.name, data_type, self.alias_name)

            def __eq__(self, other):
                if isinstance(other, MockColumn):
                    return (self.name == other.name and
                            self.cast_type == other.cast_type and
                            self.alias_name == other.alias_name)
                return False

            def __repr__(self):
                repr_str = f"col('{self.name}')"
                if self.cast_type:
                    repr_str += f".cast('{self.cast_type}')"
                if self.alias_name:
                    repr_str += f".alias('{self.alias_name}')"
                return repr_str

        def mock_col(name):
            return MockColumn(name)

        cls.MockColumn = MockColumn
        cls.mock_col = mock_col

        exec_globals = {
            'col': cls.mock_col
        }

        # Extrai a função e passa o escopo com o col mockado
        cls.func_source = extract_function_from_file(NOTEBOOK_PATH, "select_lancamentos")
        if not cls.func_source:
             raise ValueError("Function select_lancamentos not found in notebook.")

        local_scope = {}
        exec(cls.func_source, exec_globals, local_scope)
        cls.select_lancamentos = local_scope['select_lancamentos']

    def test_select_lancamentos(self):
        # Configuração
        df_mock = MagicMock()
        df_mock.select.return_value = "mocked_dataframe_result"

        # Execução
        # Since we load the function with exec, it behaves as a normal function, not a bound method.
        # But we attached it to cls, so calling self.select_lancamentos passes `self` implicitly.
        # To fix this, we should call it via its class or simply store it globally or not call it as a bound method.

        # We can extract it from the class
        func = self.__class__.select_lancamentos

        result = func(df_mock)

        # Verificação do retorno
        self.assertEqual(result, "mocked_dataframe_result")

        # Verificação se o select foi chamado com as colunas corretas
        expected_calls = [
            self.MockColumn("CODCTBLAN").alias("cod_lancamento"),
            self.MockColumn("CODEMPRESA").alias("cod_empresa"),
            self.MockColumn("CODTRANSACAO").alias("cod_transacao"),
            self.MockColumn("DEBITO").alias("debito"),
            self.MockColumn("CREDITO").alias("credito"),
            self.MockColumn("CODFUNDO").alias("cod_fundo"),
            self.MockColumn("CODCCUSTO").alias("cod_ccusto"),
            self.MockColumn("TIPO").alias("tipo"),
            self.MockColumn("DATA").alias("data_lancamento"),
            self.MockColumn("VALOR").alias("valor"),
            self.MockColumn("COMPLEMENTO").cast("string").alias("complemento"),
            self.MockColumn("SISTEMA").alias("sistema"),
            self.MockColumn("DATAINCLUSAO").alias("data_inclusao"),
            self.MockColumn("USUAINCLUSAO").alias("usuario_inclusao"),
            self.MockColumn("DATAALTERACAO").alias("data_alteracao"),
            self.MockColumn("USUAALTERACAO").alias("usuario_alteracao")
        ]

        df_mock.select.assert_called_once_with(*expected_calls)

if __name__ == '__main__':
    unittest.main()
