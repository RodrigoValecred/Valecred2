import unittest
from unittest.mock import MagicMock, patch
import sys
import os

# Garante que o pacote tests está no path
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
            # Precisamos que alias retorne algo que possa ser passado para agg
            # agg aceita objetos Column.
            # Portanto, alias deve retornar um MagicMock também (ou uma string se os mocks de agg aceitarem strings)
            # Vamos retornar uma string para simplificar a depuração, mas como agg é um mock, não importa o que ele retorna, desde que retorne *algo*.
            # Mas espere, alias() é chamado no resultado de max().
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
        """Testa o caminho feliz onde a tabela é lida e as estatísticas calculadas."""
        spark = MagicMock()
        df_titulos = MagicMock()

        # Simula leitura da tabela
        spark.read.table.return_value = df_titulos

        # Simula agregação e coleta
        # silver_stats = df_titulos.agg(...).collect()[0]
        # Precisamos que agg(...) retorne um DF e collect() retorne uma lista de Rows
        mock_stats_df = MagicMock()
        df_titulos.agg.return_value = mock_stats_df

        mock_row = {'total_titulos': 100, 'max_data_inclusao': '2023-01-01'}
        mock_stats_df.collect.return_value = [mock_row]

        # Executa função
        result = self.check_silver_titulos(spark)

        # Verifica
        spark.read.table.assert_called_with("LH_Silver.staging_titulos_limpa")
        self.assertEqual(result, df_titulos)

    def test_check_silver_titulos_error(self):
        """Test error handling when table read fails."""
        spark = MagicMock()

        # Simula erro
        spark.read.table.side_effect = Exception("Table not found")

        # Executa função
        # Esperamos que imprima erro e retorne None
        # Para suprimir a saída de impressão no teste, poderíamos fazer um patch em builtins.print, mas não é estritamente necessário a menos que queiramos fazer asserções sobre isso.
        # Vamos fazer asserção sobre isso.

        with patch('builtins.print') as mock_print:
            result = self.check_silver_titulos(spark)

            # Verifica se a exceção foi capturada
            self.assertIsNone(result)

            # Verifica se a mensagem de erro foi impressa
            # Verifica se alguma chamada conteve "ERRO"
            found_error_msg = False
            for call_args in mock_print.call_args_list:
                args, _ = call_args
                if args and "ERRO ao ler Silver Títulos" in str(args[0]):
                    found_error_msg = True
                    break
            self.assertTrue(found_error_msg, "Error message not printed")

if __name__ == '__main__':
    unittest.main()
