import unittest
import sys
import os
from unittest.mock import MagicMock, call

# Garante que o diretório tests esteja no path para importar notebook_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from notebook_utils import extract_function_from_file

# Define o caminho para o arquivo do notebook em relação à raiz do repositório
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOK_PATH = os.path.join(
    REPO_ROOT,
    "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Preparacao_Silver.Notebook/notebook-content.py"
)

class TestSilverJurosCorrections(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print(f"Extracting apply_juros_corrections from {NOTEBOOK_PATH}")
        cls.func_source = extract_function_from_file(NOTEBOOK_PATH, "apply_juros_corrections")
        if not cls.func_source:
             raise ValueError("Function apply_juros_corrections not found in notebook.")

    def test_apply_juros_corrections(self):
        # Mocks
        mock_col = MagicMock(name="col")
        mock_when = MagicMock(name="when")
        mock_df = MagicMock(name="df")
        mock_df.columns = ["JUROS"] # Simula existência de colunas

        # Simula encadeamento de when().when().otherwise()
        mock_when_ret = MagicMock(name="when_ret")
        mock_otherwise_ret = MagicMock(name="otherwise_ret")

        mock_when.return_value = mock_when_ret
        mock_when_ret.when.return_value = mock_when_ret
        mock_when_ret.otherwise.return_value = mock_otherwise_ret

        # Dados de Teste
        test_corrections = {
            -100.0: 10.0,
            -200.0: 20.0
        }

        # Globais de execução
        exec_globals = {
            'col': mock_col,
            'when': mock_when,
            'JUROS_CORRECTIONS': test_corrections
        }

        # Load function
        local_scope = {}
        exec(self.func_source, exec_globals, local_scope)
        apply_juros_corrections = local_scope['apply_juros_corrections']

        # Executa função
        result_df = apply_juros_corrections(mock_df)

        # Verification
        mock_df.withColumn.assert_called_once()
        args, _ = mock_df.withColumn.call_args
        self.assertEqual(args[0], "JUROS") # Espera letras maiúsculas conforme a lógica Silver
        self.assertEqual(args[1], mock_otherwise_ret)

        self.assertEqual(mock_when.call_count, 1)
        self.assertEqual(mock_when_ret.when.call_count, len(test_corrections) - 1)
        mock_when_ret.otherwise.assert_called_once()

    def test_apply_juros_corrections_lowercase_fallback(self):
        # Mocks
        mock_col = MagicMock(name="col")
        mock_when = MagicMock(name="when")
        mock_df = MagicMock(name="df")
        mock_df.columns = ["juros"] # Simula existência de colunas (lowercase)

        # Simula encadeamento de when().when().otherwise()
        mock_when_ret = MagicMock(name="when_ret")
        mock_otherwise_ret = MagicMock(name="otherwise_ret")

        mock_when.return_value = mock_when_ret
        mock_when_ret.when.return_value = mock_when_ret
        mock_when_ret.otherwise.return_value = mock_otherwise_ret

        test_corrections = {-100.0: 10.0}
        exec_globals = {
            'col': mock_col,
            'when': mock_when,
            'JUROS_CORRECTIONS': test_corrections
        }

        local_scope = {}
        exec(self.func_source, exec_globals, local_scope)
        apply_juros_corrections = local_scope['apply_juros_corrections']

        # Executa função
        result_df = apply_juros_corrections(mock_df)

        # Verificação - Deve detectar "juros" e usá-lo
        mock_df.withColumn.assert_called_once()
        args, _ = mock_df.withColumn.call_args
        self.assertEqual(args[0], "juros")
        self.assertEqual(args[1], mock_otherwise_ret)

    def test_empty_corrections(self):
        mock_col = MagicMock(name="col")
        mock_when = MagicMock(name="when")
        mock_df = MagicMock(name="df")

        exec_globals = {
            'col': mock_col,
            'when': mock_when,
            'JUROS_CORRECTIONS': {}
        }

        local_scope = {}
        exec(self.func_source, exec_globals, local_scope)
        apply_juros_corrections = local_scope['apply_juros_corrections']

        result_df = apply_juros_corrections(mock_df, corrections={})

        self.assertEqual(result_df, mock_df)
        mock_df.withColumn.assert_not_called()

if __name__ == '__main__':
    unittest.main()
