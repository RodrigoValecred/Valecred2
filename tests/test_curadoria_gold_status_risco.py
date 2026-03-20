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
    "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Curadoria_Gold.Notebook/notebook-content.py"
)

class TestStatusRiscoExpr(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print(f"Extracting get_status_risco_expr from {NOTEBOOK_PATH}")
        cls.func_source = extract_function_from_file(NOTEBOOK_PATH, "get_status_risco_expr")
        if not cls.func_source:
             raise ValueError("Function get_status_risco_expr not found in notebook.")

    def test_status_risco_logic(self):
        # Mocks
        col_mocks = {}
        def col_side_effect(name):
            if name not in col_mocks:
                m = MagicMock(name=f"col({name})")
                m.__lt__ = MagicMock(name=f"lt_mock") # Simula operador <
                m.__eq__ = MagicMock(name=f"eq_mock") # Simula operador ==
                m.__and__ = MagicMock(name=f"and_mock") # Simula operador &
                col_mocks[name] = m
            return col_mocks[name]

        mock_col = MagicMock(side_effect=col_side_effect)
        mock_when = MagicMock(name="when")
        mock_current_date = MagicMock(name="current_date")

        # Para encadear when().when().otherwise()
        mock_when_ret = MagicMock(name="when_ret")
        mock_when.return_value = mock_when_ret
        mock_when_ret.when.return_value = mock_when_ret # Chain
        mock_when_ret.otherwise.return_value = "RESULT_COLUMN"

        # Execution globals
        exec_globals = {
            'col': mock_col,
            'when': mock_when,
            'current_date': mock_current_date,
        }

        # Load function
        local_scope = {}
        exec(self.func_source, exec_globals, local_scope)
        get_status_risco_expr = local_scope['get_status_risco_expr']

        # Caso de Teste 1: Argumentos padrão
        result = get_status_risco_expr()

        # Verifica result
        self.assertEqual(result, "RESULT_COLUMN")

        # Verifica calls
        self.assertTrue(mock_when.called)
        self.assertTrue(mock_current_date.called) # Deve ser chamado como argumento padrão

        # Caso de Teste 2: Argumentos customizados e data explícita
        mock_date_col = MagicMock(name="custom_date")
        get_status_risco_expr("my_tto", "my_venc", mock_date_col)

        # Verifica col calls
        self.assertIn("my_tto", col_mocks)
        self.assertIn("my_venc", col_mocks)

        # Verifica comparison: my_venc < mock_date_col
        # Nota: Na função: col(col_vencimento) < current_date_col
        # Então esperamos que col_mocks["my_venc"].__lt__ seja chamado com mock_date_col
        col_mocks["my_venc"].__lt__.assert_called_with(mock_date_col)

        # Verifica comparison: my_tto == "RN"
        col_mocks["my_tto"].__eq__.assert_called_with("RN")

if __name__ == '__main__':
    unittest.main()
