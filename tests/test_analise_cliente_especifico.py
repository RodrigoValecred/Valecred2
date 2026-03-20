import unittest
import sys
import os
from unittest.mock import MagicMock, call

# Garante que o diretório tests esteja no path para importar notebook_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from notebook_utils import extract_function_from_file

current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
NOTEBOOK_PATH = os.path.join(
    repo_root,
    "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Analise_Cliente_Especifico.Notebook/notebook-content.py"
)

class TestCreateTargetVariable(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print(f"Extracting create_target_variable from {NOTEBOOK_PATH}")
        if not os.path.exists(NOTEBOOK_PATH):
            print(f"ERROR: Notebook file not found at {NOTEBOOK_PATH}")
            cls.create_target_variable = None
            return

        func_source = extract_function_from_file(NOTEBOOK_PATH, "create_target_variable")

        if func_source:
            local_scope = {}
            # Simula objetos pyspark
            mock_when = MagicMock(name="when")
            mock_col = MagicMock(name="col")

            # Configuração de when() para retornar algo com .otherwise()
            mock_when_result = MagicMock(name="when_result")
            mock_when.return_value = mock_when_result

            global_scope = {
                'when': mock_when,
                'col': mock_col
            }
            try:
                exec(func_source, global_scope, local_scope)
                cls.create_target_variable = staticmethod(local_scope["create_target_variable"])
                cls.mock_when = mock_when
                cls.mock_col = mock_col
                cls.mock_when_result = mock_when_result
            except Exception as e:
                print(f"Error executing extracted function source: {e}")
                cls.create_target_variable = None
        else:
            cls.create_target_variable = None

    def test_logic(self):
        if not self.create_target_variable:
            self.fail("Function create_target_variable not found in notebook.")

        # Executa the extracted function
        result = self.create_target_variable()

        # Verifica se ele retorna o resultado de .otherwise(1)
        self.assertEqual(result, self.mock_when_result.otherwise.return_value)
        self.mock_when_result.otherwise.assert_called_once_with(1)

        # Verifica se when foi chamado corretamente
        # Isso é difícil de afirmar exatamente devido aos operadores sobrecarregados nas simulações de Coluna do PySpark
        self.mock_when.assert_called_once()
        args, kwargs = self.mock_when.call_args
        self.assertEqual(args[1], 0)

if __name__ == '__main__':
    unittest.main()
