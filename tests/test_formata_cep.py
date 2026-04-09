import unittest
import sys
import os
import pandas as pd
from unittest.mock import MagicMock

# Define o caminho para o arquivo do notebook
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from notebook_utils import extract_function_from_file

NOTEBOOK_PATH = os.path.join(
    REPO_ROOT,
    "VALECRED_DEV/5_Notebooks/Dados_Externos/CEP/NB_Load_Bronze_CEPs_Coords.Notebook/notebook-content.py"
)

class TestFormataCep(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Extrai a função formata_cep do notebook
        cls.func_source = extract_function_from_file(NOTEBOOK_PATH, "formata_cep")
        if not cls.func_source:
             raise ValueError("Function formata_cep not found in notebook.")

        exec_globals = {
            'pd': pd,
        }
        local_scope = {}
        exec(cls.func_source, exec_globals, local_scope)
        cls.formata_cep = staticmethod(local_scope['formata_cep'])

    def test_formata_cep_valid_numbers(self):
        # Integer
        self.assertEqual(self.formata_cep(123456), "00123456")
        self.assertEqual(self.formata_cep(12345678), "12345678")
        # Float
        self.assertEqual(self.formata_cep(123456.0), "00123456")
        # String representing number
        self.assertEqual(self.formata_cep("123456"), "00123456")

    def test_formata_cep_invalid_or_nan(self):
        # NaN
        self.assertIsNone(self.formata_cep(pd.NA))
        self.assertIsNone(self.formata_cep(float('nan')))
        # Invalid conversion
        self.assertEqual(self.formata_cep("invalid"), "invalid")

if __name__ == "__main__":
    unittest.main()
