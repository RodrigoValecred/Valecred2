import unittest
import sys
import os

# Ensure the tests directory is in the path to import notebook_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from notebook_utils import extract_function_from_file

# Define the path to the notebook file relative to the repository root
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOK_PATH = os.path.join(
    REPO_ROOT,
    "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Analise_Cliente_Especifico.Notebook/notebook-content.py"
)

class TestClassificarInadimplencia(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print(f"Extracting classificar_inadimplencia from {NOTEBOOK_PATH}")
        func_source = extract_function_from_file(NOTEBOOK_PATH, "classificar_inadimplencia")

        if func_source:
            local_scope = {}
            exec(func_source, globals(), local_scope)
            cls.classificar_inadimplencia = staticmethod(local_scope["classificar_inadimplencia"])
        else:
            cls.classificar_inadimplencia = None
            print("WARNING: classificar_inadimplencia function not found in file.")

    def test_pg(self):
        """Test MOTIVO='PG' (Payment) returns 0 (Adimplente)."""
        if not self.classificar_inadimplencia:
            self.skipTest("Function not found")
        row = {'MOTIVO': 'PG', 'TTO_OPERACAO': 'ANY'}
        self.assertEqual(self.classificar_inadimplencia(row), 0)

    def test_rc_fc(self):
        """Test MOTIVO='RC' and TTO_OPERACAO='FC' returns 0 (Adimplente)."""
        if not self.classificar_inadimplencia:
            self.skipTest("Function not found")
        row = {'MOTIVO': 'RC', 'TTO_OPERACAO': 'FC'}
        self.assertEqual(self.classificar_inadimplencia(row), 0)

    def test_rc_cm(self):
        """Test MOTIVO='RC' and TTO_OPERACAO='CM' returns 0 (Adimplente)."""
        if not self.classificar_inadimplencia:
            self.skipTest("Function not found")
        row = {'MOTIVO': 'RC', 'TTO_OPERACAO': 'CM'}
        self.assertEqual(self.classificar_inadimplencia(row), 0)

    def test_rc_other(self):
        """Test MOTIVO='RC' and TTO_OPERACAO='XY' returns 1 (Inadimplente)."""
        if not self.classificar_inadimplencia:
            self.skipTest("Function not found")
        row = {'MOTIVO': 'RC', 'TTO_OPERACAO': 'XY'}
        self.assertEqual(self.classificar_inadimplencia(row), 1)

    def test_other_motivo(self):
        """Test MOTIVO='XX' returns 1 (Inadimplente)."""
        if not self.classificar_inadimplencia:
            self.skipTest("Function not found")
        row = {'MOTIVO': 'XX', 'TTO_OPERACAO': 'FC'}
        self.assertEqual(self.classificar_inadimplencia(row), 1)

    def test_missing_keys(self):
        """Ensure KeyError is raised if keys are missing."""
        if not self.classificar_inadimplencia:
            self.skipTest("Function not found")

        # In the function:
        # motivo = row['MOTIVO'] -> this will raise KeyError if 'MOTIVO' is missing
        # tto_operacao = row['TTO_OPERACAO'] -> this will raise KeyError if 'TTO_OPERACAO' is missing

        # Test missing TTO_OPERACAO (only if MOTIVO != 'PG', because if 'PG' returns immediately)
        # Wait, the function assigns variables first:
        # motivo = row['MOTIVO']
        # tto_operacao = row['TTO_OPERACAO']
        # So KeyError will happen regardless of values if keys are missing.

        row = {'MOTIVO': 'PG'} # Missing TTO_OPERACAO
        with self.assertRaises(KeyError):
            self.classificar_inadimplencia(row)

        row = {'TTO_OPERACAO': 'FC'} # Missing MOTIVO
        with self.assertRaises(KeyError):
            self.classificar_inadimplencia(row)

if __name__ == '__main__':
    unittest.main()
