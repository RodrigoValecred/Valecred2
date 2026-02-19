
import unittest
from unittest.mock import patch
import pandas as pd
import numpy as np
from tests.notebook_utils import extract_function_from_file

NOTEBOOK_PATH = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/NB_Gera_Relatorio_Diario_Clientes.Notebook/notebook-content.py"

class TestRelatorioDiarioUX(unittest.TestCase):
    def setUp(self):
        source = extract_function_from_file(NOTEBOOK_PATH, "display_risk_dashboard")
        if not source:
            self.fail("Function display_risk_dashboard not found in notebook")

        # Define the environment where the function will run
        # pd and np are required. Builtins like print, enumerate, len are available via fallback.
        self.scope = {'pd': pd, 'np': np}

        # Execute the function definition
        try:
            exec(source, globals(), self.scope)
            self.display_risk_dashboard = self.scope['display_risk_dashboard']
        except Exception as e:
            self.fail(f"Failed to execute extracted function source: {e}")

    @patch('builtins.print')
    def test_display_risk_dashboard_output_structure(self, mock_print):
        # Setup mock data
        df = pd.DataFrame({
            'grupo': ['Test Group A', 'Test Group B'],
            'valor_risco': [100.0, 500.0],
            'limite_global': [200.0, 400.0],
            'utilizacao_pct': [50.0, 125.0],
            'excesso_valor': [0.0, 100.0]
        })

        # Run the function
        self.display_risk_dashboard(df)

        # Collect all print outputs
        calls = [args[0] for args, _ in mock_print.call_args_list if args]
        full_output = "\n".join(calls)

        # Assertions
        # 1. Check Header
        self.assertIn("PAINEL DE RISCO", full_output)
        self.assertIn("════", full_output)

        # 2. Check Group A (Normal)
        self.assertIn("Test Group A", full_output)
        self.assertIn("50.0%", full_output)
        self.assertIn("✅", full_output) # Green check for <= 80% (assuming logic)

        # 3. Check Group B (Over limit)
        self.assertIn("Test Group B", full_output)
        self.assertIn("125.0%", full_output)
        self.assertIn("🚨", full_output) # Siren for > 100%
        self.assertIn("🔥 EXCESSO", full_output)

        # 4. Check Formatting
        # We expect right alignment for money: R$ ...
        self.assertIn("R$          100.00", full_output) # approximate check for padding

    @patch('builtins.print')
    def test_display_risk_dashboard_long_name(self, mock_print):
        long_name = "A Very Long Group Name That Should Be Truncated Because It Exceeds The Limit Of The Layout"
        df = pd.DataFrame({
            'grupo': [long_name],
            'valor_risco': [0],
            'limite_global': [0],
            'utilizacao_pct': [0],
            'excesso_valor': [0]
        })

        self.display_risk_dashboard(df)

        calls = [args[0] for args, _ in mock_print.call_args_list if args]
        full_output = "\n".join(calls)

        # Check truncation logic (limit 50 -> 47 + ...)
        self.assertNotIn(long_name, full_output)
        truncated_part = long_name[:47]
        self.assertIn(truncated_part, full_output)
        self.assertIn("...", full_output)

if __name__ == '__main__':
    unittest.main()
