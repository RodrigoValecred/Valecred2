
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from tests.notebook_utils import extract_function_from_file

NOTEBOOK_PATH = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/NB_Gera_Relatorio_Diario_Clientes.Notebook/notebook-content.py"

class TestRelatorioDiarioUX(unittest.TestCase):
    def setUp(self):
        # 1. Prepare Scope with Dependencies
        self.scope = {'pd': pd, 'np': np, 'datetime': datetime}

        # Inject data_hoje
        self.data_hoje = datetime(2025, 12, 23).date()
        self.scope['data_hoje'] = self.data_hoje

        # Mock Colors class since we can't easily extract classes with current util
        class MockColors:
            HEADER = ''
            BLUE = ''
            CYAN = ''
            GREEN = ''
            YELLOW = ''
            RED = ''
            RESET = ''
            BOLD = ''
        self.scope['Colors'] = MockColors

        # Extract helper function format_currency_br
        format_source = extract_function_from_file(NOTEBOOK_PATH, "format_currency_br")
        if format_source:
            exec(format_source, self.scope, self.scope)
        else:
            # Fallback mock if not found (though it should be there)
            self.scope['format_currency_br'] = lambda x: f"R$ {x:.2f}"

        # Extract display_risk_dashboard
        source = extract_function_from_file(NOTEBOOK_PATH, "display_risk_dashboard")
        if not source:
            self.fail("Function display_risk_dashboard not found in notebook")

        try:
            # Use self.scope as both globals and locals to ensure closures (like Colors) work
            exec(source, self.scope, self.scope)
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
        self.assertIn("PAINEL DE RISCO", full_output)
        self.assertIn("Test Group A", full_output)
        self.assertIn("50.0%", full_output)
        # Note: Colors are empty strings in mock, so we won't see ANSI codes, but text structure remains
        # We can check for icons if they are hardcoded strings, which they are in the notebook
        self.assertIn("✅", full_output)
        self.assertIn("🚨", full_output)

        # UX Improvement Check: "Disponível" should be shown for safe groups
        self.assertIn("Disponível:", full_output)
        # "EXCESSO" should be shown for unsafe groups (already implicit in logic, but good to check)
        self.assertIn("EXCESSO:", full_output)

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

        self.assertNotIn(long_name, full_output)
        truncated_part = long_name[:47]
        self.assertIn(truncated_part, full_output)
        self.assertIn("...", full_output)

    @patch('builtins.print')
    def test_display_risk_dashboard_validity(self, mock_print):
        # Test Case: Expired, Near Expiry, Safe
        df = pd.DataFrame({
            'grupo': ['Expired', 'Near', 'Safe'],
            'valor_risco': [10.0, 10.0, 10.0],
            'limite_global': [100.0, 100.0, 100.0],
            'utilizacao_pct': [10.0, 10.0, 10.0],
            'excesso_valor': [0, 0, 0],
            'validade_limite': [
                '2025-12-01', # Expired (Assuming self.data_hoje is 2025-12-23)
                '2025-12-30', # Near (7 days)
                '2026-06-01'  # Safe
            ]
        })

        self.display_risk_dashboard(df)

        calls = [args[0] for args, _ in mock_print.call_args_list if args]
        full_output = "\n".join(calls)

        self.assertIn("VENCIDO", full_output)
        self.assertIn("(7d)", full_output)
        self.assertIn("2026-06-01", full_output)

    def test_style_risk_dataframe(self):
        # This function is what we are adding.
        # We extract it to verify logic.
        style_source = extract_function_from_file(NOTEBOOK_PATH, "style_risk_dataframe")

        if not style_source:
             self.fail("Function style_risk_dataframe not found in notebook. Implement it!")

        # Execute
        exec(style_source, self.scope, self.scope)
        style_func = self.scope['style_risk_dataframe']

        # Test Data
        df = pd.DataFrame({
            'grupo': ['A', 'B'],
            'valor_risco': [1000.0, 2000.0],
            'limite_global': [5000.0, 1000.0],
            'utilizacao_pct': [20.0, 200.0],
            'excesso_valor': [0.0, 1000.0]
        })

        styler = style_func(df)
        html = styler.to_html()

        # Verify CSS Logic
        # 20% -> Green (#ccffcc)
        self.assertIn("#ccffcc", html)
        # 200% -> Red (#ffcccc)
        self.assertIn("#ffcccc", html)

        # Verify Currency Logic
        # Just check if R$ appears roughly correct.
        # HTML output creates <td>R$ 1.000,00</td> etc.
        self.assertIn("R$", html)

if __name__ == '__main__':
    unittest.main()
