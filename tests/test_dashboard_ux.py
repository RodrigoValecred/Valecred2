
import unittest
from unittest.mock import MagicMock
import sys
import io
import contextlib
import os
import datetime

# --- Mocking Spark ---
# This is crucial because the notebook imports pyspark.sql.functions
sys.modules['pyspark'] = MagicMock()
sys.modules['pyspark.sql'] = MagicMock()
sys.modules['pyspark.sql.functions'] = MagicMock()
sys.modules['pyspark.sql.types'] = MagicMock()
sys.modules['pyspark.sql.window'] = MagicMock()

# --- Mocking Display ---
# The notebook uses display(), which is not available in standard Python
def mock_display(obj):
    pass

import builtins
builtins.display = mock_display

class TestDashboardUX(unittest.TestCase):
    def test_dashboard_output_format(self):
        # Capture stdout to analyze print statements
        f = io.StringIO()
        with contextlib.redirect_stdout(f):
            # Execute the notebook content
            notebook_path = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/NB_Gera_Relatorio_Diario_Clientes.Notebook/notebook-content.py"
            with open(notebook_path, "r") as file:
                code = file.read()
                # Execute in a new global scope to avoid polluting the test environment
                # but pass 'display' via builtins
                exec(code, {'display': mock_display})

        output = f.getvalue()

        # 1. Verify Header Date Format (DD/MM/YYYY)
        # "Data de Referência: 23/12/2025"
        self.assertRegex(output, r"Data de Referência: \d{2}/\d{2}/\d{4}")

        # 2. Verify Item Date Format (DD/MM/YYYY) - UX Improvement
        # We now expect to find DD/MM/YYYY pattern in the item list, not YYYY-MM-DD
        # e.g., "30/12/2025 (7d)"
        self.assertRegex(output, r"\d{2}/\d{2}/\d{4} \(\d+d\)")

        # 3. Verify Legend Presence - UX Improvement
        self.assertIn("Legenda: ✅ Seguro", output)
        self.assertIn("⚠️ Atenção", output)
        self.assertIn("🚨 Crítico", output)

if __name__ == '__main__':
    unittest.main()
