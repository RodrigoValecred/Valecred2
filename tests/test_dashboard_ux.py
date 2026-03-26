
import unittest
from unittest.mock import MagicMock
import sys
import io
import contextlib
import os
import datetime

# --- Mocking Spark ---
# Isso é crucial porque o notebook importa pyspark.sql.functions
sys.modules['pyspark'] = MagicMock()
sys.modules['pyspark.sql'] = MagicMock()
sys.modules['pyspark.sql.functions'] = MagicMock()
sys.modules['pyspark.sql.types'] = MagicMock()
sys.modules['pyspark.sql.window'] = MagicMock()

# --- Mocking Display ---
# O notebook usa display(), que não está disponível no Python padrão
def mock_display(obj):
    pass

import builtins
builtins.display = mock_display

class TestDashboardUX(unittest.TestCase):
    def test_dashboard_output_format(self):
        # Captura stdout para analisar comandos de print
        f = io.StringIO()
        with contextlib.redirect_stdout(f):
            # Executa the notebook content
            notebook_path = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/NB_Gera_Relatorio_Diario_Clientes.Notebook/notebook-content.py"
            with open(notebook_path, "r") as file:
                code = file.read()
                # Executa em um novo escopo global para evitar poluir o ambiente de teste
                # mas passa 'display' via builtins
                exec(code, {'display': mock_display})

        output = f.getvalue()

        # 1. Verifica Header Date Format (DD/MM/YYYY)
        # "Data de Referência: 23/12/2025"
        self.assertRegex(output, r"Data de Referência: \d{2}/\d{2}/\d{4}")

        # 2. Verifica Item Date Format (DD/MM/YYYY) - UX Improvement
        # Agora esperamos encontrar o padrão DD/MM/YYYY na lista de itens, não YYYY-MM-DD
        # e.g., "30/12/2025 (7d)"
        self.assertRegex(output, r"\d{2}/\d{2}/\d{4} \(\d+d\)")

        # 3. Verifica Legend Presence - UX Improvement
        self.assertIn("Legenda: ✅ Seguro", output)
        self.assertIn("⚠️ Atenção", output)
        self.assertIn("🚨 Crítico", output)

if __name__ == '__main__':
    unittest.main()
