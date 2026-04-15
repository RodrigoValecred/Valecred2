
import os
import sys
import pytest
from unittest.mock import MagicMock, patch
from tests.notebook_utils import extract_function_from_file

# Caminho para o notebook
NOTEBOOK_PATH = "VALECRED_DEV/5_Notebooks/ValeCred_Artificial_Intelligence/VAI_Inferencia_Online.Notebook/notebook-content.py"

class TestVaiUX:
    def setup_method(self):
        # Extrai o código fonte da função
        self.create_progress_bar_source = extract_function_from_file(NOTEBOOK_PATH, "create_progress_bar")

        if not self.create_progress_bar_source:
            pytest.fail(f"Could not extract create_progress_bar from {NOTEBOOK_PATH}")

        # # Executa a definição da função em um namespace local
        self.local_scope = {}
        exec(self.create_progress_bar_source, {}, self.local_scope)
        self.create_progress_bar = self.local_scope['create_progress_bar']

    def test_progress_bar_0_percent(self):
        result = self.create_progress_bar(0, width=10)
        # 0% de 10 é 0 preenchido.
        expected = "[░░░░░░░░░░] 0.0%"
        assert result == expected

    def test_progress_bar_50_percent(self):
        result = self.create_progress_bar(50, width=10)
        # 50% de 10 é 5 preenchido.
        expected = "[█████░░░░░] 50.0%"
        assert result == expected

    def test_progress_bar_100_percent(self):
        result = self.create_progress_bar(100, width=10)
        # 100% de 10 é 10 preenchido.
        expected = "[██████████] 100.0%"
        assert result == expected

    def test_progress_bar_custom_width(self):
        result = self.create_progress_bar(25, width=20)
        # 25% de 20 é 5 preenchido.
        expected_bar = "█" * 5 + "░" * 15
        expected = f"[{expected_bar}] 25.0%"
        assert result == expected

    def test_progress_bar_rounding(self):
        # 12.5% de 10 é 1.25 -> int(1.25) = 1
        result = self.create_progress_bar(12.5, width=10)
        expected = "[█░░░░░░░░░] 12.5%"
        assert result == expected

    def test_progress_bar_negative_clamping(self):
        # Teste de regressão: evita largura incorreta para valores negativos
        result = self.create_progress_bar(-10, width=10)
        # Esperado: preenchido restrito (clamp) a 0, largura total 10
        expected = "[░░░░░░░░░░] 0.0%"
        assert result == expected

    def test_progress_bar_overflow_clamping(self):
        # Teste de regressão: evita largura incorreta para valores acima de 100%
        result = self.create_progress_bar(110, width=10)
        # Esperado: preenchido restrito (clamp) a 10, largura total 10
        expected = "[██████████] 100.0%"
        assert result == expected

    def test_progress_bar_extreme_overflow(self):
        # Teste de regressão para overflow extremo
        result = self.create_progress_bar(9999, width=10)
        expected = "[██████████] 100.0%"
        assert result == expected

    @patch('builtins.print')
    def test_display_terminal_dashboard(self, mock_print):
        # Extrai e executa display_terminal_dashboard
        source = extract_function_from_file(NOTEBOOK_PATH, "display_terminal_dashboard")

        # Também precisamos de create_progress_bar no escopo porque display_terminal_dashboard chama isso
        source_pb = extract_function_from_file(NOTEBOOK_PATH, "create_progress_bar")
        exec(source_pb, self.local_scope, self.local_scope)

        exec(source, self.local_scope, self.local_scope)
        display_terminal_dashboard = self.local_scope['display_terminal_dashboard']

        # Configura Dicionário de Métricas
        metrics = {
            "total_ops": 10,
            "risco_alto": 3,
            "top_motivos": [('Motivo A', 2), ('Motivo B', 1)]
        }

        # Executa função
        display_terminal_dashboard(metrics)

        # Verifica output
        output = "\n".join([call.args[0] for call in mock_print.call_args_list if call.args])
        assert "RESUMO DO PROCESSAMENTO V.A.I." in output
        assert "Total:" in output
        assert "10" in output
        assert "Alto Risco:" in output
        assert "3" in output
        assert "Discrepantes:" in output
        assert "TOP 3 MOTIVOS DE RISCO" in output
        assert "Motivo A" in output
