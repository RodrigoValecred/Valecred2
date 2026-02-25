
import os
import sys
import pytest
from unittest.mock import MagicMock, patch
from tests.notebook_utils import extract_function_from_file

# Path to the notebook
NOTEBOOK_PATH = "VALECRED_DEV/5_Notebooks/ValeCred_Artificial_Intelligence/VAI_Inferencia_Online.Notebook/notebook-content.py"

class TestVaiUX:
    def setup_method(self):
        # Extract the function source code
        self.create_progress_bar_source = extract_function_from_file(NOTEBOOK_PATH, "create_progress_bar")

        if not self.create_progress_bar_source:
            pytest.fail(f"Could not extract create_progress_bar from {NOTEBOOK_PATH}")

        # Execute the function definition in a local namespace
        self.local_scope = {}
        exec(self.create_progress_bar_source, {}, self.local_scope)
        self.create_progress_bar = self.local_scope['create_progress_bar']

    def test_progress_bar_0_percent(self):
        result = self.create_progress_bar(0, width=10)
        # 0% of 10 is 0 filled.
        expected = "[░░░░░░░░░░] 0.0%"
        assert result == expected

    def test_progress_bar_50_percent(self):
        result = self.create_progress_bar(50, width=10)
        # 50% of 10 is 5 filled.
        expected = "[█████░░░░░] 50.0%"
        assert result == expected

    def test_progress_bar_100_percent(self):
        result = self.create_progress_bar(100, width=10)
        # 100% of 10 is 10 filled.
        expected = "[██████████] 100.0%"
        assert result == expected

    def test_progress_bar_custom_width(self):
        result = self.create_progress_bar(25, width=20)
        # 25% of 20 is 5 filled.
        expected_bar = "█" * 5 + "░" * 15
        expected = f"[{expected_bar}] 25.0%"
        assert result == expected

    def test_progress_bar_rounding(self):
        # 12.5% of 10 is 1.25 -> int(1.25) = 1
        result = self.create_progress_bar(12.5, width=10)
        expected = "[█░░░░░░░░░] 12.5%"
        assert result == expected

    def test_progress_bar_negative_clamping(self):
        # Current bug: results in width 11 if not clamped
        result = self.create_progress_bar(-10, width=10)
        # Expected: clamp to 0 filled, total width 10
        expected = "[░░░░░░░░░░] -10.0%"
        assert result == expected

    def test_progress_bar_overflow_clamping(self):
        # Current bug: results in width 11 if not clamped
        result = self.create_progress_bar(110, width=10)
        # Expected: clamp to 10 filled, total width 10
        expected = "[██████████] 110.0%"
        assert result == expected

    @patch('builtins.print')
    def test_display_terminal_dashboard(self, mock_print):
        # Extract and execute display_terminal_dashboard
        source = extract_function_from_file(NOTEBOOK_PATH, "display_terminal_dashboard")

        # We also need create_progress_bar in the scope because display_terminal_dashboard calls it
        source_pb = extract_function_from_file(NOTEBOOK_PATH, "create_progress_bar")
        exec(source_pb, self.local_scope, self.local_scope)

        exec(source, self.local_scope, self.local_scope)
        display_terminal_dashboard = self.local_scope['display_terminal_dashboard']

        # Setup Metrics Dictionary
        metrics = {
            "total_ops": 10,
            "risco_alto": 3,
            "top_motivos": [('Motivo A', 2), ('Motivo B', 1)]
        }

        # Run function
        display_terminal_dashboard(metrics)

        # Verify output
        output = "\n".join([call.args[0] for call in mock_print.call_args_list if call.args])
        assert "RESUMO DO PROCESSAMENTO V.A.I." in output
        assert "Total:" in output
        assert "10" in output
        assert "Alto Risco:" in output
        assert "3" in output
        assert "TOP 3 MOTIVOS DE RISCO" in output
        assert "Motivo A" in output
