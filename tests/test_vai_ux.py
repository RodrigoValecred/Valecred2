
import os
import sys
import pytest
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
