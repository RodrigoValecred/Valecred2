import unittest
from unittest.mock import Mock, MagicMock
import sys
import os

# Adjust path to find notebook_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from notebook_utils import extract_function_from_file

# Define the path to the notebook file relative to the repository root
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOK_PATH = os.path.join(
    REPO_ROOT,
    "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Curadoria_Gold.Notebook/notebook-content.py"
)

class TestFindColumn(unittest.TestCase):
    def setUp(self):
        # Extract function source
        self.func_source = extract_function_from_file(NOTEBOOK_PATH, "find_column")
        if not self.func_source:
             self.fail(f"Could not extract function 'find_column' from '{NOTEBOOK_PATH}'")

        # Mocks for Spark functions
        self.mock_col = MagicMock(side_effect=lambda x: f"col({x})")
        self.mock_lit = MagicMock(side_effect=lambda x: f"lit({x})")

        # Execute the function definition with mocks in globals
        exec_globals = {
            'col': self.mock_col,
            'lit': self.mock_lit
        }
        exec_locals = {}
        exec(self.func_source, exec_globals, exec_locals)
        self.find_column = exec_locals['find_column']

    def test_find_first_candidate_exact_match(self):
        mock_df = MagicMock()
        mock_df.columns = ["A", "B", "C"]
        candidates = ["A", "D"]

        result = self.find_column(mock_df, candidates)

        self.assertEqual(result, "col(A)")
        self.mock_col.assert_called_with("A")

    def test_find_first_candidate_case_insensitive(self):
        mock_df = MagicMock()
        mock_df.columns = ["a", "b", "c"]
        candidates = ["A", "D"]

        result = self.find_column(mock_df, candidates)

        # The function logic is: if candidate.lower() in [c.lower() for c in df.columns]: return col(candidate)
        # So it returns col("A"), not col("a") because it uses the candidate name
        self.assertEqual(result, "col(A)")
        self.mock_col.assert_called_with("A")

    def test_find_second_candidate(self):
        mock_df = MagicMock()
        mock_df.columns = ["X", "Y", "Z"]
        candidates = ["A", "Y"]

        result = self.find_column(mock_df, candidates)

        self.assertEqual(result, "col(Y)")
        self.mock_col.assert_called_with("Y")

    def test_no_candidate_found(self):
        mock_df = MagicMock()
        mock_df.columns = ["X", "Y", "Z"]
        candidates = ["A", "B"]

        result = self.find_column(mock_df, candidates)

        self.assertEqual(result, "lit(0)")
        self.mock_lit.assert_called_with(0)

    def test_empty_candidates(self):
        mock_df = MagicMock()
        mock_df.columns = ["A", "B"]
        candidates = []

        result = self.find_column(mock_df, candidates)

        self.assertEqual(result, "lit(0)")
        self.mock_lit.assert_called_with(0)

    def test_empty_dataframe(self):
        mock_df = MagicMock()
        mock_df.columns = []
        candidates = ["A"]

        result = self.find_column(mock_df, candidates)

        self.assertEqual(result, "lit(0)")
        self.mock_lit.assert_called_with(0)

if __name__ == '__main__':
    unittest.main()
