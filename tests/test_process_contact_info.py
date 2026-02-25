import unittest
from unittest.mock import MagicMock, call
import sys
import os

# Ensure tests package is in path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from tests.notebook_utils import extract_function_from_file
except ImportError:
    from notebook_utils import extract_function_from_file

NOTEBOOK_PATH = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Process_Contact_Info.Notebook/notebook-content.py"

class TestUnfoldContactInfo(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print(f"Extracting unfold_contact_info from {NOTEBOOK_PATH}")
        cls.func_source = extract_function_from_file(NOTEBOOK_PATH, "unfold_contact_info")
        if not cls.func_source:
             print("WARNING: unfold_contact_info function not found in file.")

    def setUp(self):
        if not self.func_source:
            self.skipTest("Function not found")

        # Prepare scope with mocks for Spark functions
        # We return strings to easily verify the composition of functions
        self.mock_col = MagicMock(side_effect=lambda x: f"col({x})")
        self.mock_explode = MagicMock(side_effect=lambda x: f"explode({x})")
        self.mock_split = MagicMock(side_effect=lambda x, y: f"split({x}, {y})")
        self.mock_trim = MagicMock(side_effect=lambda x: f"trim({x})")

        local_scope = {}
        global_scope = {
            "col": self.mock_col,
            "explode": self.mock_explode,
            "split": self.mock_split,
            "trim": self.mock_trim
        }

        exec(self.func_source, global_scope, local_scope)
        self.unfold_contact_info = local_scope["unfold_contact_info"]

    def test_function_exists(self):
        """Test that the function was successfully extracted."""
        self.assertIsNotNone(self.func_source, "Function unfold_contact_info not found in notebook file.")

    def test_unfold_logic(self):
        """Test the core logic: explode(split) then trim."""
        df = MagicMock()
        # Mocking method chaining
        df_unfolded = MagicMock()
        df_cleaned = MagicMock()

        df.withColumn.return_value = df_unfolded
        df_unfolded.withColumn.return_value = df_cleaned

        result = self.unfold_contact_info(df, "INPUT_COL", "OUTPUT_COL", ";")

        # Verify first withColumn call (explode + split)
        # Expected: explode(split(col(INPUT_COL), ;))
        df.withColumn.assert_called_once()
        args, _ = df.withColumn.call_args
        self.assertEqual(args[0], "OUTPUT_COL")
        self.assertEqual(args[1], "explode(split(col(INPUT_COL), ;))")

        # Verify second withColumn call (trim)
        # Expected: trim(col(OUTPUT_COL))
        df_unfolded.withColumn.assert_called_once()
        args, _ = df_unfolded.withColumn.call_args
        self.assertEqual(args[0], "OUTPUT_COL")
        self.assertEqual(args[1], "trim(col(OUTPUT_COL))")

        self.assertEqual(result, df_cleaned)

    def test_delimiter_parameter(self):
        """Test that the delimiter parameter is correctly passed to split."""
        df = MagicMock()
        df.withColumn.return_value = MagicMock()

        # Use a safe delimiter like "," to avoid regex confusion in test
        self.unfold_contact_info(df, "INPUT_COL", "OUTPUT_COL", ",")

        # Verify split uses correct delimiter
        self.mock_split.assert_called_with("col(INPUT_COL)", ",")

if __name__ == '__main__':
    unittest.main()
