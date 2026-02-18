import unittest
import os
import sys

# Add the tests directory to sys.path to import notebook_utils
sys.path.append(os.path.dirname(__file__))

from notebook_utils import extract_function_from_file

class TestVaiUx(unittest.TestCase):
    def setUp(self):
        self.notebook_path = os.path.join(
            "VALECRED_DEV", "5_Notebooks", "ValeCred_Artificial_Intelligence",
            "VAI_Inferencia_Online.Notebook", "notebook-content.py"
        )

    def test_create_progress_bar(self):
        # Extract the function source code
        source_code = extract_function_from_file(self.notebook_path, "create_progress_bar")

        if source_code is None:
            self.fail(f"Function 'create_progress_bar' not found in {self.notebook_path}")

        # Execute the function definition in a local scope
        local_scope = {}
        exec(source_code, {}, local_scope)
        create_progress_bar = local_scope['create_progress_bar']

        # Test Case 1: 50%
        result = create_progress_bar(5, 10, length=10)
        expected = "█████░░░░░ 50.0%"
        self.assertEqual(result, expected)

        # Test Case 2: 0%
        result = create_progress_bar(0, 10, length=10)
        expected = "░░░░░░░░░░ 0.0%"
        self.assertEqual(result, expected)

        # Test Case 3: 100%
        result = create_progress_bar(10, 10, length=10)
        expected = "██████████ 100.0%"
        self.assertEqual(result, expected)

        # Test Case 4: Empty total (avoid division by zero)
        result = create_progress_bar(0, 0, length=10)
        expected = "░░░░░░░░░░ 0.0%"
        self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main()
