import unittest
import sys
import os

# Adjust path to find notebook_utils
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

try:
    from notebook_utils import extract_function_from_file
except ImportError:
    sys.path.append(os.path.join(current_dir, '..'))
    from tests.notebook_utils import extract_function_from_file

NOTEBOOK_PATH = "VALECRED_DEV/5_Notebooks/ValeCred_Artificial_Intelligence/VAI_Inferencia_Online.Notebook/notebook-content.py"

class TestVaiUX(unittest.TestCase):
    def load_function(self, func_name):
        source = extract_function_from_file(NOTEBOOK_PATH, func_name)
        if not source:
            raise ValueError(f"Could not extract function {func_name}")

        context = {}
        exec(source, context)
        return context[func_name]

    def test_create_progress_bar(self):
        try:
            create_progress_bar = self.load_function("create_progress_bar")
        except ValueError:
            self.fail("Function create_progress_bar not found in notebook yet.")

        # Test cases
        self.assertEqual(create_progress_bar(0, 10), "░░░░░░░░░░")
        self.assertEqual(create_progress_bar(50, 10), "█████░░░░░")
        self.assertEqual(create_progress_bar(100, 10), "██████████")
        self.assertEqual(create_progress_bar(25, 20), "█████░░░░░░░░░░░░░░░")

if __name__ == '__main__':
    unittest.main()
