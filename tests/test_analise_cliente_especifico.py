import unittest
import sys
import os
import pandas as pd
import numpy as np

# Ensure the tests directory is in the path to import notebook_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from notebook_utils import extract_function_from_file

# Define the path to the notebook file relative to the repository root
# Use relative path from the test file to avoid hardcoding issues if repo root changes
current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
NOTEBOOK_PATH = os.path.join(
    repo_root,
    "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Analise_Cliente_Especifico.Notebook/notebook-content.py"
)

class TestCreateTargetVariable(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print(f"Extracting create_target_variable from {NOTEBOOK_PATH}")
        # Read the file content to debug if extraction fails
        if not os.path.exists(NOTEBOOK_PATH):
            print(f"ERROR: Notebook file not found at {NOTEBOOK_PATH}")
            cls.create_target_variable = None
            return

        func_source = extract_function_from_file(NOTEBOOK_PATH, "create_target_variable")

        if func_source:
            local_scope = {}
            # We need numpy available in the scope where the function executes
            # Also ensure pandas is available if the function uses it
            global_scope = {'np': np, 'pd': pd}
            try:
                exec(func_source, global_scope, local_scope)
                cls.create_target_variable = staticmethod(local_scope["create_target_variable"])
            except Exception as e:
                print(f"Error executing extracted function source: {e}")
                cls.create_target_variable = None
        else:
            cls.create_target_variable = None
            print("WARNING: create_target_variable function not found in file.")

    def setUp(self):
        # Create a DataFrame with various cases
        # Note: the order matters for expected results
        self.df = pd.DataFrame([
            {'MOTIVO': 'PG', 'TTO_OPERACAO': 'ANY'},          # Expected: 0
            {'MOTIVO': 'RC', 'TTO_OPERACAO': 'FC'},           # Expected: 0
            {'MOTIVO': 'RC', 'TTO_OPERACAO': 'CM'},           # Expected: 0
            {'MOTIVO': 'RC', 'TTO_OPERACAO': 'XY'},           # Expected: 1
            {'MOTIVO': 'XX', 'TTO_OPERACAO': 'FC'},           # Expected: 1
            {'MOTIVO': 'PG', 'TTO_OPERACAO': None},           # Expected: 0
            {'MOTIVO': None, 'TTO_OPERACAO': 'FC'},           # Expected: 1
        ])
        self.expected_targets = [0, 0, 0, 1, 1, 0, 1]

    def test_logic(self):
        if not self.create_target_variable:
            self.fail("Function create_target_variable not found in notebook.")

        # Execute the extracted function
        result = self.create_target_variable(self.df)

        # Result should be a numpy array or Series
        # Convert to list for comparison
        if hasattr(result, 'tolist'):
            result_list = result.tolist()
        else:
            result_list = list(result)

        self.assertEqual(result_list, self.expected_targets)

if __name__ == '__main__':
    unittest.main()
