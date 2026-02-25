import unittest
import sys
import os
import pandas as pd
import numpy as np
from unittest.mock import MagicMock

# Ensure the tests directory is in the path to import notebook_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from notebook_utils import extract_function_from_file

# Define the path to the notebook file relative to the repository root
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOK_PATH = os.path.join(
    REPO_ROOT,
    "VALECRED_DEV/6_Machine_Learning/ML_Previsao_Inadimplencia_2025.Notebook/notebook-content.py"
)

class TestMLPrevisaoInadimplencia(unittest.TestCase):

    def setUp(self):
        # Extract the source code of predict_proba_udf from the notebook
        self.func_source = extract_function_from_file(NOTEBOOK_PATH, "predict_proba_udf")
        if not self.func_source:
             self.fail(f"Function predict_proba_udf not found in notebook at {NOTEBOOK_PATH}.")

    def test_predict_proba_udf_logic(self):
        """
        Tests the core logic of the UDF:
        1. Correctly uses broadcast variables.
        2. Reconstructs DataFrame.
        3. Handles categorical conversions.
        4. Calls model prediction.
        5. Returns correct probability column.
        """

        # --- Mocks ---

        # Mock features_broadcast
        mock_features = ['feature_A', 'CODSTATUSCLIENTE', 'CODRATING_CEDENTE', 'feature_B']
        mock_features_broadcast = MagicMock()
        mock_features_broadcast.value = mock_features

        # Mock model_broadcast and the model itself
        mock_model = MagicMock()
        # Mock predict_proba to return array like [[prob_0, prob_1], ...]
        # We simulate 2 rows
        mock_model.predict_proba.return_value = np.array([
            [0.3, 0.7],  # Row 1: prob_1 = 0.7
            [0.8, 0.2]   # Row 2: prob_1 = 0.2
        ])
        mock_model_broadcast = MagicMock()
        mock_model_broadcast.value = mock_model

        # Mock pandas_udf decorator
        # It needs to handle being called as @pandas_udf(DoubleType())
        def mock_pandas_udf(*args, **kwargs):
            def decorator(f):
                return f
            return decorator

        # Mock DoubleType
        mock_double_type = MagicMock()

        # --- Execution Context ---

        exec_globals = {
            'pandas_udf': mock_pandas_udf,
            'DoubleType': lambda: mock_double_type, # Called as DoubleType()
            'pd': pd,
            'features_broadcast': mock_features_broadcast,
            'model_broadcast': mock_model_broadcast
        }

        # Execute the extracted code to get the function object
        local_scope = {}
        try:
            exec(self.func_source, exec_globals, local_scope)
        except Exception as e:
            self.fail(f"Failed to execute extracted function source: {e}")

        predict_proba_udf = local_scope['predict_proba_udf']

        # --- Prepare Input Data ---

        # Create input Series for the UDF
        # feature_A (numeric), CODSTATUSCLIENTE (categorical), CODRATING_CEDENTE (categorical), feature_B (numeric)
        col_feature_A = pd.Series([10.0, 20.0])
        col_cod_status = pd.Series(['Active', 'Inactive'])
        col_cod_rating = pd.Series(['A', 'B'])
        col_feature_B = pd.Series([5.0, 15.0])

        # --- Run the UDF ---

        # The UDF takes *cols
        result = predict_proba_udf(col_feature_A, col_cod_status, col_cod_rating, col_feature_B)

        # --- Assertions ---

        # 1. Verify Result
        expected_probs = pd.Series([0.7, 0.2])
        pd.testing.assert_series_equal(result, expected_probs)

        # 2. Verify Model Call
        # Check that model.predict_proba was called with the correct DataFrame
        mock_model.predict_proba.assert_called_once()
        call_args = mock_model.predict_proba.call_args[0][0] # First arg is X

        self.assertIsInstance(call_args, pd.DataFrame)
        self.assertListEqual(list(call_args.columns), mock_features)

        # 3. Verify Categorical Conversion
        # 'CODSTATUSCLIENTE' and 'CODRATING_CEDENTE' should be of category dtype
        self.assertTrue(isinstance(call_args['CODSTATUSCLIENTE'].dtype, pd.CategoricalDtype),
                        "CODSTATUSCLIENTE should be converted to categorical dtype")
        self.assertTrue(isinstance(call_args['CODRATING_CEDENTE'].dtype, pd.CategoricalDtype),
                        "CODRATING_CEDENTE should be converted to categorical dtype")

        # 'feature_A' and 'feature_B' should NOT be categorical
        self.assertFalse(isinstance(call_args['feature_A'].dtype, pd.CategoricalDtype))
        self.assertFalse(isinstance(call_args['feature_B'].dtype, pd.CategoricalDtype))

if __name__ == '__main__':
    unittest.main()
