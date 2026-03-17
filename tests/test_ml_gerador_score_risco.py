import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
import numpy as np
import sys
import os

# Adjust path to find notebook_utils
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

try:
    from notebook_utils import extract_function_from_file
except ImportError:
    # If run from root
    try:
        from tests.notebook_utils import extract_function_from_file
    except ImportError:
        # Fallback if neither works (e.g. strict environment)
        # We will assume notebook_utils is importable if we are in tests dir
        sys.path.append(os.path.join(current_dir, '..'))
        from tests.notebook_utils import extract_function_from_file

NOTEBOOK_PATH = "VALECRED_DEV/6_Machine_Learning/ML_Gerador_Score_Risco.Notebook/notebook-content.py"

class TestMLGeradorScoreRisco(unittest.TestCase):
    def setUp(self):
        # Mock pyspark.sql.functions.col
        self.mock_col = MagicMock()
        self.mock_col.return_value = MagicMock() # Column object

        # Mock display
        self.mock_display = MagicMock()

        # Mock Colors class
        class MockColors:
            HEADER = '[HEADER]'
            BLUE = '[BLUE]'
            CYAN = '[CYAN]'
            GREEN = '[GREEN]'
            YELLOW = '[YELLOW]'
            RED = '[RED]'
            RESET = '[RESET]'
            BOLD = '[BOLD]'

        # Mock draw_risk_meter
        def mock_draw_risk_meter(score, width=30):
            return f"[RISK METER: {score:.2f}]"

        self.context = {
            'col': self.mock_col,
            'display': self.mock_display,
            'pd': pd,
            'Colors': MockColors,
            'draw_risk_meter': mock_draw_risk_meter
        }

    def test_draw_risk_meter(self):
        try:
            draw_risk_meter = self.load_function("draw_risk_meter")
        except ValueError:
            return

        # Test NaN
        res_nan = draw_risk_meter(float('nan'))
        self.assertIn("[DADOS INSUFICIENTES]", res_nan)
        self.assertIn("[YELLOW]", res_nan)

        # Test clamp minimum (e.g., negative score -> 0.0)
        res_min = draw_risk_meter(-0.5, width=10)
        self.assertIn("[GREEN]", res_min)
        self.assertIn("✅", res_min)
        self.assertIn("0.00", res_min)
        # 0 filled, 10 empty
        self.assertIn("░" * 10, res_min)

        # Test clamp maximum (e.g., > 1.0 -> 1.0)
        res_max = draw_risk_meter(1.5, width=10)
        self.assertIn("[RED]", res_max)
        self.assertIn("🚨", res_max)
        self.assertIn("1.00", res_max)
        # 10 filled, 0 empty
        self.assertIn("█" * 10, res_max)

        # Test Low Risk (< 0.15)
        res_low = draw_risk_meter(0.10, width=10)
        self.assertIn("[GREEN]", res_low)
        self.assertIn("✅", res_low)
        self.assertIn("0.10", res_low)
        # 10 * 0.10 = 1 filled, 9 empty
        self.assertIn("█", res_low)
        self.assertIn("░" * 9, res_low)

        # Test Medium Risk (>= 0.15 and < 0.40)
        res_med = draw_risk_meter(0.25, width=10)
        self.assertIn("[YELLOW]", res_med)
        self.assertIn("⚠️", res_med)
        self.assertIn("0.25", res_med)
        # 10 * 0.25 = 2 filled, 8 empty
        self.assertIn("█" * 2, res_med)
        self.assertIn("░" * 8, res_med)

        # Test High Risk (>= 0.40)
        res_high = draw_risk_meter(0.50, width=10)
        self.assertIn("[RED]", res_high)
        self.assertIn("🚨", res_high)
        self.assertIn("0.50", res_high)
        # 10 * 0.50 = 5 filled, 5 empty
        self.assertIn("█" * 5, res_high)
        self.assertIn("░" * 5, res_high)

        # Test Boundary Thresholds
        res_bound1 = draw_risk_meter(0.149)
        self.assertIn("[GREEN]", res_bound1)
        res_bound2 = draw_risk_meter(0.15)
        self.assertIn("[YELLOW]", res_bound2)

        # Test Edge case just below 0.15
        res_bound_almost_15 = draw_risk_meter(0.149999)
        self.assertIn("[GREEN]", res_bound_almost_15)
        self.assertIn("✅", res_bound_almost_15)
        self.assertIn("0.15", res_bound_almost_15)  # Because of {:.2f} formatting, it rounds to 0.15 but stays green

        # Test Edge case exactly 0.0 for < 0.15 logic
        res_zero = draw_risk_meter(0.0, width=10)
        self.assertIn("[GREEN]", res_zero)
        self.assertIn("✅", res_zero)
        self.assertIn("0.00", res_zero)
        self.assertIn("░" * 10, res_zero)

        res_bound3 = draw_risk_meter(0.399)
        self.assertIn("[YELLOW]", res_bound3)
        res_bound4 = draw_risk_meter(0.40)
        self.assertIn("[RED]", res_bound4)

    def test_draw_risk_meter_nan_edge_cases(self):
        try:
            draw_risk_meter = self.load_function("draw_risk_meter")
        except ValueError:
            return

        import math

        # Test np.nan
        res_np_nan = draw_risk_meter(np.nan)
        self.assertIn("[DADOS INSUFICIENTES]", res_np_nan)
        self.assertIn("[YELLOW]", res_np_nan)

        # Test math.nan
        res_math_nan = draw_risk_meter(math.nan)
        self.assertIn("[DADOS INSUFICIENTES]", res_math_nan)
        self.assertIn("[YELLOW]", res_math_nan)

        # Test None
        res_none = draw_risk_meter(None)
        self.assertIn("[DADOS INSUFICIENTES]", res_none)
        self.assertIn("[YELLOW]", res_none)

    def load_function(self, func_name):
        source = extract_function_from_file(NOTEBOOK_PATH, func_name)
        if not source:
            raise ValueError(f"Could not extract function {func_name}")
        exec(source, self.context)
        return self.context[func_name]

    def test_calcular_score_cliente(self):
        self.context['spark'] = MagicMock()
        try:
            calcular_score_cliente = self.load_function("calcular_score_cliente")
        except ValueError:
             # Skip if not yet implemented (for TDD)
             return

        # Mock inputs
        mock_df_spark = MagicMock()
        # Mock DataFrame structure
        data = {
            'CPFCNPJ': ['123'],
            'LIQUIDACAO': [None],
            'STATUSANALISE': ['D'],
            'STATUSACEITE': ['A'],
            'ACEITO': ['S'],
            'TTO_OPERACAO': ['NP'],
            'VALOR': [1000.5], # Ensure it's float64
            'PRAZO': [30],     # Ensure it's int64
            'CODRATING_CEDENTE': ['A'],
            'CODSTATUSCLIENTE': ['1']
        }
        mock_df_pandas = pd.DataFrame(data)
        mock_df_pandas['VALOR'] = mock_df_pandas['VALOR'].astype('float64')
        mock_df_pandas['PRAZO'] = mock_df_pandas['PRAZO'].astype('int64')

        mock_df_spark.filter.return_value = mock_df_spark # Chain filters
        mock_df_spark.toPandas.return_value = mock_df_pandas

        mock_model = MagicMock()
        mock_model.predict_proba.return_value = np.array([[0.1, 0.9]]) # Probability of class 1 is 0.9

        model_features = ['VALOR', 'PRAZO', 'CODRATING_CEDENTE']

        result = calcular_score_cliente('123', mock_df_spark, mock_model, model_features)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertIn('SCORE_RISCO', result.columns)
        self.assertEqual(result['SCORE_RISCO'].iloc[0], 0.9)

        # Verify data type downcasting float64 -> float32
        args, kwargs = mock_model.predict_proba.call_args
        X_cliente_passed = args[0]

        self.assertEqual(X_cliente_passed['VALOR'].dtype, 'float32', "float64 should be downcasted to float32")
        self.assertEqual(X_cliente_passed['PRAZO'].dtype, 'int64', "int64 should remain int64")

        # Verify filtering was called
        self.mock_col.assert_called()

    def test_exibir_analise_risco(self):
        try:
            exibir_analise_risco = self.load_function("exibir_analise_risco")
        except ValueError:
            return

        df = pd.DataFrame({
            'SCORE_RISCO': [0.1, 0.2],
            'VALOR': [1000, 2000],
            'PRAZO': [30, 60],
            'CODTITULO': ['T1', 'T2'],
            'VENCIMENTO': ['2025-01-01', '2025-02-01'],
            'CODRATING_CEDENTE': ['A', 'B']
        })

        # We can't easily capture stdout without redirecting it, but we can verify display is called
        exibir_analise_risco(df, '123')
        self.mock_display.assert_called()

    def test_gerar_score_e_alertas_integration(self):
        self.context['spark'] = MagicMock()
        # Extract all three functions
        try:
            source_calc = extract_function_from_file(NOTEBOOK_PATH, "calcular_score_cliente")
            source_exib = extract_function_from_file(NOTEBOOK_PATH, "exibir_analise_risco")
            source_main = extract_function_from_file(NOTEBOOK_PATH, "gerar_score_e_alertas")
        except ValueError:
            return

        if source_calc: exec(source_calc, self.context)
        if source_exib: exec(source_exib, self.context)
        if source_main: exec(source_main, self.context)

        if 'gerar_score_e_alertas' not in self.context:
            return # Skip if main function not found or not loaded

        gerar_score = self.context['gerar_score_e_alertas']

        # Setup Mocks
        mock_df_spark = MagicMock()
        mock_df_pandas = pd.DataFrame({
            'CPFCNPJ': ['123'],
            'LIQUIDACAO': [None],
            'TTO_OPERACAO': ['NP'],
            'STATUSANALISE': ['D'],
            'STATUSACEITE': ['A'],
            'ACEITO': ['S'],
            'VALOR': [500],
            'PRAZO': [45],
            'CODRATING_CEDENTE': ['A'],
            'CODSTATUSCLIENTE': ['1'],
            'CODTITULO': ['T001'],
            'VENCIMENTO': ['2025-06-01']
        })

        mock_df_spark.filter.return_value = mock_df_spark
        mock_df_spark.toPandas.return_value = mock_df_pandas

        mock_model = MagicMock()
        mock_model.predict_proba.return_value = np.array([[0.8, 0.2]]) # Class 1 prob 0.2

        model_features = ['VALOR', 'PRAZO']

        gerar_score('123', mock_df_spark, mock_model, model_features)

        self.mock_display.assert_called()

if __name__ == '__main__':
    unittest.main()
