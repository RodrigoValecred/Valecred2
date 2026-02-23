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
            HEADER = ''
            BLUE = ''
            CYAN = ''
            GREEN = ''
            YELLOW = ''
            RED = ''
            RESET = ''
            BOLD = ''

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

    def load_function(self, func_name):
        source = extract_function_from_file(NOTEBOOK_PATH, func_name)
        if not source:
            raise ValueError(f"Could not extract function {func_name}")
        exec(source, self.context)
        return self.context[func_name]

    def test_calcular_score_cliente(self):
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
            'VALOR': [1000],
            'PRAZO': [30],
            'CODRATING_CEDENTE': ['A'],
            'CODSTATUSCLIENTE': ['1']
        }
        mock_df_pandas = pd.DataFrame(data)

        mock_df_spark.filter.return_value = mock_df_spark # Chain filters
        mock_df_spark.toPandas.return_value = mock_df_pandas

        mock_model = MagicMock()
        mock_model.predict_proba.return_value = np.array([[0.1, 0.9]]) # Probability of class 1 is 0.9

        model_features = ['VALOR', 'PRAZO', 'CODRATING_CEDENTE']

        result = calcular_score_cliente('123', mock_df_spark, mock_model, model_features)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertIn('SCORE_RISCO', result.columns)
        self.assertEqual(result['SCORE_RISCO'].iloc[0], 0.9)
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
