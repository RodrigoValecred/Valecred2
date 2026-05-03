import unittest
import sys
import os
import pandas as pd
import numpy as np
from unittest.mock import MagicMock

# Garante que o diretório tests esteja no path para importar notebook_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from notebook_utils import extract_function_from_file

# Define o caminho para o arquivo do notebook em relação à raiz do repositório
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOK_PATH = os.path.join(
    REPO_ROOT,
    "VALECRED_DEV/6_Machine_Learning/ML_Previsao_Inadimplencia_2025.Notebook/notebook-content.py"
)

class TestMLPrevisaoInadimplencia(unittest.TestCase):

    def setUp(self):
        # Extrai o código fonte de predict_proba_udf do notebook
        self.func_source = extract_function_from_file(NOTEBOOK_PATH, "predict_proba_udf")
        if not self.func_source:
             self.fail(f"Function predict_proba_udf not found in notebook at {NOTEBOOK_PATH}.")

    def test_predict_proba_udf_logic(self):
        """
        Testa a lógica principal da UDF:
        1. Usa corretamente variáveis broadcast.
        2. Reconstrói o DataFrame.
        3. Gerencia conversões categóricas.
        4. Chama a previsão do modelo.
        5. Retorna a coluna de probabilidade correta.
        """

        # --- Mocks ---

        # Simula features_broadcast
        mock_features = ['feature_A', 'CODSTATUSCLIENTE', 'CODRATING_CEDENTE', 'feature_B']
        mock_features_broadcast = MagicMock()
        mock_features_broadcast.value = mock_features

        # Simula model_broadcast e o próprio modelo
        mock_model = MagicMock()
        # Simula predict_proba para retornar array como [[prob_0, prob_1], ...]
        # Simulamos 2 linhas
        mock_model.predict_proba.return_value = np.array([
            [0.3, 0.7],  # Linha 1: prob_1 = 0.7
            [0.8, 0.2]   # Linha 2: prob_1 = 0.2
        ])
        mock_model_broadcast = MagicMock()
        mock_model_broadcast.value = mock_model

        # Simula decorador pandas_udf
        # Precisa lidar com a chamada como @pandas_udf(DoubleType())
        def mock_pandas_udf(*args, **kwargs):
            def decorator(f):
                return f
            return decorator

        # Simula DoubleType
        mock_double_type = MagicMock()

        # --- Contexto de Execução ---

        from typing import Iterator, Tuple

        exec_globals = {
            'pandas_udf': mock_pandas_udf,
            'DoubleType': lambda: mock_double_type, # Chamado como DoubleType()
            'pd': pd,
            'features_broadcast': mock_features_broadcast,
            'model_broadcast': mock_model_broadcast,
            'Iterator': Iterator,
            'Tuple': Tuple
        }

        # Executa o código extraído para obter o objeto da função
        local_scope = {}
        try:
            exec(self.func_source, exec_globals, local_scope)
        except Exception as e:
            self.fail(f"Failed to execute extracted function source: {e}")

        predict_proba_udf = local_scope['predict_proba_udf']

        # --- Prepare Dados de Entrada ---

        # Cria Series de entrada para o UDF
        # feature_A (numeric), CODSTATUSCLIENTE (categorical), CODRATING_CEDENTE (categorical), feature_B (numeric)
        col_feature_A = pd.Series([10.0, 20.0])
        col_cod_status = pd.Series(['Active', 'Inactive'])
        col_cod_rating = pd.Series(['A', 'B'])
        col_feature_B = pd.Series([5.0, 15.0])

        # --- Executa o UDF ---

        # O UDF recebe iterator de tuple de colunas
        input_iterator = iter([(col_feature_A, col_cod_status, col_cod_rating, col_feature_B)])
        result_iterator = predict_proba_udf(input_iterator)

        # O UDF é um gerador
        result_series = next(result_iterator)

        # --- Asserções ---

        # 1. Verifica Result
        expected_probs = pd.Series([0.7, 0.2])
        pd.testing.assert_series_equal(result_series, expected_probs)

        # 2. Verifica a chamada do modelo (Model Call)
        # Verifica se model.predict_proba foi chamado com o DataFrame correto
        mock_model.predict_proba.assert_called_once()
        call_args = mock_model.predict_proba.call_args[0][0] # O primeiro argumento é X

        self.assertIsInstance(call_args, pd.DataFrame)
        self.assertListEqual(list(call_args.columns), mock_features)

        # 3. Verifica Categorical Conversion
        # 'CODSTATUSCLIENTE' e 'CODRATING_CEDENTE' devem ser do tipo category
        self.assertTrue(isinstance(call_args['CODSTATUSCLIENTE'].dtype, pd.CategoricalDtype),
                        "CODSTATUSCLIENTE should be converted to categorical dtype")
        self.assertTrue(isinstance(call_args['CODRATING_CEDENTE'].dtype, pd.CategoricalDtype),
                        "CODRATING_CEDENTE should be converted to categorical dtype")

        # 'feature_A' e 'feature_B' NÃO devem ser categóricas
        self.assertFalse(isinstance(call_args['feature_A'].dtype, pd.CategoricalDtype))
        self.assertFalse(isinstance(call_args['feature_B'].dtype, pd.CategoricalDtype))

if __name__ == '__main__':
    unittest.main()
