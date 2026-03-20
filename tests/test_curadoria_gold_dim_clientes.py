import unittest
import sys
import os
from unittest.mock import MagicMock, call

# Garante que o diretório tests esteja no path para importar notebook_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from notebook_utils import extract_function_from_file

# Define o caminho para o arquivo do notebook em relação à raiz do repositório
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOK_PATH = os.path.join(
    REPO_ROOT,
    "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Curadoria_Gold.Notebook/notebook-content.py"
)

class TestJoinClienteDimensions(unittest.TestCase):

    def test_join_structure(self):
        func_source = extract_function_from_file(NOTEBOOK_PATH, "join_cliente_dimensions")
        if not func_source:
             self.fail("Function join_cliente_dimensions not found in notebook. Please implement it.")

        # Simulações para DataFrames
        mock_df_base = MagicMock(name="df_base")
        mock_df_cad_geral = MagicMock(name="df_cad_geral")
        mock_df_metrics_ops = MagicMock(name="df_metrics_ops")
        mock_df_metrics_titulos = MagicMock(name="df_metrics_titulos")
        mock_df_esteira_pivot = MagicMock(name="df_esteira_pivot")
        mock_df_esteira_min = MagicMock(name="df_esteira_min")
        mock_df_esteira_latest = MagicMock(name="df_esteira_latest")
        mock_df_limites_agg = MagicMock(name="df_limites_agg")
        mock_df_grupos_prep = MagicMock(name="df_grupos_prep")
        mock_df_limites_grupo = MagicMock(name="df_limites_grupo")
        mock_df_risco_grupo = MagicMock(name="df_risco_grupo")
        mock_df_info_gestor = MagicMock(name="df_info_gestor")
        mock_df_client_rate = MagicMock(name="df_client_rate")
        mock_df_status_cad = MagicMock(name="df_status_cad")

        # Simula atributos necessários para condições de junção
        mock_df_base.cod_cliente = MagicMock(name="df_base.cod_cliente")
        mock_df_esteira_pivot.cod_cliente_pivot = MagicMock(name="df_esteira_pivot.cod_cliente_pivot")
        mock_df_esteira_min.cod_cliente_min = MagicMock(name="df_esteira_min.cod_cliente_min")
        mock_df_esteira_latest.cod_cliente_latest = MagicMock(name="df_esteira_latest.cod_cliente_latest")
        mock_df_client_rate.cod_cliente_rate = MagicMock(name="df_client_rate.cod_cliente_rate")
        mock_df_status_cad.cod_cliente_status = MagicMock(name="df_status_cad.cod_cliente_status")

        # Configure joins to return a chainable mock
        # Não precisamos ser muito rigorosos sobre os valores de retorno desde que tenham .join() e .drop()

        # Execution Context
        exec_globals = {
            'col': MagicMock(),
            'lit': MagicMock(),
        }

        local_scope = {}
        exec(func_source, exec_globals, local_scope)
        join_cliente_dimensions = local_scope['join_cliente_dimensions']

        # Executa função
        result = join_cliente_dimensions(
            mock_df_base,
            mock_df_cad_geral,
            mock_df_metrics_ops,
            mock_df_metrics_titulos,
            mock_df_esteira_pivot,
            mock_df_esteira_latest,
            mock_df_limites_agg,
            mock_df_grupos_prep,
            mock_df_limites_grupo,
            mock_df_risco_grupo,
            mock_df_info_gestor,
            mock_df_client_rate,
            mock_df_status_cad
        )

        # Verifica result is a DataFrame (mock)
        self.assertTrue(isinstance(result, MagicMock))

        # Verificação básica: df_base.join foi chamado
        mock_df_base.join.assert_called()

if __name__ == '__main__':
    unittest.main()
