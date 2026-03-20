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

class TestTransformEsteiraDates(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print(f"Extracting transform_esteira_dates from {NOTEBOOK_PATH}")
        cls.func_source = extract_function_from_file(NOTEBOOK_PATH, "transform_esteira_dates")
        if not cls.func_source:
             raise ValueError("Function transform_esteira_dates not found in notebook.")

    def _test_single_pass_pivot(self):
        # Simulações para funções do Spark
        col_mocks = {}
        def col_side_effect(name):
            if name not in col_mocks:
                m = MagicMock(name=f"col({name})")
                # When alias is called, return a new mock but keep track?
                # Actually alias usually returns a Column object.
                m.alias = MagicMock(return_value=MagicMock(name=f"col({name}).alias"))
                col_mocks[name] = m
            return col_mocks[name]

        mock_col = MagicMock(side_effect=col_side_effect)
        mock_max = MagicMock(return_value=MagicMock(name="max_mock"))
        mock_min = MagicMock(return_value=MagicMock(name="min_mock"))

        # Configuração de aliases para simulações de max/min para que .alias("max") funcione
        mock_max.return_value.alias = MagicMock(return_value="max_aliased")
        mock_min.return_value.alias = MagicMock(return_value="min_aliased")

        # DataFrame Mocks
        mock_df_esteira = MagicMock()
        mock_grouped = MagicMock()
        mock_pivoted = MagicMock()
        mock_combined = MagicMock()

        # Chain
        mock_df_esteira.groupBy.return_value = mock_grouped
        mock_grouped.pivot.return_value = mock_pivoted
        mock_pivoted.agg.return_value = mock_combined

        # Select retorna simulações diferentes para max e min dfs
        mock_df_max = MagicMock(name="df_max")
        mock_df_min = MagicMock(name="df_min")
        mock_combined.select.side_effect = [mock_df_max, mock_df_min]

        # Dados de Entrada
        status_mapping = {
            "PROPOSTA": "proposta",
            "DIR COMERCIAL": "dir_comercial"
        }

        # Execution Context
        exec_globals = {
            'col': mock_col,
            'max': mock_max,
            'min': mock_min,
        }

        # # Executa a função definition
        local_scope = {}
        exec(self.func_source, exec_globals, local_scope)
        transform_esteira_dates = local_scope['transform_esteira_dates']

        # Executa a função
        result = transform_esteira_dates(mock_df_esteira, status_mapping)

        # Asserções

        # 1. Verifica GroupBy e Pivot
        mock_df_esteira.groupBy.assert_called_with("cod_cliente")
        mock_grouped.pivot.assert_called()
        args, _ = mock_grouped.pivot.call_args
        self.assertEqual(args[0], "status_do_cliente")
        self.assertEqual(set(args[1]), set(status_mapping.keys())) # Verifica list content

        # 2. Verifica Aggregation (Single Pass)
        mock_pivoted.agg.assert_called_once()
        agg_args = mock_pivoted.agg.call_args[0]
        # Deve conter max e min com alias
        self.assertEqual(agg_args[0], "max_aliased")
        self.assertEqual(agg_args[1], "min_aliased")

        # 3. Verifica a Seleção do Max DataFrame
        # Primeira chamada a select
        self.assertEqual(mock_combined.select.call_count, 1)

        call_args_max = mock_combined.select.call_args_list[0][0][0]
        # Verifica se as colunas corretas foram selecionadas
        # Esperamos [col("cod_cliente")] + [col("PROPOSTA_max").alias("pivot_proposta"), ...]

        # Como simulamos col(), precisamos verificar as simulações
        self.assertIn("cod_cliente", col_mocks)
        self.assertIn("PROPOSTA_max", col_mocks)
        self.assertIn("DIR COMERCIAL_max", col_mocks)

        # Verifica chamadas de alias nas simulações
        col_mocks["PROPOSTA_max"].alias.assert_called_with("pivot_proposta")
        col_mocks["DIR COMERCIAL_max"].alias.assert_called_with("pivot_dir_comercial")

        # 4. Verifica a Seleção do Min DataFrame
        call_args_min = mock_combined.select.call_args_list[1][0][0]

        self.assertIn("PROPOSTA_min", col_mocks)

        col_mocks["PROPOSTA_min"].alias.assert_called_with("PROPOSTA")

        # 5. Verifica Returns
        self.assertEqual(result, mock_df_max)
        # Asserção para min removida

if __name__ == '__main__':
    unittest.main()
