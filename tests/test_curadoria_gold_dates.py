import unittest
import sys
import os
from unittest.mock import MagicMock

# Garante que o diretório tests esteja no path para importar notebook_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from notebook_utils import extract_function_from_file

# Define o caminho para o arquivo do notebook em relação à raiz do repositório
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOK_PATH = os.path.join(
    REPO_ROOT,
    "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Curadoria_Gold.Notebook/notebook-content.py"
)

class TestFunnelDates(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print(f"Extracting calculate_funnel_dates from {NOTEBOOK_PATH}")
        cls.func_source = extract_function_from_file(NOTEBOOK_PATH, "calculate_funnel_dates")
        if not cls.func_source:
             raise ValueError("Function calculate_funnel_dates not found in notebook.")

    def test_calculate_funnel_dates_logic(self):
        # Mocks
        col_mocks = {}
        def col_side_effect(name):
            if name not in col_mocks:
                m = MagicMock(name=f"col({name})")
                col_mocks[name] = m
            return col_mocks[name]

        mock_col = MagicMock(side_effect=col_side_effect)
        mock_greatest = MagicMock(name="greatest")
        mock_coalesce = MagicMock(name="coalesce")

        # Simula DataFrame
        mock_df = MagicMock(name="df")
        # withColumn retorna df
        mock_df.withColumn.return_value = mock_df

        # Globais de execução
        exec_globals = {
            'col': mock_col,
            'greatest': mock_greatest,
            'coalesce': mock_coalesce,
        }

        # Carrega a função
        local_scope = {}
        exec(self.func_source, exec_globals, local_scope)
        calculate_funnel_dates = local_scope['calculate_funnel_dates']

        # Execution
        result_df = calculate_funnel_dates(mock_df)

        # Verification
        self.assertEqual(result_df, mock_df)

        # Verifique transformações específicas
        # .withColumn("data_aprovacao", greatest(col("pivot_checklist"), col("pivot_assinatura")))

        # Verifica se greatest foi chamado para data_aprovacao
        # Esperamos que greatest seja chamado com colunas simuladas específicas
        arg1 = col_mocks["pivot_checklist"]
        arg2 = col_mocks["pivot_assinatura"]
        mock_greatest.assert_any_call(arg1, arg2)

        # .withColumn("data_conclusao", coalesce(col("pivot_bizagi"), col("pivot_concluido")))
        arg3 = col_mocks["pivot_bizagi"]
        arg4 = col_mocks["pivot_concluido"]
        mock_coalesce.assert_any_call(arg3, arg4)

        # Verifica chamadas df.withColumn
        calls = mock_df.withColumn.call_args_list
        column_names = [c[0][0] for c in calls]

        self.assertIn("data_aprovacao", column_names)
        self.assertIn("data_conclusao", column_names)
        self.assertIn("data_comite", column_names)
        self.assertIn("data_reserva", column_names)
        self.assertIn("data_entrada", column_names)

if __name__ == '__main__':
    unittest.main()
