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

class TestCreateFatoOperacoes(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print(f"Extracting functions from {NOTEBOOK_PATH}")
        cls.func_source = extract_function_from_file(NOTEBOOK_PATH, "create_fato_operacoes")
        if not cls.func_source:
             raise ValueError("Function create_fato_operacoes not found in notebook.")

        cls.helper1_source = extract_function_from_file(NOTEBOOK_PATH, "prepare_operacoes_dataframe")
        if not cls.helper1_source:
             raise ValueError("Function prepare_operacoes_dataframe not found in notebook.")

        cls.helper2_source = extract_function_from_file(NOTEBOOK_PATH, "join_operacoes_dimensions")
        if not cls.helper2_source:
             raise ValueError("Function join_operacoes_dimensions not found in notebook.")

        cls.helper3_source = extract_function_from_file(NOTEBOOK_PATH, "select_fato_operacoes_columns")
        if not cls.helper3_source:
             raise ValueError("Function select_fato_operacoes_columns not found in notebook.")

    def test_sk_produto_join(self):
        # Dicionário para armazenar simulações criadas por col()
        col_mocks = {}

        def col_side_effect(name):
            if name not in col_mocks:
                m = MagicMock(name=f"col({name})")
                # Support ~col(...)
                m.__invert__ = MagicMock(return_value=MagicMock(name=f"~col({name})"))
                col_mocks[name] = m
            return col_mocks[name]

        mock_col = MagicMock(side_effect=col_side_effect)
        mock_lit = MagicMock(return_value=MagicMock(name="lit_mock"))
        mock_to_date = MagicMock(return_value=MagicMock(name="to_date_mock"))
        mock_broadcast = MagicMock(side_effect=lambda x: x)

        # DataFrame Mocks
        mock_df_ops = MagicMock()
        mock_df_cal = MagicMock()
        mock_df_prod = MagicMock()

        # Configura simulações de encadeamento de métodos
        mock_df_prep = MagicMock()
        mock_df_ops.withColumn.return_value = mock_df_prep

        mock_df_prep_2 = MagicMock()
        mock_df_prep.withColumn.return_value = mock_df_prep_2

        # Filter should be applied before joins (Optimization)
        mock_df_filtered = MagicMock()
        mock_df_prep_2.filter.return_value = mock_df_filtered

        mock_df_joined_1 = MagicMock()
        mock_df_filtered.join.return_value = mock_df_joined_1

        mock_df_joined_2 = MagicMock()
        mock_df_joined_1.join.return_value = mock_df_joined_2

        mock_df_selected = MagicMock()
        mock_df_joined_2.select.return_value = mock_df_selected

        mock_df_final = MagicMock()
        mock_df_selected.dropDuplicates.return_value = mock_df_final

        # Execution Context (Globals)
        exec_globals = {
            'col': mock_col,
            'lit': mock_lit,
            'to_date': mock_to_date,
            'broadcast': mock_broadcast,
        }
        # Adiciona outras funções potenciais como simulações fictícias
        for func_name in ['when', 'length', 'regexp_replace', 'collect_list', 'concat_ws', 'upper', 'greatest',
                          'substring', 'year', 'lead', 'date_add', 'lag', 'max', 'coalesce', 'dayofweek',
                          'dayofmonth', 'date_sub', 'trim', 'datediff', 'sum', 'min', 'count', 'round',
                          'floor', 'least', 'current_date', 'split', 'pow', 'xxhash64']:
             exec_globals[func_name] = MagicMock()

        # # Executa a definição da função
        local_scope = {}
        # Auxiliares devem estar em exec_globals para que create_fato_operacoes os veja
        exec(self.helper1_source, exec_globals)
        exec(self.helper2_source, exec_globals)
        exec(self.helper3_source, exec_globals)

        exec(self.func_source, exec_globals, local_scope)
        create_fato_operacoes = local_scope['create_fato_operacoes']

        # Executa a função
        result = create_fato_operacoes(mock_df_ops, mock_df_cal, mock_df_prod)

        # Asserções

        # 1. Verifica se df_dim_produto.select foi chamado com "chave_produto" e "sk_produto"
        mock_df_prod.select.assert_called_with("chave_produto", "sk_produto")

        # 2. Verifica join
        mock_df_joined_1.join.assert_called()
        args, kwargs = mock_df_joined_1.join.call_args
        self.assertEqual(args[1], "chave_produto")
        self.assertEqual(args[2], "left")

        # 3. Verifica Select includes sk_produto
        mock_df_joined_2.select.assert_called()
        call_args = mock_df_joined_2.select.call_args[0]

        self.assertIn("sk_produto", col_mocks)
        sk_prod_mock = col_mocks["sk_produto"]
        self.assertIn(sk_prod_mock, call_args)

        self.assertIn("sk_operacao", col_mocks)
        sk_op_mock = col_mocks["sk_operacao"]
        self.assertIn(sk_op_mock, call_args)

if __name__ == '__main__':
    unittest.main()
