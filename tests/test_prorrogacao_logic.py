import unittest
from unittest.mock import MagicMock, call, patch
import sys
import os

# Ensure the tests directory is in the path to import notebook_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from notebook_utils import extract_function_from_file

# Define the path to the notebook file relative to the repository root
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOK_PATH = os.path.join(
    REPO_ROOT,
    "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Prepara_Tabela_Operacoes.Notebook/notebook-content.py"
)

class TestProrrogacaoLogic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print(f"Extracting process_tab_operacoes_prorrogacao from {NOTEBOOK_PATH}")
        cls.func_source = extract_function_from_file(NOTEBOOK_PATH, "process_tab_operacoes_prorrogacao")
        if not cls.func_source:
             raise ValueError("Function process_tab_operacoes_prorrogacao not found in notebook.")

    def test_process_logic(self):
        # Mocks for global dependencies
        mock_spark = MagicMock()
        mock_col = MagicMock()
        mock_to_date = MagicMock()
        mock_lit = MagicMock()

        # Mock DataFrames
        mock_df_prorrogacao = MagicMock(name="df_prorrogacao")
        mock_df_titulos = MagicMock(name="df_titulos")
        mock_df_operacoes = MagicMock(name="df_operacoes")

        # Setup read.table side effect
        def read_table_side_effect(table_name):
            if "tab_operacoes_prorrogacao" in table_name:
                return mock_df_prorrogacao
            elif "staging_titulos_limpa" in table_name:
                return mock_df_titulos
            elif "staging_operacoes_limpa" in table_name:
                return mock_df_operacoes
            return MagicMock()

        mock_spark.read.table.side_effect = read_table_side_effect

        # Setup columns for prorrogacao
        mock_df_prorrogacao.columns = [
            "CODTITULO", "CODOPERACAO", "DATAINCLUSAO", "TARIFA",
            "USUAINCLUSAO", "DATAALTERACAO", "USUAALTERACAO",
            "VALORDEVIDO", "VALORPROR", "VALORBOLETO"
        ]

        # Setup col() mock to support alias chaining
        def col_side_effect(name):
            m = MagicMock(name=f"col('{name}')")
            m.alias.return_value = m
            return m
        mock_col.side_effect = col_side_effect

        # Setup chaining
        mock_df_prorrogacao_select = MagicMock(name="df_prorrogacao_select")
        mock_df_prorrogacao.select.return_value = mock_df_prorrogacao_select

        mock_df_prorrogacao_renamed = MagicMock(name="df_prorrogacao_renamed")
        mock_df_prorrogacao_select.withColumnRenamed.return_value = mock_df_prorrogacao_renamed
        mock_df_prorrogacao_renamed.withColumnRenamed.return_value = mock_df_prorrogacao_renamed

        mock_df_titulos_select = MagicMock(name="df_titulos_select")
        mock_df_titulos.select.return_value = mock_df_titulos_select

        mock_df_operacoes_select = MagicMock(name="df_operacoes_select")
        mock_df_operacoes.select.return_value = mock_df_operacoes_select

        mock_df_joined_1 = MagicMock(name="df_joined_1")
        mock_df_prorrogacao_renamed.join.return_value = mock_df_joined_1

        mock_df_joined_2 = MagicMock(name="df_joined_2")
        mock_df_joined_1.join.return_value = mock_df_joined_2

        mock_df_transformed = MagicMock(name="df_transformed")
        mock_df_joined_2.withColumn.return_value = mock_df_transformed

        mock_df_final = MagicMock(name="df_final")
        mock_df_transformed.drop.return_value = mock_df_final

        # Global context
        exec_globals = {
            'spark': mock_spark,
            'col': mock_col,
            'to_date': mock_to_date,
            'lit': mock_lit,
            'source_lakehouse': 'LH_Bronze',
            'target_lakehouse': 'LH_Silver',
            'check_should_skip': MagicMock(return_value=False)
        }

        local_scope = {}
        exec(self.func_source, exec_globals, local_scope)
        process_func = local_scope['process_tab_operacoes_prorrogacao']

        # Execute
        process_func()

        # Assertions
        # Check Drop with CORRECT names (lowercase without underscores)
        mock_df_transformed.drop.assert_called()
        args, _ = mock_df_transformed.drop.call_args
        expected_dropped = [
            "tarifa", "usuainclusao", "dataalteracao", "usuaalteracao",
            "valordevido", "valorpror", "valorboleto"
        ]
        for col_name in expected_dropped:
            self.assertIn(col_name, args, f"Expected {col_name} to be dropped")

if __name__ == '__main__':
    unittest.main()
