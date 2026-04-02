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
    "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Gold_Dim_Danfe.Notebook/notebook-content.py"
)

class TestParseDanfe(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print(f"Extracting parse_danfe from {NOTEBOOK_PATH}")
        cls.func_source = extract_function_from_file(NOTEBOOK_PATH, "parse_danfe")
        if not cls.func_source:
             raise ValueError("Function parse_danfe not found in notebook.")

    def test_parse_danfe_calls(self):
        # Simulações para funções do PySpark
        mock_col = MagicMock(name="col")
        mock_substring = MagicMock(name="substring")

        # Simula comportamento de Column
        def col_side_effect(name):
            m = MagicMock(name=f"col('{name}')")
            return m

        mock_col.side_effect = col_side_effect

        # Simula DataFrame
        mock_df = MagicMock(name="df")

        # withColumn encadeável
        mock_df.withColumn.return_value = mock_df
        # withColumnRenamed encadeável
        mock_df.withColumnRenamed.return_value = mock_df

        # Contexto de execução
        exec_globals = {
            'col': mock_col,
            'substring': mock_substring,
        }

        # # Executa a função definition
        local_scope = {}
        exec(self.func_source, exec_globals, local_scope)
        parse_danfe = local_scope['parse_danfe']

        # Chama a função
        result_df = parse_danfe(mock_df)

        # Asserções

        # 1. Verifica se as colunas esperadas foram adicionadas
        expected_cols = {
            "uf": (1, 2),
            "aamm": (3, 4),
            "cnpj": (7, 14),
            "modelo": (21, 2),
            "serie": (23, 3),
            "numero_nf": (26, 9),
            "codigo_nf": (35, 9),
            "dv": (44, 1)
        }

        # Obtém todas as chamadas para withColumn
        self.assertEqual(mock_df.withColumn.call_count, len(expected_cols))

        calls = mock_df.withColumn.call_args_list

        # Coleta colunas adicionadas
        added_cols = [c[0][0] for c in calls]
        for col_name in expected_cols:
            self.assertIn(col_name, added_cols)

        # 2. Verifica substring calls
        self.assertEqual(mock_substring.call_count, len(expected_cols))

        substring_calls = mock_substring.call_args_list

        # Verifica se temos uma chamada substring correspondente a cada parâmetro de coluna esperado
        # Não podemos vincular facilmente a chamada substring à chamada withColumn sem uma simulação mais complexa,
        # mas podemos verificar se o conjunto de chamadas de substring atende às nossas expectativas.

        expected_params = list(expected_cols.values()) # Lista de (início, comprimento)

        found_params = []
        for call_args in substring_calls:
            args, _ = call_args
            # args[0] é o objeto col, args[1] é início, args[2] é comprimento
            found_params.append((args[1], args[2]))

        # Classifica ambas as listas para comparar
        expected_params.sort()
        found_params.sort()

        self.assertEqual(expected_params, found_params, "Mismatch in substring parameters")

        # 3. Verifica withColumnRenamed
        mock_df.withColumnRenamed.assert_called_once_with("CHAVEDANFE", "chave_danfe")

        # 4. Verifica se col foi chamado com CHAVEDANFE pelo menos uma vez
        # Na realidade é chamado para cada chamada substring.
        col_calls = mock_col.call_args_list
        for call_args in col_calls:
            self.assertEqual(call_args[0][0], "CHAVEDANFE")

if __name__ == '__main__':
    unittest.main()
