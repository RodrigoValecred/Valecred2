import unittest
import sys
import os

# Garante que o diretório tests esteja no path para importar notebook_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from notebook_utils import extract_function_from_file

# Define o caminho para o arquivo do notebook em relação à raiz do repositório
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOK_PATH = os.path.join(
    REPO_ROOT,
    "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Prepara_Tabela_Cadastros.Notebook/notebook-content.py"
)

class TestSiglaExpr(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.func_source = extract_function_from_file(NOTEBOOK_PATH, "get_sigla_expr")
        if not cls.func_source:
             raise ValueError("Function get_sigla_expr not found in notebook.")

    def test_sigla_expression_structure(self):
        # Classe auxiliar para simular objeto Column com sobrecarga de operadores
        class MockColumn(str):
            def __new__(cls, content):
                return super().__new__(cls, content)

            def __invert__(self):
                return MockColumn(f"~{self}")

            def isin(self, other):
                return MockColumn(f"{self}.isin({other})")

            def __and__(self, other):
                return MockColumn(f"({self} & {other})")

            def __gt__(self, other):
                return MockColumn(f"({self} > {other})")

        # Simulações que retornam a representação de string da operação
        def mock_col(name):
            return MockColumn(f"col({name})")

        def mock_upper(col):
            return f"upper({col})"

        def mock_regexp_replace(col, pattern, replacement):
            return f"regexp_replace({col}, '{pattern}', '{replacement}')"

        def mock_split(col, pattern):
            return f"split({col}, '{pattern}')"

        def mock_length(col):
            return MockColumn(f"length({col})")

        def mock_substring(col, pos, length):
            return f"substring({col}, {pos}, {length})"

        def mock_array_join(col, delimiter):
            return f"array_join({col}, '{delimiter}')"

        # Lambda executors
        def mock_array_filter(col, func):
            # Executa o lambda com uma coluna dummy "x"
            # O lambda espera um objeto de coluna que suporte operadores
            res = func(MockColumn("x"))
            return f"array_filter({col}, x -> {res})"

        def mock_transform(col, func):
            res = func(MockColumn("x"))
            return f"transform({col}, x -> {res})"

        exec_globals = {
            'col': mock_col,
            'upper': mock_upper,
            'regexp_replace': mock_regexp_replace,
            'split': mock_split,
            'array_filter': mock_array_filter, # O notebook importa filtro como array_filter
            'transform': mock_transform,
            'array_join': mock_array_join,
            'length': mock_length,
            'substring': mock_substring,
        }

        local_scope = {}
        exec(self.func_source, exec_globals, local_scope)
        get_sigla_expr = local_scope['get_sigla_expr']

        # Executa a função
        result = get_sigla_expr("nome_base")

        # Lógica Esperada:
        stopwords = ["DA", "DE", "DO", "DAS", "DOS", "E", "LTDA", "S.A", "SA", "ME", "EPP", "S/A"]

        expected_part1 = "split(regexp_replace(upper(col(nome_base)), '[^A-Z0-9 ]', ''), ' ')"
        expected_filter = f"array_filter({expected_part1}, x -> ((length(x) > 0) & ~x.isin({stopwords})))"
        expected_transform = f"transform({expected_filter}, x -> substring(x, 1, 1))"
        expected_final = f"array_join({expected_transform}, '')"

        self.assertEqual(result, expected_final)

if __name__ == '__main__':
    unittest.main()
