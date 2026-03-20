import unittest
import sys
import os

try:
    from tests.notebook_utils import extract_function_from_file
except ImportError:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from notebook_utils import extract_function_from_file

NOTEBOOK_PATH = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Extrai_Observacoes_Contratos.Notebook/notebook-content.py"

class MockColumn:
    def __init__(self, name=None, expr=None):
        self.name = name
        self.expr = expr

    def cast(self, type_name):
        return MockColumn(expr=f"cast({self.expr or self.name} as {type_name})")

    def __repr__(self):
        return self.expr if self.expr else f"col({self.name})"

    def __eq__(self, other):
        return str(self) == str(other)

def mock_col(name):
    return MockColumn(name=name)

def mock_regexp_replace(col_obj, pattern, replacement):
    return MockColumn(expr=f"regexp_replace({col_obj}, '{pattern}', '{replacement}')")

class TestConverterMoedaBr(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print(f"Extracting converter_moeda_br from {NOTEBOOK_PATH}")
        func_source = extract_function_from_file(NOTEBOOK_PATH, "converter_moeda_br")

        if func_source:
            # Create a context with mock functions to be used as globals during function execution
            context = {
                "col": mock_col,
                "regexp_replace": mock_regexp_replace,
                # 'lit' é importado no notebook mas não é usado nesta função específica,
                # mas é bom ter se mudanças futuras o usarem.
                "lit": lambda x: MockColumn(expr=f"lit({x})")
            }
            local_scope = {}
            # Executa a definição da função. O corpo da função NÃO é executado aqui.
            # O objeto da função é criado e vinculado a 'converter_moeda_br' no local_scope.
            # Captura 'context' como seu escopo global porque passamos como 'globals'.
            exec(func_source, context, local_scope)
            cls.converter_moeda_br = staticmethod(local_scope["converter_moeda_br"])
        else:
            cls.converter_moeda_br = None
            print("WARNING: converter_moeda_br function not found in file.")

    def test_function_exists(self):
        self.assertIsNotNone(self.converter_moeda_br, "Function converter_moeda_br not found in notebook file.")

    def test_converter_moeda_br_structure(self):
        if not self.converter_moeda_br: self.skipTest("Function not found")

        result = self.converter_moeda_br("valor_br")

        # A lógica da função:
        # 1. regexp_replace(col(col_name), "\.", "")  -> remove thousands separator
        # 2. regexp_replace(..., ",", ".")            -> substitui vírgula decimal por ponto
        # 3. .cast("double")                          -> converte para double

        # Representação de string esperada de nossas simulações:
        # cast(regexp_replace(regexp_replace(col(valor_br), '\.', ''), ',', '.') as double)

        # Nota sobre barras invertidas:
        # No código fonte: "\." (barra invertida + ponto).
        # Em nossa simulação: isso se torna o literal de string '\.'.
        # Na string esperada abaixo: precisamos escapar a barra invertida para o literal de string do python.
        expected_expr = "cast(regexp_replace(regexp_replace(col(valor_br), '\\.', ''), ',', '.') as double)"

        self.assertIsInstance(result, MockColumn)
        self.assertEqual(str(result), expected_expr)

if __name__ == '__main__':
    unittest.main()
