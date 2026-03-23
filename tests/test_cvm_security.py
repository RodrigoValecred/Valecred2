import ast
import os
import sys
import unittest

# Garante que possamos importar do pacote tests
sys.path.append(os.getcwd())
from tests.notebook_utils import extract_function_from_file

class TestCVMSecurity(unittest.TestCase):
    def test_requests_timeout(self):
        # Localiza o arquivo do notebook
        # Assumindo que executamos da raiz do repositório
        filepath = "VALECRED_DEV/5_Notebooks/Dados_Externos/CVM/NB_Load_From_CVM.Notebook/notebook-content.py"

        if not os.path.exists(filepath):
            self.fail(f"Notebook file not found at {filepath}")

        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()

        # Limpa comandos mágicos (embora normalmente não sejam esperados em conteúdo .py de notebooks fabric, mas é seguro ter)
        lines = content.splitlines()
        clean_lines = []
        for line in lines:
            if line.strip().startswith("%") or line.strip().startswith("!"):
                clean_lines.append(f"# {line}")
            else:
                clean_lines.append(line)
        source = "\n".join(clean_lines)

        try:
            tree = ast.parse(source)
        except SyntaxError as e:
            self.fail(f"Syntax error parsing notebook: {e}")

        requests_calls = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                # Verifica requests.get ou requests.head
                is_requests_call = False
                if isinstance(node.func, ast.Attribute) and node.func.attr in ['get', 'head']:
                     if isinstance(node.func.value, ast.Name) and node.func.value.id == 'requests':
                        is_requests_call = True

                if is_requests_call:
                    requests_calls.append(node)

        self.assertTrue(len(requests_calls) > 0, "No requests.get/head calls found in the notebook")

        for call in requests_calls:
            keywords = {kw.arg: kw.value for kw in call.keywords}
            func_name = call.func.attr

            # Verifica timeout
            if 'timeout' not in keywords:
                self.fail(f"requests.{func_name} call at line {call.lineno} missing 'timeout' argument (DoS risk)")

    def test_requests_stream(self):
        # Localiza o arquivo do notebook
        filepath = "VALECRED_DEV/5_Notebooks/Dados_Externos/CVM/NB_Load_From_CVM.Notebook/notebook-content.py"

        if not os.path.exists(filepath):
            self.fail(f"Notebook file not found at {filepath}")

        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()

        # Clean magic commands
        lines = content.splitlines()
        clean_lines = []
        for line in lines:
            if line.strip().startswith("%") or line.strip().startswith("!"):
                clean_lines.append(f"# {line}")
            else:
                clean_lines.append(line)
        source = "\n".join(clean_lines)

        try:
            tree = ast.parse(source)
        except SyntaxError as e:
            self.fail(f"Syntax error parsing notebook: {e}")

        requests_calls = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                # Verifica requests.get (HEAD não baixa conteúdo, portanto stream é irrelevante/implícito)
                is_requests_call = False
                if isinstance(node.func, ast.Attribute) and node.func.attr == 'get':
                     if isinstance(node.func.value, ast.Name) and node.func.value.id == 'requests':
                        is_requests_call = True

                if is_requests_call:
                    requests_calls.append(node)

        # Verifica se encontramos alguma chamada
        self.assertTrue(len(requests_calls) > 0, "No requests.get calls found in the notebook")

        for call in requests_calls:
            keywords = {kw.arg: kw.value for kw in call.keywords}
            func_name = call.func.attr

            # Verifica stream=True
            if 'stream' not in keywords:
                self.fail(f"requests.{func_name} call at line {call.lineno} missing 'stream=True' argument (Memory Exhaustion risk)")

            # Verifica se o valor de stream é explicitamente True
            stream_val = keywords['stream']
            if not (isinstance(stream_val, ast.Constant) and stream_val.value is True):
                 self.fail(f"requests.{func_name} call at line {call.lineno} has 'stream' argument but it is not set to True")

class TestCVMPeriodValidation(unittest.TestCase):
    def setUp(self):
        # Localiza o arquivo do notebook
        self.notebook_path = "VALECRED_DEV/5_Notebooks/Dados_Externos/CVM/NB_Load_From_CVM.Notebook/notebook-content.py"

        # Extrai o código fonte da função
        self.func_source = extract_function_from_file(self.notebook_path, "validate_periodo")

        if self.func_source is None:
            self.fail(f"Function validate_periodo not found in {self.notebook_path}")

        # # Executa a função definition in a local scope
        self.exec_globals = {}
        exec(self.func_source, self.exec_globals)
        self.validate_periodo = self.exec_globals["validate_periodo"]

    def test_valid_periodos(self):
        """Test valid period formats (YYYYMM)."""
        valid_cases = ["202501", "201012", "202409", "205001"]
        for p in valid_cases:
            with self.subTest(periodo=p):
                self.assertTrue(self.validate_periodo(p), f"Should accept valid period: {p}")

    def test_invalid_periodos_length(self):
        """Testa períodos com comprimento incorreto."""
        invalid_cases = ["2025", "2025011", "1", ""]
        for p in invalid_cases:
            with self.subTest(periodo=p):
                with self.assertRaisesRegex(ValueError, "Período deve ter o formato YYYYMM"):
                    self.validate_periodo(p)

    def test_invalid_periodos_year(self):
        """Testa períodos com ano fora dos limites (2010-2050)."""
        invalid_cases = ["200912", "199912", "205101", "300001", "000001"]
        for p in invalid_cases:
            with self.subTest(periodo=p):
                with self.assertRaisesRegex(ValueError, "Ano inválido"):
                    self.validate_periodo(p)

    def test_invalid_periodos_month(self):
        """Testa períodos com mês fora dos limites (1-12)."""
        invalid_cases = ["202500", "202513", "202599"]
        for p in invalid_cases:
            with self.subTest(periodo=p):
                with self.assertRaisesRegex(ValueError, "Mês inválido"):
                    self.validate_periodo(p)

    def test_invalid_periodos_content(self):
        """Testa períodos com caracteres não dígitos."""
        invalid_cases = ["20250a", "abcdef", "2025-1", "20.250"]
        for p in invalid_cases:
            with self.subTest(periodo=p):
                with self.assertRaises(ValueError):
                    self.validate_periodo(p)

    def test_invalid_types(self):
        """Test non-string inputs."""
        invalid_cases = [202501, None, ["202501"], 123456]
        for p in invalid_cases:
            with self.subTest(periodo=p):
                with self.assertRaises((TypeError, ValueError)):
                    self.validate_periodo(p)

if __name__ == '__main__':
    unittest.main()
