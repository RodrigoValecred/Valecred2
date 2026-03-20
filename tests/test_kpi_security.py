import ast
import os
import unittest

class TestKPISecurity(unittest.TestCase):
    def test_no_hardcoded_passwords(self):
        filepath = "VALECRED_DEV/8_RealTime/KPI_DA_TV.Notebook/notebook-content.py"
        if not os.path.exists(filepath):
            # Tenta encontrar em relação ao diretório de testes se executado a partir da raiz
            filepath = os.path.join(os.getcwd(), filepath)

        if not os.path.exists(filepath):
             self.fail(f"Notebook file not found at {filepath}")

        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()

        # Limpa comandos mágicos se houver (embora este arquivo pareça limpo)
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

        option_calls = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                # Verifica chamadas para .option()
                if isinstance(node.func, ast.Attribute) and node.func.attr == 'option':
                    option_calls.append(node)

        found_password_option = False
        for call in option_calls:
            # Verifica args
            if len(call.args) >= 2:
                key_arg = call.args[0]
                value_arg = call.args[1]

                # Verifica se o primeiro argumento é o literal "password"
                if isinstance(key_arg, ast.Constant) and key_arg.value == "password":
                    found_password_option = True
                    # Assert second arg is NOT a string literal
                    if isinstance(value_arg, ast.Constant) and isinstance(value_arg.value, str):
                        self.fail(f"CRITICAL: Hardcoded password detected! Found: .option('password', '{value_arg.value}')")

            # Verifica kwargs (improvável para spark .option mas possível em python)
            # Spark .option(key, value) usually positional

        # Garante que encontramos a chamada option (para o teste não passar falsamente por perder o código)
        self.assertTrue(found_password_option, "Did not find any .option('password', ...) call. Code structure might have changed.")

if __name__ == '__main__':
    unittest.main()
