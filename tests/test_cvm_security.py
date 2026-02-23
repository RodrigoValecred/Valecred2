import ast
import os
import unittest

class TestCVMSecurity(unittest.TestCase):
    def test_requests_timeout(self):
        filepath = "VALECRED_DEV/7_Dados_Externos/CVM/NB_Load_From_CVM.Notebook/notebook-content.py"
        if not os.path.exists(filepath):
            # Try finding it relative to test dir if run from there
            filepath = os.path.join("..", filepath)

        if not os.path.exists(filepath):
             self.fail(f"Notebook file not found at {filepath}")

        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()

        # Clean magic commands (lines starting with %)
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
                # Check for requests.get or requests.head
                if isinstance(node.func, ast.Attribute) and node.func.attr in ['get', 'head']:
                     if isinstance(node.func.value, ast.Name) and node.func.value.id == 'requests':
                        requests_calls.append(node)

        self.assertTrue(len(requests_calls) > 0, "No requests.get or requests.head calls found in the notebook")

        for call in requests_calls:
            method_name = call.func.attr
            keywords = {kw.arg: kw.value for kw in call.keywords}

            # Check for timeout
            self.assertIn('timeout', keywords, f"requests.{method_name} call missing 'timeout' argument (DoS risk)")

if __name__ == '__main__':
    unittest.main()
