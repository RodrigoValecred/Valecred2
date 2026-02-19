import ast
import os
import unittest

class TestBrasilIOSecurity(unittest.TestCase):
    def test_requests_timeout_and_headers(self):
        filepath = "VALECRED_DEV/7_Dados_Externos/NB_Load_Bronze_From_BrasilIO.Notebook/notebook-content.py"
        if not os.path.exists(filepath):
            # Try finding it relative to test dir
            filepath = os.path.join("..", filepath)

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

        requests_get_calls = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                # Check for requests.get
                is_requests_get = False
                if isinstance(node.func, ast.Attribute) and node.func.attr == 'get':
                     if isinstance(node.func.value, ast.Name) and node.func.value.id == 'requests':
                        is_requests_get = True

                if is_requests_get:
                    requests_get_calls.append(node)

        self.assertTrue(len(requests_get_calls) > 0, "No requests.get calls found in the notebook")

        for call in requests_get_calls:
            keywords = {kw.arg: kw.value for kw in call.keywords}

            # Check for timeout
            self.assertIn('timeout', keywords, "requests.get call missing 'timeout' argument (DoS risk)")

            # Check for headers (optional but recommended)
            # We look for 'headers' keyword argument
            # self.assertIn('headers', keywords, "requests.get call missing 'headers' argument (User-Agent recommended)")
            # I will enforce headers too as planned
            self.assertIn('headers', keywords, "requests.get call missing 'headers' argument (User-Agent recommended)")

if __name__ == '__main__':
    unittest.main()
