import ast
import os
import unittest

class TestCVMSecurity(unittest.TestCase):
    def test_requests_timeout(self):
        # Locate the notebook file
        # Assuming we run from repo root
        filepath = "VALECRED_DEV/7_Dados_Externos/CVM/NB_Load_From_CVM.Notebook/notebook-content.py"

        if not os.path.exists(filepath):
            self.fail(f"Notebook file not found at {filepath}")

        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()

        # Clean magic commands (though not expected in .py content of fabric notebooks usually, but safe to have)
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

            # Check for timeout
            if 'timeout' not in keywords:
                self.fail(f"requests.{func_name} call at line {call.lineno} missing 'timeout' argument (DoS risk)")

    def test_requests_stream(self):
        # Locate the notebook file
        filepath = "VALECRED_DEV/7_Dados_Externos/CVM/NB_Load_From_CVM.Notebook/notebook-content.py"

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
                # Check for requests.get (HEAD doesn't download content, so stream is irrelevant/implicit)
                is_requests_call = False
                if isinstance(node.func, ast.Attribute) and node.func.attr == 'get':
                     if isinstance(node.func.value, ast.Name) and node.func.value.id == 'requests':
                        is_requests_call = True

                if is_requests_call:
                    requests_calls.append(node)

        # Check if we found any calls
        self.assertTrue(len(requests_calls) > 0, "No requests.get calls found in the notebook")

        for call in requests_calls:
            keywords = {kw.arg: kw.value for kw in call.keywords}
            func_name = call.func.attr

            # Check for stream=True
            if 'stream' not in keywords:
                self.fail(f"requests.{func_name} call at line {call.lineno} missing 'stream=True' argument (Memory Exhaustion risk)")

            # Check if stream value is explicitly True
            stream_val = keywords['stream']
            if not (isinstance(stream_val, ast.Constant) and stream_val.value is True):
                 self.fail(f"requests.{func_name} call at line {call.lineno} has 'stream' argument but it is not set to True")

if __name__ == '__main__':
    unittest.main()
