import unittest
import ast
import os

class TestCVMSecurity(unittest.TestCase):
    def test_requests_timeout_and_stream(self):
        filepath = "VALECRED_DEV/7_Dados_Externos/CVM/NB_Load_From_CVM.Notebook/notebook-content.py"
        with open(filepath, 'r', encoding='utf-8') as f:
            source = f.read()

        # Clean magic commands
        lines = source.splitlines()
        clean_lines = []
        for line in lines:
            if line.strip().startswith('%') or line.strip().startswith('!'):
                clean_lines.append(f"# {line}")
            else:
                clean_lines.append(line)
        source = "\n".join(clean_lines)

        tree = ast.parse(source)

        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func_name = ""
                if isinstance(node.func, ast.Attribute):
                    # Handle requests.get, requests.head
                    if isinstance(node.func.value, ast.Name) and node.func.value.id == "requests":
                        func_name = node.func.attr

                if func_name in ["get", "head", "post", "put", "delete"]:
                    # Check for timeout
                    has_timeout = False
                    for keyword in node.keywords:
                        if keyword.arg == "timeout":
                            has_timeout = True
                            break

                    self.assertTrue(has_timeout, f"requests.{func_name} call at line {node.lineno} missing 'timeout' argument")

                    # Check for stream=True in requests.get
                    if func_name == "get":
                        has_stream = False
                        for keyword in node.keywords:
                            if keyword.arg == "stream" and isinstance(keyword.value, ast.Constant) and keyword.value.value is True:
                                has_stream = True
                                break
                        # We only mandate stream=True for the download part, but since we are checking all calls,
                        # let's be strict or context aware.
                        # In this file, there is only one requests.get and it is for download.
                        self.assertTrue(has_stream, f"requests.get call at line {node.lineno} should have stream=True")

if __name__ == '__main__':
    unittest.main()
