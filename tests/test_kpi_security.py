import ast
import os
import unittest

class TestKPISecurity(unittest.TestCase):
    def test_no_hardcoded_passwords(self):
        filepath = "VALECRED_DEV/8_RealTime/KPI_DA_TV.Notebook/notebook-content.py"
        if not os.path.exists(filepath):
            # Try finding it relative to test dir if running from root
            filepath = os.path.join(os.getcwd(), filepath)

        if not os.path.exists(filepath):
             self.fail(f"Notebook file not found at {filepath}")

        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()

        # Clean magic commands if any (though this file looks clean)
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
                # Check for .option() calls
                if isinstance(node.func, ast.Attribute) and node.func.attr == 'option':
                    option_calls.append(node)

        found_password_option = False
        for call in option_calls:
            # Check args
            if len(call.args) >= 2:
                key_arg = call.args[0]
                value_arg = call.args[1]

                # Check if first arg is "password" literal
                if isinstance(key_arg, ast.Constant) and key_arg.value == "password":
                    found_password_option = True
                    # Assert second arg is NOT a string literal
                    if isinstance(value_arg, ast.Constant) and isinstance(value_arg.value, str):
                        self.fail(f"CRITICAL: Hardcoded password detected! Found: .option('password', '{value_arg.value}')")

            # Check kwargs (unlikely for spark .option but possible in python)
            # Spark .option(key, value) usually positional

        # Ensure we actually found the option call (so the test isn't passing falsely because it missed the code)
        self.assertTrue(found_password_option, "Did not find any .option('password', ...) call. Code structure might have changed.")

if __name__ == '__main__':
    unittest.main()
