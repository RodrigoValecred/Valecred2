import ast
import os

def extract_function_from_file(filepath, function_name):
    """
    Extracts the source code of a function from a python file using AST.
    """
    # Check if file exists at path, or relative to potential test run locations
    candidates = [
        filepath,
        os.path.join("..", filepath),
        os.path.join(os.getcwd(), filepath),
    ]

    found_path = None
    for p in candidates:
        if os.path.exists(p):
            found_path = p
            break

    if not found_path:
        # Try to match the original logic more closely if simple checks fail
        # Original logic: if not exists, try ../filepath. If not exists, revert to filepath.
        # This is covered by candidates, but let's be verbose in logging if needed.
        print(f"File not found: {filepath}")
        return None

    with open(found_path, 'r', encoding='utf-8') as f:
        source = f.read()

    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        print(f"Syntax error parsing {found_path}: {e}")
        return None

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            if hasattr(ast, 'get_source_segment'):
                return ast.get_source_segment(source, node)
            else:
                lines = source.splitlines()
                start = node.lineno - 1
                end = node.end_lineno
                return "\n".join(lines[start:end])
    return None
