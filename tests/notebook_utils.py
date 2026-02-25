import ast
import os
import textwrap

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

    # Pre-process source to remove magic commands that break AST parsing
    lines = source.splitlines()
    clean_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith('%') or stripped.startswith('!'):
            clean_lines.append(f"# {line}") # Comment out magic commands
        else:
            clean_lines.append(line)
    source = "\n".join(clean_lines)

    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        print(f"Syntax error parsing {found_path}: {e}")
        return None

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == function_name:
            if hasattr(ast, 'get_source_segment'):
                raw_source = ast.get_source_segment(source, node)
            else:
                lines = source.splitlines()
                start = node.lineno - 1
                end = node.end_lineno
                raw_source = "\n".join(lines[start:end])

            return textwrap.dedent(raw_source) if raw_source else None
    return None
