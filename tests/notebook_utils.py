import ast
import os
import textwrap

def extract_function_from_file(filepath, function_name):
    """
    Extracts the source code of a function from a python file using AST.
    """
    # Verifica se o arquivo existe no caminho, ou em relação aos possíveis locais de execução do teste
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
        # Tenta corresponder à lógica original mais de perto se verificações simples falharem
        # Lógica original: se não existir, tenta ../filepath. Se não existir, reverte para filepath.
        # Isso é coberto por candidatos, mas vamos ser verbosos no log se necessário.
        print(f"File not found: {filepath}")
        return None

    with open(found_path, 'r', encoding='utf-8') as f:
        source = f.read()

    # Pré-processa a fonte para remover comandos mágicos que quebram a análise AST
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
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            if hasattr(ast, 'get_source_segment'):
                raw_source = ast.get_source_segment(source, node)
            else:
                lines = source.splitlines()
                start = node.lineno - 1
                end = node.end_lineno
                raw_source = "\n".join(lines[start:end])

            return textwrap.dedent(raw_source) if raw_source else None
    return None
