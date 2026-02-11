import pytest
import os
import re
import unicodedata

def get_normalize_col():
    """
    Extracts the normalize_col function from notebook-content.py without
    executing the whole script, which would fail due to missing Spark context.
    """
    current_dir = os.path.dirname(__file__)
    filepath = os.path.join(current_dir, 'notebook-content.py')

    if not os.path.exists(filepath):
        # Fallback for different execution contexts
        filepath = 'VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Prepara_Tabela_Operacoes.Notebook/notebook-content.py'

    with open(filepath, 'r') as f:
        lines = f.readlines()

    func_lines = []
    in_func = False
    for line in lines:
        if line.startswith('def normalize_col('):
            in_func = True
            func_lines.append(line)
        elif in_func:
            # Function ends when a line is not indented and not empty
            if line.startswith('    ') or line.strip() == '':
                func_lines.append(line)
            else:
                break

    if not func_lines:
        raise Exception(f"Could not find normalize_col in {filepath}")

    func_code = "".join(func_lines)
    exec_globals = {'re': re, 'unicodedata': unicodedata}
    exec(func_code, exec_globals)
    return exec_globals['normalize_col']

normalize_col = get_normalize_col()

def test_normalize_col_unicode():
    assert normalize_col("Coração") == "coracao"
    assert normalize_col("Ação e Reação") == "acao_e_reacao"

def test_normalize_col_uppercase():
    assert normalize_col("COLUMNNAME") == "columnname"
    assert normalize_col("COLUMN_NAME") == "column_name"

def test_normalize_col_pascal_case():
    assert normalize_col("ColumnName") == "column_name"
    assert normalize_col("MyAwesomeColumn") == "my_awesome_column"

def test_normalize_col_camel_case():
    assert normalize_col("camelCase") == "camel_case"
    assert normalize_col("myVariable") == "my_variable"

def test_normalize_col_special_characters():
    assert normalize_col("Column-Name!") == "column_name"
    assert normalize_col("Column@Name#2024") == "column_name_2024"

def test_normalize_col_multiple_delimiters():
    assert normalize_col("Column   Name") == "column_name"
    assert normalize_col("Column__Name") == "column_name"
    assert normalize_col("Column - _ Name") == "column_name"

def test_normalize_col_strip():
    assert normalize_col("_Column_") == "column"
    assert normalize_col("  Column  ") == "column"
    assert normalize_col("---Column---") == "column"

def test_normalize_col_numbers():
    assert normalize_col("Column123") == "column123"
    assert normalize_col("123Column") == "123_column"
