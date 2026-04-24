import re

file_path = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/Relatorios/NB_Risk_Aggregation.Notebook/notebook-content.py"

with open(file_path, "r") as f:
    content = f.read()

# 1. df_operacoes
search_ops = """new_cols_operacoes = [cols_operacoes.get(c, c) for c in df_operacoes.columns]"""
replace_ops = """# ⚡ Bolt: Cachear colunas do DataFrame em uma tupla para evitar chamadas RPC O(N) para o driver preservando ordem posicional.
df_operacoes_cols = tuple(df_operacoes.columns)
new_cols_operacoes = [cols_operacoes.get(c, c) for c in df_operacoes_cols]"""
content = content.replace(search_ops, replace_ops)

# 2. df_cedentes
search_cedentes = """new_cols_cedentes = [cols_cedentes.get(c, c) for c in df_cedentes.columns]"""
replace_cedentes = """# ⚡ Bolt: Cachear colunas do DataFrame em uma tupla para evitar chamadas RPC O(N) para o driver preservando ordem posicional.
df_cedentes_cols = tuple(df_cedentes.columns)
new_cols_cedentes = [cols_cedentes.get(c, c) for c in df_cedentes_cols]"""
content = content.replace(search_cedentes, replace_cedentes)

with open(file_path, "w") as f:
    f.write(content)

print("Optimized NB_Risk_Aggregation.Notebook!")
