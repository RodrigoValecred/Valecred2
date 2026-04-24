import re

file_path = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Curadoria_Gold.Notebook/notebook-content.py"

with open(file_path, "r") as f:
    content = f.read()

# 1. df_dropped
search = """new_columns = [renames.get(c, c) for c in df_dropped.columns]"""
replace = """# ⚡ Bolt: Cachear colunas do DataFrame em uma tupla para evitar chamadas RPC O(N) para o driver preservando ordem.
df_dropped_cols = tuple(df_dropped.columns)
new_columns = [renames.get(c, c) for c in df_dropped_cols]"""
content = content.replace(search, replace)

with open(file_path, "w") as f:
    f.write(content)

print("Optimized NB_Curadoria_Gold.Notebook!")
