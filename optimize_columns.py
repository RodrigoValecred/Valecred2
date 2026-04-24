import re

file_path = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Prepara_Tabela_Operacoes.Notebook/notebook-content.py"

with open(file_path, "r") as f:
    content = f.read()

# 1. df_dedup
search_dedup = """    # Garantir snake_case em todas as colunas e aplicar renomeações específicas
    return df_dedup.select([col(c).alias(rename_map.get(c.lower(), c.lower())) for c in df_dedup.columns])"""
replace_dedup = """    # ⚡ Bolt: Cachear colunas do DataFrame em uma tupla para evitar chamadas RPC iterativas e preservar ordem.
    df_dedup_cols = tuple(df_dedup.columns)
    # Garantir snake_case em todas as colunas e aplicar renomeações específicas
    return df_dedup.select([col(c).alias(rename_map.get(c.lower(), c.lower())) for c in df_dedup_cols])"""
content = content.replace(search_dedup, replace_dedup)

# 2. df_estudo
search_estudo = """    # Aplicar normalização
    new_cols = [col(c).alias(normalize_col(c)) for c in df_estudo.columns]"""
replace_estudo = """    # ⚡ Bolt: Cachear colunas do DataFrame em uma tupla para evitar chamadas RPC O(N) para o driver preservando ordem.
    df_estudo_cols = tuple(df_estudo.columns)
    # Aplicar normalização
    new_cols = [col(c).alias(normalize_col(c)) for c in df_estudo_cols]"""
content = content.replace(search_estudo, replace_estudo)

# 3. df_prorrogacao
search_prorro = """    df_prorrogacao_norm = df_prorrogacao.select(
        [col(c).alias(rename_map.get(c.lower(), c.lower())) for c in df_prorrogacao.columns]
    )"""
replace_prorro = """    # ⚡ Bolt: Fazer cache de df.columns em uma tupla local para evitar chamadas RPC iterativas e preservar ordem.
    df_prorrogacao_cols = tuple(df_prorrogacao.columns)
    df_prorrogacao_norm = df_prorrogacao.select(
        [col(c).alias(rename_map.get(c.lower(), c.lower())) for c in df_prorrogacao_cols]
    )"""
content = content.replace(search_prorro, replace_prorro)


with open(file_path, "w") as f:
    f.write(content)

print("Optimized!")
