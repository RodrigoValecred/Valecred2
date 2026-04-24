import re

file_path = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Generic_Silver.Notebook/notebook-content.py"

with open(file_path, "r") as f:
    content = f.read()

search = """        # Aplica a função mestra em todas as colunas
        new_cols = [self._clean_name(c) for c in self.df_source.columns]"""
replace = """        # ⚡ Bolt: Feito o cache de `df.columns` em uma tupla ou lista local para evitar múltiplas chamadas RPC ao driver. A ordem original (dada por df.columns) é preservada perfeitamente na tupla.
        df_cols = tuple(self.df_source.columns)
        # Aplica a função mestra em todas as colunas
        new_cols = [self._clean_name(c) for c in df_cols]"""

content = content.replace(search, replace)

with open(file_path, "w") as f:
    f.write(content)

print("Optimized!")
