
import sys
import re

filepath = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Curadoria_Gold.Notebook/notebook-content.py"
with open(filepath, "r") as f:
    content = f.read()

# --- Fix Escrow Loading Logic in Célula 0.2 ---
# Original: df_escrow = spark.read.table("LH_Silver.staging_operacoes_escrow")
# New: df_escrow = spark.read.table("LH_Silver.staging_operacoes_escrow").groupBy("cod_operacao").agg(max("ESCROW").alias("ESCROW"))

# Need to check if 'max' is imported in Célula 0.1?
# Célula 0.1 imports: ..., sum, min, count, round, floor, least, current_date, split
# 'max' IS imported in Célula 0.1.

search_escrow_load = 'df_escrow = spark.read.table("LH_Silver.staging_operacoes_escrow")'
replace_escrow_load = 'df_escrow = spark.read.table("LH_Silver.staging_operacoes_escrow").groupBy("cod_operacao").agg(max("ESCROW").alias("ESCROW"))'

content = content.replace(search_escrow_load, replace_escrow_load)

# Write back
with open(filepath, "w") as f:
    f.write(content)
