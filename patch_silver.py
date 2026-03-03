file_path = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Prepara_Tabela_Operacoes.Notebook/notebook-content.py"

with open(file_path, "r") as f:
    content = f.read()

content = content.replace('col("TARIFARECOMPRA").alias("tarifa_recompra")', 'col("TARIFARECOMPRA").alias("tarifa_recompra"),\n        col("FLOATING").alias("floating")')

with open(file_path, "w") as f:
    f.write(content)

print("Patched silver notebook.")
