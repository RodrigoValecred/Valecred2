file_path = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Curadoria_Gold.Notebook/notebook-content.py"

with open(file_path, "r") as f:
    content = f.read()

# I want to add `col("data_aceite"),` and `col("floating")` inside `select_fato_operacoes_columns`
replacement_str = """        col("taxa").alias("taxa_operacao"),
        col("era"),
        col("data_deferimento"),
        col("data_aceite"),
        col("floating"),
        col("chave_base_cliente"),"""

content = content.replace('        col("taxa").alias("taxa_operacao"),\n        col("era"),\n        col("data_deferimento"),\n        col("chave_base_cliente"),', replacement_str)

with open(file_path, "w") as f:
    f.write(content)

print("Patched gold notebook.")
