file_path_gold = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Curadoria_Gold.Notebook/notebook-content.py"
with open(file_path_gold, "r") as f:
    content_gold = f.read()

# Remove data_aceite
content_gold = content_gold.replace('        col("data_deferimento"),\n        col("data_aceite"),\n        col("floating"),', '        col("data_deferimento"),\n        col("floating"),')

with open(file_path_gold, "w") as f:
    f.write(content_gold)


file_path_report = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/Relatorios/NB_Gold_Relatorio_Produtos_Mensal.Notebook/notebook-content.py"
with open(file_path_report, "r") as f:
    content_report = f.read()

# Replace data_aceite with data_deferimento
content_report = content_report.replace(
    'df_titulos_dates = df_titulos.join(df_ops.select("cod_operacao", "data_aceite"), "cod_operacao", "inner") \\\n        .withColumn("prazo_original_dias", datediff(col("vencimento"), col("data_aceite")))',
    'df_titulos_dates = df_titulos.join(df_ops.select("cod_operacao", "data_deferimento"), "cod_operacao", "inner") \\\n        .withColumn("prazo_original_dias", datediff(col("vencimento"), col("data_deferimento")))'
)

with open(file_path_report, "w") as f:
    f.write(content_report)

print("Reverted PR.")
