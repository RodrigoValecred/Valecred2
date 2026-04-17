# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 📝 Scribe: Daily Documentation Sync [2025-04-17]
# 📑 Summary of Changes: Created ad-hoc report for calculating daily VOP records.
# 🛠️ Files Modified: NB_Gold_Record_VOP.Notebook/notebook-content.py
# 🚩 Pending Review: None

from pyspark.sql.functions import col, sum, desc

# Lê a tabela de operações na camada Gold
df_ops = spark.read.table("LH_Gold.fato_operacoes")

# Filtra apenas operações deferidas e agrupa por data de deferimento
df_vop_por_dia = (
    df_ops.filter(col("status_analise") == "D")
    .groupBy("data_deferimento")
    .agg(sum("valor_de_face").alias("vop_total"))
    .orderBy(desc("vop_total"))
)

# Mostra os top 10 dias com maior VOP
df_vop_por_dia.show(10)

# Para pegar exatamente o dia do recorde:
record = df_vop_por_dia.first()
print(f"O recorde de VOP foi no dia {record['data_deferimento']} com um total de {record['vop_total']}")
