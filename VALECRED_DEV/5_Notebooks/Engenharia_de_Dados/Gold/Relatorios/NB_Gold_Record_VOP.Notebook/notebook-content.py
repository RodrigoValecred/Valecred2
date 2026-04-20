# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ee40705b-0100-49bc-8f35-81d71839f042",
# META       "default_lakehouse_name": "LH_Gold",
# META       "default_lakehouse_workspace_id": "41ae19db-f71d-471f-9ac7-ccbc2c75ce11",
# META       "known_lakehouses": [
# META         {
# META           "id": "ee40705b-0100-49bc-8f35-81d71839f042"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# 📝 Scribe: Daily Documentation Sync [2025-04-17]
# 📑 Summary of Changes: Updated ad-hoc report to break down VOP by document type (t_doc) and product (tto).
# 🛠️ Files Modified: NB_Gold_Record_VOP.Notebook/notebook-content.py
# 🚩 Pending Review: None

from pyspark.sql.functions import col, sum, desc, round

# Lê as tabelas na camada Gold
df_ops = spark.read.table("LH_Gold.fato_operacoes")
df_titulos = spark.read.table("LH_Gold.fato_titulos")

# Filtra apenas operações deferidas
df_ops_validas = df_ops.filter(col("status_analise") == "D")

# 1. Total Geral por Dia para encontrar os Top Dias
df_vop_total = (
    df_ops_validas
    .groupBy("data_deferimento")
    .agg(round(sum("valor_de_face"), 2).alias("vop_total"))
    .orderBy(desc("vop_total"))
)

print("Top 10 Dias de VOP (Geral):")
df_vop_total.show(10)

# Extrai os Top 10 dias para focar a análise
top_dias = [row["data_deferimento"] for row in df_vop_total.limit(10).collect()]

df_ops_top_dias = df_ops_validas.filter(col("data_deferimento").isin(top_dias))

# 2. Breakdown por Produto (TTO) usando valor_de_face da operacao
df_vop_por_tto = (
    df_ops_top_dias
    .groupBy("data_deferimento")
    .pivot("tto")
    .agg(round(sum("valor_de_face"), 2))
    .fillna(0)
    .orderBy(desc("data_deferimento"))
)

print("\nBreakdown de VOP por Produto (TTO) nos Top Dias:")
df_vop_por_tto.show(10)

# 3. Breakdown por Tipo de Documento (t_doc) usando valor do titulo
# Faz o join com a tabela de títulos apenas para as operações válidas dos top dias
df_join_titulos = df_titulos.join(
    df_ops_top_dias.select("cod_operacao", "data_deferimento"),
    "cod_operacao",
    "inner"
)

df_vop_por_tdoc = (
    df_join_titulos
    .groupBy("data_deferimento")
    .pivot("t_doc")
    .agg(round(sum("valor"), 2))
    .fillna(0)
    .orderBy(desc("data_deferimento"))
)

print("\nBreakdown de VOP por Tipo de Documento (t_doc) nos Top Dias:")
df_vop_por_tdoc.show(10)

# Para pegar exatamente o dia do recorde:
record = df_vop_total.first()
print(f"\nO recorde absoluto de VOP foi no dia {record['data_deferimento']} com um total de {record['vop_total']}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
