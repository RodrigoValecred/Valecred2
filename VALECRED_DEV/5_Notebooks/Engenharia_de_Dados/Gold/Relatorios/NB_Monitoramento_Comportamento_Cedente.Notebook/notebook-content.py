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

# Fabric notebook source

import pyspark.sql.functions as F
from pyspark.sql.window import Window

print("Iniciando monitoramento diário da carteira do cedente...")

# 1. Carregar Dados
df_carteira = spark.table("LH_Gold.carteira_de_titulos")
df_ops = spark.table("LH_Gold.fato_operacoes")

# O Join para pegar o cod_cliente
df_base = df_carteira.join(df_ops.select("cod_operacao", "cod_cliente"), "cod_operacao", "inner")

# 2. Identificar Títulos Vencidos (usando a data atual)
df_risco = df_base.withColumn(
    "vencido",
    F.when(F.coalesce(F.col("venc_prorrogado"), F.col("vencimento")) < F.current_date(), 1).otherwise(0)
)

# 3. Calcular Risco Total e Vencido Atual
df_agregado_atual = df_risco.groupBy("cod_cliente").agg(
    F.sum("valor").alias("risco_total_aberto"),
    F.sum(F.when(F.col("vencido") == 1, F.col("valor")).otherwise(0)).alias("risco_total_vencido")
)

# 4. Calcular Risco Vencido de 5 dias atrás
# Comparamos o que estava vencido há 5 dias com a data de vencimento
df_risco_5_dias = df_base.withColumn(
    "vencido_5_dias",
    F.when(F.coalesce(F.col("venc_prorrogado"), F.col("vencimento")) < F.date_sub(F.current_date(), 5), 1).otherwise(0)
)

df_agregado_5_dias = df_risco_5_dias.groupBy("cod_cliente").agg(
    F.sum(F.when(F.col("vencido_5_dias") == 1, F.col("valor")).otherwise(0)).alias("risco_vencido_5_dias_atras")
)

# 5. Juntar e Analisar
df_final = df_agregado_atual.join(df_agregado_5_dias, "cod_cliente", "left")

df_final = df_final.withColumn(
    "houve_aumento",
    F.when(F.col("risco_total_vencido") > F.col("risco_vencido_5_dias_atras"), F.lit(True)).otherwise(F.lit(False))
)

# 6. Salvar
df_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Gold.Monitoramento_Comportamento_Cedente")
print("Processo concluído.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
