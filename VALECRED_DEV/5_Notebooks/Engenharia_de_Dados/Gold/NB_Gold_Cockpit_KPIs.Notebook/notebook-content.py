# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "553c2931-573b-4db0-838d-a70a01306d32",
# META       "default_lakehouse_name": "LH_Bronze",
# META       "default_lakehouse_workspace_id": "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Notebook Gold Cockpit KPIs
# **Objetivo:** Criar tabela Gold_Cockpit_KPIs com agregação de KPIs de risco e inadimplência.
# **Origem:** Tabelas Silver (Titulos, Operacoes, Clientes).

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, lit, datediff, current_date, coalesce

# Leitura das tabelas Silver (já limpas) do OneLake
# Adaptado para os nomes reais das tabelas Silver
df_titulos = spark.read.table("LH_Silver.staging_titulos_limpa")
df_operacoes = spark.read.table("LH_Silver.staging_operacoes_limpa")
df_clientes = spark.read.table("LH_Silver.staging_clientes_limpa")

print("Tabelas carregadas.")

# CELL ********************

# Enriquecimento e Calculo de Métricas

# 1. Join Titulos com Operacoes para obter cod_cliente
# staging_titulos_limpa tem 'cod_operacao'. staging_operacoes_limpa tem 'cod_operacao' e 'cod_cliente'.
df_titulos_cliente = df_titulos.join(
    df_operacoes.select("cod_operacao", "cod_cliente"),
    "cod_operacao",
    "left"
)

# 2. Calcular Dias de Atraso e Status
# Se 'valor_devido' não existir, calculamos como (valor - amortizacoes)
# Se 'dias_atraso' não existir, calculamos via datediff(current_date, venc_prorrogado)

if "valor_devido" not in df_titulos_cliente.columns:
    df_titulos_cliente = df_titulos_cliente.withColumn(
        "valor_devido",
        col("valor") - coalesce(col("amortizacoes"), lit(0))
    )

df_prep = df_titulos_cliente \
    .withColumn("dias_atraso", datediff(current_date(), col("venc_prorrogado"))) \
    .withColumn("status_open", when(col("liquidacao").isNull(), lit("Aberto")).otherwise(lit("Fechado")))

# 3. Agregação por Cliente (KPIs)
# Regra: Inadimplência > 14 dias de atraso e Status Aberto

df_risco = df_prep.groupBy("cod_cliente").agg(
    F.sum(
        F.when(
            (F.col("dias_atraso") > 14) & (F.col("status_open") == "Aberto"),
            F.col("valor_devido")
        ).otherwise(0)
    ).alias("Valor_Inadimplente_14d"),

    F.sum(
        F.when(
            F.col("status_open") == "Aberto",
            F.col("valor_devido")
        ).otherwise(0)
    ).alias("Carteira_Total")
).withColumn(
    "Percentual_Inadimplencia",
    F.col("Valor_Inadimplente_14d") / F.col("Carteira_Total")
)

# CELL ********************

# Salvando na Camada Gold
table_name = "LH_Gold.Gold_Cockpit_KPIs"
df_risco.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"Tabela {table_name} criada com sucesso.")
