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
# META         },
# META         {
# META           "id": "553c2931-573b-4db0-838d-a70a01306d32"
# META         },
# META         {
# META           "id": "8f85c372-56ad-4f3f-acf9-3be2e9b99513"
# META         }
# META       ]
# META     }
# META   }
# META }


# MARKDOWN ********************

# # Notebook NB_Gold_Relatorio_Saidas_Risco
# **Objetivo:** Gerar o relatório de saídas em risco (operações não liquidadas em atraso) cruzando informações da carteira de crédito na camada Gold.

# MARKDOWN ********************

# # Relatório de Saídas de Risco
# Identifica clientes marcados como SAÍDA DE RISCO, com CNPJ, data do status e o parecer.

# CELL ********************

from pyspark.sql.functions import col

# Ler as tabelas necessarias
df_pareceres_status = spark.read.table("LH_Silver.pareceres_de_alteracao_de_status")
df_clientes = spark.read.table("LH_Silver.staging_clientes_limpa")
df_pareceres_bronze = spark.read.table("LH_Bronze.cad_geral_pareceres")

# Filtrar status de SAÍDA DE RISCO
df_saidas = df_pareceres_status.filter(
    col("STATUS_DO_CLIENTE").ilike("%SAIDA DE RISCO%") | 
    col("STATUS_DO_CLIENTE").ilike("%SAÍDA DE RISCO%")
)

# Obter o CNPJ (de staging_clientes_limpa)
df_clientes_dedup = df_clientes.dropDuplicates(["cod_cliente"])
df_saidas_cnpjs = df_saidas.join(
    df_clientes_dedup.select("cod_cliente", "cpf_cnpj"),
    df_saidas.CODCLIENTE == df_clientes_dedup.cod_cliente,
    "left"
)

# Buscar o parecer original para analise
df_pareceres_detalhe = df_pareceres_bronze.select(
    col("CODPARECER"), 
    col("OBS").alias("PARECER_COMPLETO")
)

df_relatorio = df_saidas_cnpjs.join(
    df_pareceres_detalhe,
    on="CODPARECER",
    how="left"
).select(
    "CODCLIENTE",
    "cpf_cnpj",
    "STATUS_DO_CLIENTE",
    "DATALOG",
    "PARECER_COMPLETO",
    "USUARIO"
)

table_name = "LH_Gold.relatorio_saidas_risco"
df_relatorio.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(table_name)
print(f"Relatório {table_name} gerado com sucesso!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
