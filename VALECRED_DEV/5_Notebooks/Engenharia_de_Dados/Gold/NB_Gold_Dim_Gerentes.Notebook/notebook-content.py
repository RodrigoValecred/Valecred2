# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8f85c372-56ad-4f3f-acf9-3be2e9b99513",
# META       "default_lakehouse_name": "LH_Silver",
# META       "default_lakehouse_workspace_id": "41ae19db-f71d-471f-9ac7-ccbc2c75ce11",
# META       "known_lakehouses": [
# META         {
# META           "id": "8f85c372-56ad-4f3f-acf9-3be2e9b99513"
# META         },
# META         {
# META           "id": "ee40705b-0100-49bc-8f35-81d71839f042"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Notebook Gold: Dimensão Gerentes
# **Objetivo:** Construir a tabela `LH_Gold.dim_gerentes` a partir de tabelas Silver (`staging_gerentes`, `staging_usuarios`, etc.).
# 
# **Dependências:** `NB_Prepara_Tabela_Cadastros` (Silver).

# CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

from pyspark.sql.functions import col, regexp_replace, coalesce, lit
from delta.tables import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Iniciando construção da dim_gerentes...")

# Leitura das tabelas Silver
print("Lendo tabelas Silver...")
df_gerentes = spark.read.table("LH_Silver.staging_gerentes")
df_usuarios_staging = spark.read.table("LH_Silver.staging_usuarios")
df_geral_pf_pj_limpa = spark.read.table("LH_Silver.staging_cad_geral_pf_pj_limpa")
df_plataformas = spark.read.table("LH_Silver.staging_plataformas")

# Join com Usuarios (Prioridade 1)
print("Fazendo Join das tabelas...")
df_gerentes_with_users = df_gerentes.alias("g") \
    .join(df_usuarios_staging.alias("u"), col("g.cod_usuario") == col("u.cod_usuario"), "left")

# Join com Cadastro Geral (Prioridade 2 - Fallback via CPF/CNPJ)
# Tratamento de CPF/CNPJ para garantir match (apenas números)
df_gerentes_clean_cpf = df_gerentes_with_users \
    .withColumn("cpf_cnpj_clean", regexp_replace(col("g.cpf_cnpj"), "[^0-9]", ""))

df_geral_clean = df_geral_pf_pj_limpa.alias("cad") \
    .withColumn("cpf_cnpj_clean", regexp_replace(col("cad.cpf_cnpj"), "[^0-9]", ""))

df_gerentes_enriched = df_gerentes_clean_cpf \
    .join(df_geral_clean.select(col("cpf_cnpj_clean"), col("cad.nome").alias("nome_geral")), "cpf_cnpj_clean", "left")

df_dim_gerentes = df_gerentes_enriched \
    .join(df_plataformas, "cod_agencia", "left") \
    .select(
        col("g.cod_broker"),
        coalesce(col("u.nome"), col("nome_geral"), lit("GERENTE NÃO IDENTIFICADO")).alias("nome_gerente"),
        col("g.cpf_cnpj"),
        col("g.data_inicio_gestao"),
        col("g.data_contratacao"),
        col("g.tipo_gerente"),
        col("g.taxa_comissao"),
        col("g.meses_de_casa"),
        col("g.status_ativo"),
        col("cod_agencia"),
        col("nome_plataforma"),
        col("gestor_da_plataforma")
    )

output_path_dim_gerentes = "LH_Gold.dim_gerentes"
df_dim_gerentes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_dim_gerentes)
print(f"Tabela 'dim_gerentes' salva em: {output_path_dim_gerentes}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
