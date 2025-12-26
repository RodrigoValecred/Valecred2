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
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Notebook de Preparação Gold - Dim_Clientes

# MARKDOWN ********************

# ## Seção 0: Configuração do ambiente

# CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

from pyspark.sql.functions import col

source_lakehouse = "LH_Silver"
target_lakehouse = "LH_Gold"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 1: Dim_Clientes

# CELL ********************

#1.1 Carga dos clientes Silver
print("Carregando dados dos clientes Lakehouse Silver")
df_silver_clientes = spark.read.table(f"{source_lakehouse}.staging_clientes_limpa")
df_silver_clientes.show(5)

#1.2 Carga dos cadastros Silver
print("Carregando dados dos cadastros do Lakehouse Silver")
df_silver_cadastros = spark.read.table(f"{source_lakehouse}.staging_cad_geral_pf_pj_limpa")
df_silver_cadastros.show(5)

#1.3 Carga do Excel de Grupos Economicos
print("Carregando dados dos grupos economicos")
df_silver_grupos = spark.read.table(f"{source_lakehouse}.sup_grupos_economicos")
df_silver_grupos.show(5)
df_silver_grupos_renomeados = df_silver_grupos \
    .withColumnRenamed("codcliente","cod_cliente") \
    .withColumnRenamed("nomegrupo", "grupo_economico") \
    .select(
        col("cod_cliente"),
        col("grupo_economico")
    )
df_silver_grupos_renomeados.show(5)

#1.4 Realizando o Join
df_dim_clientes_final = df_silver_clientes.join(
    df_silver_cadastros,
    on="cpf_cnpj",
    how="left"
)
df_dim_clientes_final.show(5)

df_clientes_com_grupo = df_dim_clientes_final.join(
    df_silver_grupos_renomeados,
    on="cod_cliente",
    how="left"
)
df_clientes_com_grupo.show(5)

df_clientes_com_grupo.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Gold.dim_clientes")
print(f"Tabela 'fato_titulos' salva em: LH_Gold.dim_clientes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 2: Dim_Grupos_Economicos

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("Carregando dados dos contratos...")
df_contratos = spark.read.table(f"{source_lakehouse}.staging_contratos_clientes_limpa")
df_contratos.show(5)
print(f"Total CLientes Únicos: {df_contratos.count()}")

w_mais_recente = Window.partitionBy("cod_cliente").orderBy(F.col("dt_ini_contrato").desc())

df_limites_ativos = df_contratos.filter(F.col("status") == 'A') 

df_limites_calculados = df_limites_ativos.withColumn(
    "limite_total_cliente",
    F.coalesce(F.col("limite_fomento"),F.lit(0)) +
    F.coalesce(F.col("limite_comissaria"), F.lit(0))
)

df_final_contratos = df_limites_calculados.select(
    "cod_cliente",
    "cod_contrato",
    "validade_limite",
    "limite_total_cliente"
)

print(f"Total CLientes Únicos após filtragem: {df_final_contratos.count()}")
df_final_contratos.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
