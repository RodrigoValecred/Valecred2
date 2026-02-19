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

# # Notebook de Construção da Dimensão Empresas (Gold)
# **Objetivo:** Criar a tabela `LH_Gold.dim_empresas` a partir de `staging_empresas` e dados cadastrais, aplicando regras de negócio específicas para derivação de nomes.

# CELL ********************

# Configuração e Imports
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

from pyspark.sql.functions import col, lit, concat, udf, regexp_replace, when
from pyspark.sql.types import StringType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# A lógica antiga de derivação de nomes foi substituída por um join com a tabela `LH_Silver.sup_apelido_empresas`.
# Consulte o histórico para ver a lógica legada se necessário.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Iniciando construção da dim_empresas...")

# 1. Leitura dos dados
df_empresas = spark.read.table("LH_Silver.staging_empresas")

# Explicit Safety Filter (Garante apenas IDs desejados mesmo se staging tiver mais)
df_empresas = df_empresas.filter(col("cod_empresa").isin([6, 14, 24, 25]))

df_cadastros = spark.read.table("LH_Silver.staging_cad_geral_pf_pj_limpa")
df_apelidos = spark.read.table("LH_Silver.sup_apelido_empresas")

# 2. Preparação e Join
# Limpar CNPJ da tabela de empresas para garantir match numérico
df_empresas_clean = df_empresas.withColumn("cnpj_clean", regexp_replace(col("cnpj"), "[^0-9]", ""))

# Join with aliases to avoid ambiguity on 'nome' column
df_joined = df_empresas_clean.alias("e").join(
    df_cadastros.alias("c"),
    col("e.cnpj_clean") == col("c.cpf_cnpj"),
    "left"
)

# Join com tabela de apelidos para substituir a lógica complexa de derivação
df_joined_final = df_joined.join(
    df_apelidos.alias("a"),
    col("c.nome") == col("a.nome"),
    "left"
)

# 3. Transformações
df_final = df_joined_final \
    .withColumn("base", lit(40)) \
    .withColumn("chave_base_empresa", concat(col("base").cast("string"), lit("-"), col("e.cod_empresa").cast("string"))) \
    .withColumn("chave_base_cadastro", concat(col("base").cast("string"), lit("-"), col("e.cnpj_clean"))) \
    .withColumn("TIPO", when(col("chave_base_empresa") == "40-14", "SECURITIZADORA").otherwise("FIDC")) \
    .select(
        col("base"),
        col("chave_base_empresa"),
        col("chave_base_cadastro"),
        col("e.cnpj"),
        col("e.cod_empresa"),
        col("c.nome").alias("nome_original"),
        col("a.apelido").alias("empresa"),
        col("TIPO")
    )

# 4. Escrita
output_path = "LH_Gold.dim_empresas"
df_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path)
print(f"Tabela '{output_path}' criada com sucesso.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
