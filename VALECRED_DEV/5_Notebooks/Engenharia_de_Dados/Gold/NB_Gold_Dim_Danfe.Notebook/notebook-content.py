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

# # Notebook de Construção da Dimensão Danfe (Gold)
# **Objetivo:** Criar a tabela `LH_Gold.dim_danfe` a partir das chaves DANFE presentes nos títulos.
# **Origem:** `LH_Silver.staging_titulos_limpa`.

# CELL ********************

# Configuração e Imports
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

from pyspark.sql.functions import col, length, regexp_replace, substring

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Iniciando construção da dim_danfe...")

# 1. Leitura dos dados
df_titulos = spark.read.table("LH_Silver.staging_titulos_limpa")

# 2. Seleção e Limpeza
# Filtrar chaves válidas (comprimento 44) e remover lixo
# A coluna na silver já deve estar snake_case 'chave_danfe' (verificado em NB_Prepara_Tabela_Titulos)
df_chave_filtrada = df_titulos \
    .select(col("chave_danfe").alias("CHAVEDANFE")) \
    .na.drop(subset=["CHAVEDANFE"]) \
    .filter((col("CHAVEDANFE") != "") & (length(col("CHAVEDANFE")) == 44)) \
    .filter(~col("CHAVEDANFE").contains("XML NF-E")) \
    .withColumn("CHAVEDANFE", regexp_replace(col("CHAVEDANFE"), " ", "0")) \
    .distinct()

# 3. Extração dos componentes (Parsing)
# Substring em Spark é 1-based.
df_detalhada = df_chave_filtrada \
    .withColumn("uf", substring(col("CHAVEDANFE"), 1, 2)) \
    .withColumn("aamm", substring(col("CHAVEDANFE"), 3, 4)) \
    .withColumn("cnpj", substring(col("CHAVEDANFE"), 7, 14)) \
    .withColumn("modelo", substring(col("CHAVEDANFE"), 21, 2)) \
    .withColumn("serie", substring(col("CHAVEDANFE"), 23, 3)) \
    .withColumn("numero_nf", substring(col("CHAVEDANFE"), 26, 9)) \
    .withColumn("codigo_nf", substring(col("CHAVEDANFE"), 35, 9)) \
    .withColumn("dv", substring(col("CHAVEDANFE"), 44, 1)) \
    .withColumnRenamed("CHAVEDANFE", "chave_danfe")

# 4. Escrita
output_path = "LH_Gold.dim_danfe"
df_detalhada.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path)
print(f"Tabela '{output_path}' criada com sucesso.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
