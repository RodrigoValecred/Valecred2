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
# META       "default_lakehouse_workspace_id": "41ae19db-f71d-471f-9ac7-ccbc2c75ce11",
# META       "known_lakehouses": [
# META         {
# META           "id": "553c2931-573b-4db0-838d-a70a01306d32"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# **Objetivo:** Criar a tabela `tbl_vadu_bronze` vazia com o schema correto para receber dados brutos.

# CELL ********************

from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType

# Define a estrutura da tabela
schema = StructType([
    StructField("Bordero_ID", LongType(), True),
    StructField("CNPJ_Sacado", StringType(), True),
    StructField("JSON_Bruto", StringType(), True),
    StructField("Data_Hora_Ingestao", TimestampType(), True)
])

# Cria um DataFrame vazio com esse esquema
df_empty = spark.createDataFrame([], schema)

# Grava como tabela Delta no Lakehouse
df_empty.write.format("delta").mode("overwrite").saveAsTable("tbl_vadu_bronze")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
