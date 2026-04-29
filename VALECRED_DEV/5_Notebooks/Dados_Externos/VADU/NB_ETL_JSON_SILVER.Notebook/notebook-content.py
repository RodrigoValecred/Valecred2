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
# META         },
# META         {
# META           "id": "8f85c372-56ad-4f3f-acf9-3be2e9b99513"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS lh_bronze.tbl_vadu_silver;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import get_json_object, col, when

df_bronze = spark.read.table("tbl_vadu_bronze")

df_silver = df_bronze.select(
    col("Bordero_ID"),
    col("CNPJ_Sacado"),
    # Dados Gerais
    get_json_object(col("JSON_Bruto"), "$.sacNome").alias("Nome_Empresa"),
    get_json_object(col("JSON_Bruto"), "$.sacValorTitulos").cast("double").alias("Valor_Operacao"),
    
    # Inadimplências (Exemplos de Flags)
    when(col("JSON_Bruto").contains("Falencia Decretada"), True).otherwise(False).alias("Flag_Falencia"),
    when(col("JSON_Bruto").contains("Recuperação Judicial"), True).otherwise(False).alias("Flag_Recuperacao_Judicial"),
    
    # Visão Cedente
    when(get_json_object(col("JSON_Bruto"), "$.logs[*].saclogCondicao").contains("possuiVisaoCedente == true"), 1)
    .otherwise(0).alias("Tem_Visao_Cedente"),
    
    col("Data_Hora_Ingestao")
)

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("tbl_vadu_silver")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, explode, from_json
from pyspark.sql.types import ArrayType, StringType

# 1. Definimos que 'logs' é uma lista (Array) de textos (JSONs individuais)
schema_logs = ArrayType(StringType())

# 2. Transformamos a String em Array e depois explodimos
df_logs = df_bronze.select(
    col("Bordero_ID"),
    col("CNPJ_Sacado"),
    explode(
        from_json(get_json_object(col("JSON_Bruto"), "$.logs"), schema_logs)
    ).alias("log_individual_json")
)

display(df_logs)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
