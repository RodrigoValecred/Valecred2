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
# META           "id": "ee40705b-0100-49bc-8f35-81d71839f042"
# META         },
# META         {
# META           "id": "8f85c372-56ad-4f3f-acf9-3be2e9b99513"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
import math

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Função Haversine para calcular distância em KM
def haversine(lat1, lon1, lat2, lon2):
    R = 6371 # Raio da Terra em KM
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Registrar a função para usar no Spark
haversine_udf = F.udf(haversine, FloatType())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Supondo que você tenha uma tabela 'pontos_crise' com lat/long dos bloqueios
# Carregar as tabelas
df_titulos = spark.read.table("LH_Silver.staging_titulos_limpa")
df_ceps = spark.read.table("LH_Bronze.cep_coordenadas")
# df_crise = spark.read.table("silver_fidc.tbl_pontos_bloqueio")

# Limpeza rápida (remover traços de CEP se houver: 01001-000 -> 01001000)
from pyspark.sql.functions import regexp_replace, col

df_titulos = df_titulos.withColumn("cep_limpo", regexp_replace(col("cep_sacado"), "[^0-9]", ""))
df_ceps = df_ceps \
    .withColumn("cep_inicial_limpo", regexp_replace(col("cep_inicial"), "[^0-9]", "")) \
    .withColumn("cep_final_limpo", regexp_replace(col("cep_final"), "[^0-9]", ""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cruzamento (Cross Join para testar cada título contra cada bloqueio)
df_analise = df_titulos.crossJoin(df_crise) \
    .withColumn("distancia_km", haversine_udf("lat_sacado", "long_sacado", "lat_bloqueio", "long_bloqueio"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filtrar apenas o que está a menos de 50km de um bloqueio (Sua Geofence)
df_risco = df_analise.filter(F.col("distancia_km") <= 50) \
    .select("id_titulo", "valor", "sacado", "distancia_km").distinct()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_risco.write.format("delta").mode("overwrite").saveAsTable("gold_fidc.titulos_em_risco")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
