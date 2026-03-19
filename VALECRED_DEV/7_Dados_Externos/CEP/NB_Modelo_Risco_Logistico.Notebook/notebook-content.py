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

# Criar tabela provisória para pontos de crise logística
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Lista de focos de crise (Exemplos reais de pontos críticos em greves)
data = [
    ("Bloqueio - Marginal Tietê (SP)", -23.518, -46.618, "Crítico"),
    ("Protesto - Rod. Fernão Dias (MG)", -19.951, -44.015, "Alto"),
    ("Greve - Porto de Santos (SP)", -23.951, -46.333, "Crítico"),
    ("Barricada - BR-116 (PR)", -25.428, -49.273, "Moderado")
]

schema = StructType([
    StructField("local_crise", StringType(), True),
    StructField("lat_crise", FloatType(), True),
    StructField("lon_crise", FloatType(), True),
    StructField("severidade", StringType(), True)
])

df_pontos_crise = spark.createDataFrame(data, schema)
df_pontos_crise.write.format("delta").mode("overwrite").saveAsTable("LH_Silver.pontos_crise")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Carregar as tabelas
df_titulos = spark.read.table("LH_Gold.fato_titulos")
df_crise = spark.read.table("LH_Silver.pontos_crise")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_titulos_limpos = df_titulos.filter(
    F.col("lat_sacado").isNotNull() &
    F.col("long_sacado").isNotNull()
)
df_crise_limpa = df_crise.filter(
    F.col("lat_crise").isNotNull() &
    F.col("lon_crise").isNotNull()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cruzamento (Cross Join para testar cada título contra cada bloqueio)
df_analise = df_titulos_limpos.crossJoin(df_crise_limpa) \
    .withColumn("distancia_km", haversine_udf("lat_sacado", "long_sacado", "lat_crise", "lon_crise"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filtrar apenas o que está a menos de 50km de um bloqueio (Sua Geofence)
df_war_room = df_analise.filter(F.col("distancia_km") <= 50) \
    .groupBy("cod_titulo", "valor", "cpf_cnpj_sacado", "distancia_km") \
    .agg(F.min("distancia_km").alias("distancia_minita"),
        F.first("severidade").alias("risco_logistico"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_war_room.write.format("delta").mode("overwrite").saveAsTable("LH_Gold.titulos_em_risco_logistico")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
