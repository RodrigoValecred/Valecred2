from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, sum, datediff, current_date

# Initialize Spark
spark = SparkSession.builder \
    .appName("DiagnoseMissingClients") \
    .master("local[*]") \
    .config("spark.ui.enabled", "false") \
    .getOrCreate()

# Simulação de Dados
# Cliente 1: Tem títulos pagos (Deve estar no estudo)
# Cliente 2: Tem APENAS títulos em aberto (Novo cliente) -> Esperado estar AUSENTE com a lógica atual
# Cliente 3: Tem títulos pagos e abertos (Deve estar no estudo)

data_titulos = [
    # Client 1 (Paid)
    (1, "2023-01-01", "2023-01-10", "2023-01-05", 1000.0, 1000.0),
    # Client 2 (Only Open - No Liquidacao)
    (2, "2023-01-01", "2023-06-01", None, 5000.0, 0.0),
    # Client 3 (Mixed)
    (3, "2023-01-01", "2023-01-10", "2023-01-12", 2000.0, 2000.0),
    (3, "2023-02-01", "2023-06-01", None, 3000.0, 0.0),
]

columns = ["cod_cliente", "data_inclusao", "venc_prorrogado", "liquidacao", "valor_devido", "valor_pago"]

df_titulos = spark.createDataFrame(data_titulos, columns)

# Reprodução da Lógica Atual
print("--- Reproducing Logic ---")

# 1.1 Metrics (Paid)
df_pagos = df_titulos.filter(col("liquidacao").isNotNull()) \
    .withColumn("dias_atraso_real", datediff(col("liquidacao"), col("venc_prorrogado")))

df_metrics_pagos = df_pagos.groupBy("cod_cliente").agg(
    avg("dias_atraso_real").alias("media_atraso_historico")
)

# 1.2 Metrics (Risk - Open)
df_aberto = df_titulos.filter(col("liquidacao").isNull()) \
    .withColumn("dias_atraso_atual", datediff(current_date(), col("venc_prorrogado")))

df_metrics_risco = df_aberto.groupBy("cod_cliente").agg(
    sum(when(col("dias_atraso_atual") > 5, col("valor_devido")).otherwise(0)).alias("saldo_inadimplente_atual")
)

# Lógica do Join (A Suspeita)
print("Performing Left Join (Current Logic)...")
df_features = df_metrics_pagos \
    .join(df_metrics_risco, "cod_cliente", "left") \
    .na.fill(0)

df_features.show()

# Verifica se o Cliente 2 está ausente
clients_found = [row.cod_cliente for row in df_features.collect()]
print(f"Clients found: {clients_found}")
if 2 not in clients_found:
    print("CONFIRMED: Client 2 (Only Open Titles) is MISSING due to Left Join.")
else:
    print("Logic seems fine? (Unexpected)")

# Proposed Fix: Full Outer Join
print("\n--- Testing Fix (Full Outer Join) ---")
df_features_fixed = df_metrics_pagos \
    .join(df_metrics_risco, "cod_cliente", "full_outer") \
    .na.fill(0)

df_features_fixed.show()
clients_found_fixed = [row.cod_cliente for row in df_features_fixed.collect()]
print(f"Clients found with fix: {clients_found_fixed}")

if 2 in clients_found_fixed:
    print("SUCCESS: Client 2 is now included.")

spark.stop()
