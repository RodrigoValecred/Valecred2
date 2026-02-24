# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "553c2931-573b-4db0-838d-a70a01306d32",
# META       "default_lakehouse_name": "LH_Gold",
# META       "default_lakehouse_workspace_id": "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Análise de Cluster de Clientes (Risco e Comportamento)
# **Objetivo:** Segmentar a base de clientes em 3 grupos comportamentais (RFM e Behavioral Scoring):
# 1. **Prime (Estável)**: Pagam em dia e possuem consistência.
# 2. **Rentável (Atraso Moderado)**: Pagam com atraso, gerando receita de juros, mas sem risco crítico.
# 3. **Alerta (Risco)**: Atrasos crescentes, alta volatilidade e inadimplência atual.

# CELL ********************

from pyspark.sql.functions import col, datediff, avg, sum, count, max, current_date, when, lit, desc, min, create_map, stddev, coalesce, abs
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql.types import DoubleType
from itertools import chain

# Configurações
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")

# Ler Tabelas Gold
print("Carregando tabelas Gold...")
try:
    df_titulos = spark.read.table("LH_Gold.fato_titulos")
    df_clientes = spark.read.table("LH_Gold.dim_clientes")
except Exception as e:
    print(f"Erro ao carregar tabelas: {e}")
    # Fallback ou Exit se necessário
    raise e

# CELL ********************

# ## 1. Feature Engineering
# Calcular métricas comportamentais por cliente (RFM + Latency + Volatility).

print("Calculando métricas por cliente...")

# 1.1 Métricas de Histórico de Pagamento (Títulos Liquidados)
# Filtramos apenas títulos liquidados para analisar comportamento histórico
df_pagos = df_titulos.filter(col("liquidacao").isNotNull()) \
    .withColumn("dias_atraso_real", datediff(col("liquidacao"), col("venc_prorrogado")))

df_metrics_pagos = df_pagos.groupBy("cod_cliente").agg(
    avg("dias_atraso_real").alias("media_atraso_historico"),
    stddev("dias_atraso_real").alias("volatilidade_atraso"), # Consistência
    sum("valor_pago").alias("valor_total_pago"), # Monetary (Volume) - usando valor_pago ou valor_devido se valor_pago nulo
    count(when(col("dias_atraso_real") <= 0, 1)).alias("qtd_pontual"),
    count("*").alias("qtd_total_pagos"),
    max("dias_atraso_real").alias("max_atraso_historico"),
    max("liquidacao").alias("data_ultima_liquidacao") # Recency
).withColumn("taxa_pontualidade", col("qtd_pontual") / col("qtd_total_pagos")) \
 .withColumn("dias_sem_pagar", datediff(current_date(), col("data_ultima_liquidacao"))) # Recency (Dias sem operar/pagar)

# 1.2 Métricas de Risco Atual (Títulos em Aberto)
# Analisamos títulos em aberto e vencidos
df_aberto = df_titulos.filter(col("liquidacao").isNull()) \
    .withColumn("dias_atraso_atual", datediff(current_date(), col("venc_prorrogado")))

df_metrics_risco = df_aberto.groupBy("cod_cliente").agg(
    sum(when(col("dias_atraso_atual") > 5, col("valor_devido")).otherwise(0)).alias("saldo_inadimplente_atual"),
    max("dias_atraso_atual").alias("max_atraso_atual")
)

# 1.3 Tendência (Recente vs Antigo)
# Recente = Últimos 90 dias de liquidação
df_pagos_trend = df_pagos.withColumn("dias_desde_pagamento", datediff(current_date(), col("liquidacao")))

df_trend_recent = df_pagos_trend.filter(col("dias_desde_pagamento") <= 90) \
    .groupBy("cod_cliente").agg(avg("dias_atraso_real").alias("media_atraso_90d"))

df_trend_old = df_pagos_trend.filter((col("dias_desde_pagamento") > 90) & (col("dias_desde_pagamento") <= 180)) \
    .groupBy("cod_cliente").agg(avg("dias_atraso_real").alias("media_atraso_180d"))

# Join Final das Métricas (CORREÇÃO DE BUG: Full Outer Join para incluir clientes sem histórico de pagamentos mas com dívidas)
# Antes: Left Join (excluía novos clientes inadimplentes)
# Agora: Full Join entre Pagos e Risco, depois Left com Tendências (pois tendência depende de pagamento)

df_features = df_metrics_pagos \
    .join(df_metrics_risco, "cod_cliente", "full_outer") \
    .join(df_trend_recent, "cod_cliente", "left") \
    .join(df_trend_old, "cod_cliente", "left") \
    .na.fill(0) # Preencher nulos com 0

# Calcular Tendência (Recente - Antigo).
df_features_final = df_features.withColumn("tendencia_atraso", col("media_atraso_90d") - col("media_atraso_180d")) \
    .select(
        "cod_cliente",
        "media_atraso_historico",
        "taxa_pontualidade",
        "saldo_inadimplente_atual",
        "tendencia_atraso",
        "volatilidade_atraso",
        "valor_total_pago",
        "dias_sem_pagar",
        "max_atraso_historico"
    )

# CELL ********************

# ## 2. Clustering (K-Means)

print("Executando K-Means...")

# 2.1 Preparação (Assembler + Scaler)
# Features expandidas para melhor segmentação
feature_cols = [
    "media_atraso_historico",
    "taxa_pontualidade",
    "tendencia_atraso",
    "saldo_inadimplente_atual",
    "volatilidade_atraso",
    "valor_total_pago"
]

# Vetorização
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
df_vectorized = assembler.transform(df_features_final)

# Normalização (Crítico devido à mistura de unidades: dias, percentual, reais)
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
scaler_model = scaler.fit(df_vectorized)
df_scaled = scaler_model.transform(df_vectorized)

# 2.2 Treinamento
# k=3 (Bom, Rentável, Risco)
kmeans = KMeans(k=3, seed=42, featuresCol="features", predictionCol="cluster_id")
model = kmeans.fit(df_scaled)
df_clustered = model.transform(df_scaled)

# 2.3 Validação (Silhouette Score)
evaluator = ClusteringEvaluator(featuresCol="features", metricName="silhouette", distanceMeasure="squaredEuclidean")
silhouette = evaluator.evaluate(df_clustered)
print(f"Silhouette Score (Qualidade dos Clusters): {silhouette:.4f}")
print("Nota: Silhouette próximo de 1 indica clusters bem definidos. Próximo de -1 indica atribuição errada.")

# CELL ********************

# ## 3. Definição dos Perfis (Labeling)

print("Definindo perfis...")

# Analisar Centroides para dar nome aos clusters de forma dinâmica
df_profiling = df_clustered.groupBy("cluster_id").agg(
    avg("media_atraso_historico").alias("avg_delay"),
    avg("saldo_inadimplente_atual").alias("avg_risk"),
    avg("taxa_pontualidade").alias("avg_punctuality"),
    avg("volatilidade_atraso").alias("avg_volatility"),
    avg("valor_total_pago").alias("avg_volume"),
    count("*").alias("count")
).sort("avg_delay")

profiles = df_profiling.collect()

# Lógica Dinâmica de Atribuição (Baseada em Atraso e Risco):
# 1. Ordenamos por Atraso Médio (crescente).
# 2. O menor atraso (profiles[0]) é "Prime".
# 3. O maior atraso (profiles[2]) é "Alerta".
# 4. Intermediário é "Rentável".

cluster_map = {
    profiles[0]['cluster_id']: "1. Prime (Estável)",
    profiles[1]['cluster_id']: "2. Rentável (Atraso Moderado)",
    profiles[2]['cluster_id']: "3. Alerta (Risco de Inadimplência)"
}

print("Mapeamento de Clusters identificado:")
for row in profiles:
    print(f"Cluster {row['cluster_id']}: Delay={row['avg_delay']:.2f}, Risk={row['avg_risk']:.2f}, Volat={row['avg_volatility']:.2f} -> {cluster_map[row['cluster_id']]}")

# Aplicar Mapeamento
mapping_expr = create_map([lit(x) for x in chain(*cluster_map.items())])
df_final_labeled = df_clustered.withColumn("perfil_cliente", mapping_expr[col("cluster_id")])

# CELL ********************

# ## 4. Salvar Resultado

print("Salvando tabela final LH_Gold.analise_cluster_clientes...")

df_output = df_final_labeled.join(df_clientes.select("cod_cliente", "nome"), "cod_cliente", "left") \
    .select(
        "cod_cliente",
        "nome",
        "perfil_cliente",
        "media_atraso_historico",
        "taxa_pontualidade",
        "tendencia_atraso",
        "saldo_inadimplente_atual",
        "volatilidade_atraso",
        "valor_total_pago",
        "max_atraso_historico"
    )

table_name = "LH_Gold.analise_cluster_clientes"
df_output.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(table_name)

print(f"Tabela {table_name} salva com sucesso!")
print("Amostra dos dados:")
df_output.show(10, truncate=False)
