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
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Análise de Cluster de Clientes (Híbrido: Regras de Negócio + K-Means)
# **Objetivo:** Segmentar a base de clientes em 3 grupos comportamentais (RFM e Behavioral Scoring) com priorização de risco (PDD).
# # **Estratégia Híbrida:**
# 1. **Regras de Negócio (Hard Filters):** Clientes em situação crítica (Renegociação ou Atraso > 120 dias) são *automaticamente* classificados como "Alerta".
# 2. **K-Means:** Aplicado apenas aos clientes restantes para distinguir entre "Prime" e "Rentável".
# # **Perfis:**
# 1. **Prime (Estável)**: Pagam em dia e possuem consistência.
# 2. **Rentável (Atraso Moderado)**: Pagam com atraso, gerando receita de juros, mas sem risco crítico.
# 3. **Alerta (Risco)**: Renegociação ('RN'), Atraso > 120 dias, ou comportamento degradante.

# CELL ********************

from pyspark.sql.functions import col, datediff, avg, sum, count, max, current_date, when, lit, min, create_map, stddev, coalesce, abs
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from itertools import chain
import time

# Configurações
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")

# Ler Tabelas Gold
print("Carregando tabelas Gold...")
try:
    df_titulos = spark.read.table("LH_Gold.fato_titulos")
    df_clientes = spark.read.table("LH_Gold.dim_clientes")
    # df_operacoes = spark.read.table("LH_Gold.fato_operacoes") # Pode ser necessário para verificar RN se não estiver em titulos
except Exception as e:
    print(f"Erro ao carregar tabelas: {e}")
    raise e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ## 1. Feature Engineering (Com Flags de Risco)

print("Calculando métricas e flags de risco...")

# 1.1 Títulos Liquidados (Histórico)
df_pagos = df_titulos.filter(col("liquidacao").isNotNull()) \
    .withColumn("dias_atraso_real", datediff(col("liquidacao"), col("venc_prorrogado")))

df_metrics_pagos = df_pagos.groupBy("cod_cliente").agg(
    avg("dias_atraso_real").alias("media_atraso_historico"),
    stddev("dias_atraso_real").alias("volatilidade_atraso"),
    sum("valor_pago").alias("valor_total_pago"),
    count(when(col("dias_atraso_real") <= 0, 1)).alias("qtd_pontual"),
    count("*").alias("qtd_total_pagos"),
    max("dias_atraso_real").alias("max_atraso_historico"),
    max("liquidacao").alias("data_ultima_liquidacao")
).withColumn("taxa_pontualidade", col("qtd_pontual") / col("qtd_total_pagos")) \
 .withColumn("dias_sem_pagar", datediff(current_date(), col("data_ultima_liquidacao")))

# 1.2 Risco Atual (Aberto) + Flags Críticas (RN e > 120 dias)
df_aberto = df_titulos.filter(col("liquidacao").isNull()) \
    .withColumn("dias_atraso_atual", datediff(current_date(), col("venc_prorrogado")))

# Verificar 'RN' (Renegociação). Se 'chave_produto' ou 'tto' indicar RN.
# Assumindo que 'chave_produto' em fato_titulos contém 'RN' ou similar.
# Se não, precisaríamos join com fato_operacoes. Vamos assumir que 'chave_produto' == 'RN' é válido.
# Se chave_produto não existir, usar tto (se disponível).
col_produto = "chave_produto" if "chave_produto" in df_aberto.columns else "tto" # Fallback

df_metrics_risco = df_aberto.groupBy("cod_cliente").agg(
    sum(when(col("dias_atraso_atual") > 5, col("valor_devido")).otherwise(0)).alias("saldo_inadimplente_atual"),
    max("dias_atraso_atual").alias("max_atraso_atual"),
    # Flag: Tem atraso > 120 dias? (PDD 100%)
    max(when(col("dias_atraso_atual") > 120, 1).otherwise(0)).alias("flag_pdd_120"),
    # Flag: Tem produto Renegociação ('RN') em aberto?
    max(when(col(col_produto) == "RN", 1).otherwise(0)).alias("flag_renegociacao")
)

# 1.3 Tendência
df_pagos_trend = df_pagos.withColumn("dias_desde_pagamento", datediff(current_date(), col("liquidacao")))
df_trend_recent = df_pagos_trend.filter(col("dias_desde_pagamento") <= 90) \
    .groupBy("cod_cliente").agg(avg("dias_atraso_real").alias("media_atraso_90d"))
df_trend_old = df_pagos_trend.filter((col("dias_desde_pagamento") > 90) & (col("dias_desde_pagamento") <= 180)) \
    .groupBy("cod_cliente").agg(avg("dias_atraso_real").alias("media_atraso_180d"))

# Join Final
df_features = df_metrics_pagos \
    .join(df_metrics_risco, "cod_cliente", "full_outer") \
    .join(df_trend_recent, "cod_cliente", "left") \
    .join(df_trend_old, "cod_cliente", "left") \
    .na.fill(0)

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
        "max_atraso_historico",
        "max_atraso_atual",
        "flag_pdd_120",
        "flag_renegociacao"
    )

# 🧠 OTIMIZAÇÃO TENSOR: Reuso do cache de engenharia de features pesada
# Este dataframe é usado tanto para 'df_critical' quanto para 'df_to_cluster', e subsequentemente no KMeans
print("⚡ Tensor: Caching df_features_final to prevent re-computation...")
start_cache = time.time()
df_features_final.cache()
count_features = df_features_final.count() # Force materialization
print(f"⚡ Tensor: Feature store cached. Count: {count_features}. Time: {time.time() - start_cache:.2f}s")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ## 2. Clustering (K-Means)

print("Executando K-Means...")

# 2.1 Preparação (Assembler + Scaler)
# 🧠 OTIMIZAÇÃO TENSOR: Removido bloco de treinamento redundante de K=3.
# A lógica abaixo usa uma abordagem híbrida (Regras Rígidas + K=2) começando a partir de df_features_final.
# O modelo K=3 anterior foi treinado mas seus resultados nunca foram usados.
print("⚡ Tensor: Skipped redundant K=3 Model Training.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Aplicando regras de negócio (Hard Rules) para Risco Crítico...")

# 2.1 Separação: Risco Crítico vs Clusterizável
# Regra: Se (Atraso > 120) OU (Renegociação) -> "3. Alerta (Risco)" AUTOMÁTICO
df_critical = df_features_final.filter(
    (col("flag_pdd_120") == 1) |
    (col("flag_renegociacao") == 1) |
    (col("max_atraso_atual") > 60) # Regra extra de segurança: atraso > 60 dias já é grave
)
df_critical = df_critical.withColumn("perfil_cliente", lit("3. Alerta (Risco de Inadimplência)")) \
                         .withColumn("origem_classificacao", lit("Regra de Negócio (PDD/RN)"))

# ⚡ Bolt: Caching df_critical e df_to_cluster antes do count() para evitar re-computação
df_critical.cache()
print(f"Clientes classificados como Risco por Regra: {df_critical.count()}")

# Clientes Restantes para Clusterização (Prime vs Rentável)
df_to_cluster = df_features_final.join(df_critical.select("cod_cliente"), "cod_cliente", "left_anti")
df_to_cluster.cache()
print(f"Clientes restantes para Clusterização: {df_to_cluster.count()}")

# 2.2 K-Means nos Restantes
# 🧠 Otimização Tensor: Substituir count() > 0 por not df.isEmpty() para evitar varredura completa dos dados
if not df_to_cluster.isEmpty():
    print("Executando K-Means nos clientes restantes...")

    # Features (sem as flags, pois elas já definiram o grupo crítico)
    feature_cols = [
        "media_atraso_historico",
        "taxa_pontualidade",
        "tendencia_atraso",
        "saldo_inadimplente_atual",
        "volatilidade_atraso",
        "valor_total_pago"
    ]

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
    df_vectorized = assembler.transform(df_to_cluster)

    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
    scaler_model = scaler.fit(df_vectorized)
    df_scaled = scaler_model.transform(df_vectorized)

    # K=2 agora (Prime vs Rentável/Moderado), pois o "Ruim" já foi separado
    # A menos que queiramos subdividir o "Alerta" leve. Vamos usar K=2 para forçar a distinção.
    # Ou K=3 se quisermos "Prime", "Bom", "Regular".
    # O pedido original era 3 grupos. Já temos o "Alerta".
    # Vamos tentar dividir o resto em "Prime" e "Rentável". K=2.
    kmeans = KMeans(k=2, seed=42, featuresCol="features", predictionCol="cluster_id")
    model = kmeans.fit(df_scaled)
    df_clustered = model.transform(df_scaled)

    # Labeling Dinâmico (K=2)
    df_profiling = df_clustered.groupBy("cluster_id").agg(avg("media_atraso_historico").alias("avg_delay")).sort("avg_delay")
    profiles = df_profiling.collect()

    # Menor atraso = Prime
    cluster_map = {
        profiles[0]['cluster_id']: "1. Prime (Estável)",
        profiles[1]['cluster_id']: "2. Rentável (Atraso Moderado)"
    }

    mapping_expr = create_map([lit(x) for x in chain(*cluster_map.items())])
    df_clustered_labeled = df_clustered.withColumn("perfil_cliente", mapping_expr[col("cluster_id")]) \
                                       .withColumn("origem_classificacao", lit("Algoritmo K-Means"))

    # Selecionar colunas iguais para Union
    cols_final = df_critical.columns
    df_final_combined = df_critical.unionByName(df_clustered_labeled.select(*cols_final))

else:
    print("Todos os clientes caíram na regra crítica (improvável).")
    df_final_combined = df_critical

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ## 3. Salvar Resultado

print("Salvando tabela final LH_Gold.analise_cluster_clientes...")

df_output = df_final_combined.join(df_clientes.select("cod_cliente", "nome"), "cod_cliente", "left") \
    .select(
        "cod_cliente",
        "nome",
        "perfil_cliente",
        "origem_classificacao",
        "media_atraso_historico",
        "taxa_pontualidade",
        "tendencia_atraso",
        "saldo_inadimplente_atual",
        "volatilidade_atraso",
        "valor_total_pago",
        "max_atraso_historico",
        "flag_pdd_120",
        "flag_renegociacao"
    )

table_name = "LH_Gold.analise_cluster_clientes"
df_output.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(table_name)

print(f"Tabela {table_name} salva com sucesso!")
print("Amostra dos dados:")
df_output.show(10, truncate=False)

# 🧠 OTIMIZAÇÃO TENSOR: Liberar memória
df_features_final.unpersist()
# ⚡ Bolt: Liberar os caches recém-criados
df_critical.unpersist()
df_to_cluster.unpersist()
print("⚡ Tensor: Cache cleared.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
