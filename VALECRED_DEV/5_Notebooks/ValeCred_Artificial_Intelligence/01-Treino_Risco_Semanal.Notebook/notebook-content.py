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
# META           "id": "8f85c372-56ad-4f3f-acf9-3be2e9b99513"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ==============================================================================
# IMPORTAÇÕES UNIFICADAS
# ==============================================================================
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from sklearn.ensemble import IsolationForest
import pandas as pd
import mlflow
import mlflow.sklearn

# ==============================================================================
# 1. ENGENHARIA DE DADOS (SPARK) - A BASE ROBUSTA
# ==============================================================================
print("1. Iniciando Engenharia de Dados...")

# A. Carregar tabelas (Com filtro de data para performance)
# IMPORTANTE: Selecionar 'chave_produto' para o Join e 'taxa' para a IA
df_ops = spark.table("spark_catalog.LH_Silver.staging_operacoes_limpa") \
    .filter("data_analise >= date_sub(current_date(), 365)") \
    .select("cod_operacao", "taxa", "data_analise", "chave_produto") 

df_titulos = spark.table("spark_catalog.LH_Silver.staging_titulos_limpa") \
    .select("cod_operacao", "cod_titulo", "valor", "prazo", "cpf_cnpj_sacado")

df_produtos = spark.table("spark_catalog.LH_Silver.dim_produto")

# Calcular a "Liquidez Interna" (Capacidade de Pagamento)
df_pagamentos = spark.table("LH_Silver.staging_titulos_limpa") \
    .filter("liquidacao >= date_sub(current_date(), 180)") \
    .groupBy("cpf_cnpj_sacado") \
    .agg(
        ((F.sum("valor") - F.sum("valor_devido")) / 6).alias("media_pagamento_mensal") # Média dos últimos 6 meses
    )
# df_pagamentos.show(5)

# B. Agrupar Títulos (Granularidade Sacado)
df_agg_titulos = df_titulos.groupBy("cod_operacao", "cpf_cnpj_sacado").agg(
    F.sum("valor").alias("vlr_total_sacado"),
    F.count("cod_titulo").alias("qtd_titulos"),
    F.avg("prazo").alias("prazo_medio_titulos"),
    F.max("valor").alias("maior_titulo")
)

# C. JOIN MESTRE (Operação + Títulos + PRODUTO)
# Primeiro juntamos operação com títulos
df_join_ops = df_ops.join(df_agg_titulos, on="cod_operacao", how="inner")

# Agora juntamos com o Produto
df_full = df_join_ops.join(
    df_produtos,
    F.col("chave_produto") == F.col("chave_produto_txt"),
    how="left"
).drop(df_produtos.chave_produto_txt)

# Tratamento de segurança: Se não achar o produto, vira 0
df_full = df_full.fillna(0, subset=["cod_produto_ia"])

df_features = df_full.join(df_pagamentos, on="cpf_cnpj_sacado", how="left")
# df_features.show(5)

# D. Feature Engineering (Janelas de Exposição)
w_sacado = Window.partitionBy("cpf_cnpj_sacado").orderBy("data_analise") \
                 .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_features_spark = df_features.withColumn(
    "exposicao_acumulada", 
    F.sum("vlr_total_sacado").over(w_sacado)
).withColumn(
    "concentracao_operacao",
    F.col("vlr_total_sacado") / F.col("exposicao_acumulada")
).fillna(0).withColumn(
    "ratio_cobertura_liquidez",
    F.col("vlr_total_sacado") / F.col("media_pagamento_mensal")
)
# df_features_spark.show(5)

print("✅ Engenharia concluída!")

# ==============================================================================
# 2. CIÊNCIA DE DADOS (SCIKIT-LEARN)
# ==============================================================================
print("2. Iniciando Treinamento da IA...")

# Converter para Pandas
df_pandas = df_features_spark.toPandas()

# Definir as Features
feature_cols = [
    'vlr_total_sacado', 
    'prazo_medio_titulos', 
    'taxa', 
    'qtd_titulos',
    'exposicao_acumulada',
    'concentracao_operacao',
    'cod_produto_ia',
    'ratio_cobertura_liquidez'
]

print("🧹 Limpando dados (Removendo NaNs)...")
for col in feature_cols:
    if col in df_pandas.columns:
        df_pandas[col] = df_pandas[col].fillna(0)

import numpy as np
df_pandas[feature_cols] = df_pandas[feature_cols].replace([np.inf, -np.inf], 0)

# Treinamento (Isolation Forest)
# contamination=0.02 (2% de anomalias)
model = IsolationForest(n_estimators=100, contamination=0.02, random_state=42, n_jobs=-1)

# Fit e Predict
df_pandas['anomaly_score'] = model.fit_predict(df_pandas[feature_cols])

print("✅ Modelo treinado com sucesso!")
# df_pandas.show(5)
# ==============================================================================
# 3. SALVAR RESULTADOS (MLFLOW E LAKEHOUSE)
# ==============================================================================

with mlflow.start_run():
    mlflow.sklearn.log_model(model, "Modelo_Risco_FIDC")
    mlflow.log_param("features_usadas", str(feature_cols))
    print("💾 Modelo salvo no MLflow.")

# ==============================================================================
# CONSOLIDAÇÃO GOLD: TABELA MESTRA DE PERFIL DO SACADO 🏆
# ==============================================================================
print("💾 Salvando Perfil Analítico Unificado (Gold)...")

df_perfil_unificado = df_pandas.groupby("cpf_cnpj_sacado").agg({
    'exposicao_acumulada': 'max',      # A maior exposição que ele já teve (ou a última)
    'prazo_medio_titulos': 'mean',     # O prazo médio que ele costuma operar
    'media_pagamento_mensal': 'max'    # A capacidade de pagamento (é valor fixo por cliente)
}).reset_index()

df_perfil_unificado.columns = [
    'cpf_cnpj_sacado', 
    'exposicao_maxima_historica', 
    'prazo_medio_historico', 
    'media_pagamento_mensal'
]

cols_float = ['exposicao_maxima_historica', 'prazo_medio_historico', 'media_pagamento_mensal']
for col in cols_float:
    # fillna(0) garante que não tem NaN
    # astype(float) força virar número decimal simples (Double), matando o tipo Decimal problemático
    df_perfil_unificado[col] = df_perfil_unificado[col].fillna(0).astype(float)

df_perfil_unificado['media_pagamento_mensal'] = df_perfil_unificado['media_pagamento_mensal'].replace(0, 1.0)

df_perfil_unificado = df_perfil_unificado.fillna(0)

spark.createDataFrame(df_perfil_unificado) \
    .write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("LH_Gold.Perfil_Analitico_Sacado")

print("🚀 Processo Finalizado! Tabela LH_Gold.Perfil_Analitico_Sacado atualizada.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
