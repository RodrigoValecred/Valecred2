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

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Carregar as tabelas (baseado nos nomes do seu log de erro)
df_ops = spark.table("spark_catalog.LH_Silver.staging_operacoes_limpa")
df_titulos = spark.table("spark_catalog.LH_Silver.staging_titulos_limpa")

# 2. CORREÇÃO AQUI: Agrupar também pelo SACADO
# Isso garante que se uma operação tiver 3 sacados, teremos 3 linhas, mantendo o ID do sacado vivo.
df_agg_titulos = df_titulos.groupBy("cod_operacao", "cpf_cnpj_sacado").agg(
    F.sum("valor").alias("vlr_total_operacao_por_sacado"), # Valor que ESSE sacado tem nessa operação
    F.count("cod_titulo").alias("qtd_titulos"),
    F.mean("valor").alias("ticket_medio_titulo"),
    F.max("valor").alias("maior_titulo")
)

# 3. Join
# Agora o df_full terá a granularidade: Operação + Sacado
df_full = df_ops.join(df_agg_titulos, on="cod_operacao", how="inner")

# 4. A Lógica de Janela (Agora vai funcionar porque a coluna existe)
# Ordenamos por data_analise para ver o histórico de crescimento da dívida desse sacado
w_sacado = Window.partitionBy("cpf_cnpj_sacado").orderBy("data_analise") \
                 .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_features = df_full.withColumn(
    "exposicao_acumulada_sacado", 
    F.sum("vlr_total_operacao_por_sacado").over(w_sacado)
)

# Cálculo de concentração (Quanto essa operação representa da dívida total dele até agora?)
df_features = df_features.withColumn(
    "concentracao_operacao",
    F.col("vlr_total_operacao_por_sacado") / F.col("exposicao_acumulada_sacado")
)

# Tratamento de nulos/infinitos caso seja a primeira operação
df_features = df_features.fillna(0, subset=["concentracao_operacao"])

# Visualizar para validar
display(df_features.select("cod_operacao", "cpf_cnpj_sacado", "data_analise", "exposicao_acumulada_sacado", "concentracao_operacao"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from sklearn.ensemble import IsolationForest
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType

# ==============================================================================
# 1. PREPARAÇÃO DOS DADOS (Engenharia - Spark)
# ==============================================================================

# Definir janela de tempo (ex: últimos 180 ou 365 dias para treino recente)
# Isso garante que o modelo veja ~3 ciclos completos do Prazo Médio de 50 dias
# Filtramos ANTES de processar para ganhar performance no Fabric
df_ops = spark.table("spark_catalog.LH_Silver.staging_operacoes_limpa") \
    .filter("data_analise >= date_sub(current_date(), 180)") \
    .select("cod_operacao", "taxa", "data_analise") # Selecionar só o útil

df_titulos = spark.table("spark_catalog.LH_Silver.staging_titulos_limpa") \
    .select("cod_operacao", "cod_titulo", "valor", "prazo", "cpf_cnpj_sacado")

# Agregação Inteligente (Nível Operação + Sacado)
# Aqui calculamos o prazo médio ponderado ou simples dos títulos daquele sacado
df_agg = df_titulos.groupBy("cod_operacao", "cpf_cnpj_sacado").agg(
    F.sum("valor").alias("vlr_total_sacado"),
    F.count("cod_titulo").alias("qtd_titulos"),
    F.avg("prazo").alias("prazo_medio_titulos"), # Média do prazo dos títulos
    F.max("valor").alias("maior_titulo")
)

# Join com a Operação para pegar a TAXA (que é nível operação)
df_full = df_agg.join(df_ops, on="cod_operacao", how="inner")

# Janela para Calcular Exposição Acumulada (Risco Sacado)
w_sacado = Window.partitionBy("cpf_cnpj_sacado").orderBy("data_analise") \
                 .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_features_spark = df_full.withColumn(
    "exposicao_acumulada", 
    F.sum("vlr_total_sacado").over(w_sacado)
).withColumn(
    "concentracao_operacao",
    F.col("vlr_total_sacado") / F.col("exposicao_acumulada")
).fillna(0)

# ==============================================================================
# 2. MODELAGEM (Ciência de Dados - Scikit-Learn)
# ==============================================================================

# Converter para Pandas (Trazendo para a memória do Driver)
df_pandas = df_features_spark.toPandas()

# Definir as Features (As colunas que o robô vai olhar para julgar)
feature_cols = [
    'vlr_total_sacado', 
    'prazo_medio_titulos', 
    'taxa', 
    'qtd_titulos',
    'exposicao_acumulada',  # Fundamental: O robô aprende que exposição alta exige comportamento X
    'concentracao_operacao' # Fundamental: Detecta "All-in" (operações únicas muito grandes)
]

# Treinamento do Modelo
# n_estimators=100: cria 100 árvores de decisão
# contamination=0.02: Estamos dizendo "Marque os 2% mais estranhos como anomalia"
model = IsolationForest(n_estimators=100, contamination=0.02, random_state=42, n_jobs=-1)

# Fit e Predict (-1 = Anomalia, 1 = Normal)
df_pandas['anomaly_score'] = model.fit_predict(df_pandas[feature_cols])

# ==============================================================================
# 3. SAÍDA (Output - Lakehouse Gold)
# ==============================================================================
# ... (Todo o código de treino que você já fez) ...

import mlflow
import mlflow.sklearn

# 1. Salvar o Cérebro (Modelo)
with mlflow.start_run():
    mlflow.sklearn.log_model(model, "Modelo_Risco_FIDC")
    print("Modelo salvo no MLflow com sucesso.")

# 2. Salvar o Contexto (Perfil do Sacado para o Online usar)
# O robô online precisa saber o histórico do sacado para julgar o presente
df_perfil_sacados = df_pandas.groupby("cpf_cnpj_sacado").agg({
    'exposicao_acumulada': 'max', # Pega a última exposição conhecida
    'prazo_medio_titulos': 'mean'
}).reset_index()

# Salvar essa tabela de apoio
spark.createDataFrame(df_perfil_sacados).write.mode("overwrite").format("delta").saveAsTable("LH_Gold.Apoio_Perfil_Sacados")
print("Tabela de Perfil de Sacados atualizada.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
