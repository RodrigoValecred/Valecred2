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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import mlflow
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 1. LEITURA DIRETA DO LAKEHOUSE (Sem JDBC)

# CELL ********************

# Ler a tabela que já está no seu Lakehouse
df_hoje_raw = spark.table("LH_Bronze.bronze_operacoes_intraday")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# 2. CORREÇÃO IMEDIATA DA PRECISÃO (O erro dos 40 dígitos)
# 
# Antes de qualquer coisa, convertemos os decimais gigantes para Double
# para o Spark não travar com "ArithmeticException".

# CELL ********************

cols_numericas = ["VOLUME_OPERADO", "taxa", "prazo_medio"]
df_hoje_clean = df_hoje_raw

for col_name in cols_numericas:
    # Verifica se a coluna existe antes de tentar converter
    if col_name in df_hoje_raw.columns:
        df_hoje_clean = df_hoje_clean.withColumn(col_name, F.col(col_name).cast(DoubleType()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 3. TRATAMENTO E ENRIQUECIMENTO
# 
# A. Renomear colunas (Ajuste Crítico aqui)
# Agora estamos pegando 'valor_titulo' (que existe na Bronze) e virando 'vlr_total_sacado'

# CELL ********************

df_hoje_ajustado = df_hoje_clean \
    .withColumnRenamed("NBORDERO", "id_operacao") \
    .withColumnRenamed("valor_titulo", "vlr_total_sacado") \
    .withColumnRenamed("prazo_medio", "prazo_medio_titulos") \
    .withColumnRenamed("taxa_aquisicao", "taxa") \
    .withColumnRenamed("QTD_TITULOS", "qtd_titulos") \
    .withColumnRenamed("cpf_cnpj_sacado", "cpf_cnpj_sacado")

# B. Ler o PERFIL DO SACADO
df_perfil = spark.table("LH_Gold.perfil_risco_sacado")

# C. O JOIN
df_enrich = df_hoje_ajustado.join(df_perfil, on="cpf_cnpj_sacado", how="left")

# D. Tratar nulos
df_enrich = df_enrich.fillna(0, subset=["exposicao_total_d1", "qtd_titulos_aberto", "maior_atraso_atual"])

# E. Calcular métricas para a IA (Agora vai funcionar pois vlr_total_sacado existe)
df_enrich = df_enrich.withColumn(
    "exposicao_acumulada", 
    F.col("exposicao_total_d1") + F.col("vlr_total_sacado")
).withColumn(
    "concentracao_operacao",
    F.col("vlr_total_sacado") / F.col("exposicao_acumulada")
).fillna(0)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 4. A IA (INFERÊNCIA)

# CELL ********************

df_pandas = df_enrich.toPandas()

# LISTA OFICIAL DE FEATURES
features = [
    'vlr_total_sacado', 
    'prazo_medio_titulos', 
    'taxa', 
    'exposicao_acumulada',
    'concentracao_operacao',
    'qtd_titulos'
]

# Garantia contra nulos
df_pandas[features] = df_pandas[features].fillna(0)

# RUN_ID = "c122501f-3c87-43dc-af99-8af8d918866c"
RUN_ID = "f0c0000a-f643-4759-8e08-01cc6d1962de"
model_uri = f"runs:/{RUN_ID}/Modelo_Risco_FIDC"

try:
    print(f"Carregando IA da Run: {RUN_ID}...")
    loaded_model = mlflow.sklearn.load_model(model_uri)
    
    # --- O PULO DO GATO ---
    # Perguntamos ao modelo: "Qual a ordem exata das colunas que você quer?"
    if hasattr(loaded_model, "feature_names_in_"):
        cols_esperadas = loaded_model.feature_names_in_
        print(f"Reordenando colunas para: {cols_esperadas}")
        
        # Garantimos que passamos EXATAMENTE o que ele pede, na ordem que ele pede
        df_pandas['anomaly_score'] = loaded_model.predict(df_pandas[cols_esperadas])
    else:
        # Fallback caso o modelo seja muito antigo (improvável no Fabric)
        print("Aviso: Modelo sem metadados de colunas. Tentando ordem manual.")
        df_pandas['anomaly_score'] = loaded_model.predict(df_pandas[features])

    print("✅ SUCESSO ABSOLUTO: Inteligência Artificial aplicada!")

except Exception as e:
    print(f"❌ Erro ao carregar IA: {e}")
    print("⚠️ Usando regra manual de contingência.")
    
    # Regra de Contingência
    df_pandas['anomaly_score'] = df_pandas.apply(
        lambda x: -1 if (x['taxa'] < 1.5 or x['prazo_medio_titulos'] > 60) else 1, axis=1
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 5. SAÍDA (Tabela Gold para a TV)

# CELL ********************

df_pandas['status_ia'] = df_pandas['anomaly_score'].apply(lambda x: "ALTO RISCO" if x == -1 else "NORMAL")
df_pandas['data_processamento'] = pd.Timestamp.now()

# Salvar overwrite (Para a TV piscar sempre com o dado mais novo)
df_final = spark.createDataFrame(df_pandas)
df_final.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("LH_Gold.Alertas_Risco_TV")

print(f"Sucesso! {len(df_pandas)} operações enviadas para o Painel.")
display(df_final.select("id_operacao", "cpf_cnpj_sacado", "vlr_total_sacado", "status_ia"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
