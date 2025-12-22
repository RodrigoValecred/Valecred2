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

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import mlflow
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# ==============================================================================
# 1. LEITURA E PREPARAÇÃO DOS DADOS (Spark)
# ==============================================================================
print("1. Lendo dados do Lakehouse...")

# Leitura da tabela Bronze (Operações do Dia)
# Usando tratamento de erro caso a tabela não exista ainda
try:
    df_hoje_raw = spark.table("LH_Bronze.Bronze_Operacoes_Intraday")
except:
    # Fallback para nome minúsculo se necessário
    df_hoje_raw = spark.table("LH_Bronze.bronze_operacoes_intraday")

# Correção preventiva de tipos (Decimal -> Double) para evitar ArithmeticException
cols_numericas = ["valor_titulo", "taxa_aquisicao", "prazo_medio"]
df_hoje_clean = df_hoje_raw
for col_name in cols_numericas:
    if col_name in df_hoje_raw.columns:
        df_hoje_clean = df_hoje_clean.withColumn(col_name, F.col(col_name).cast(DoubleType()))

# Renomear colunas para bater com o treinamento da V.A.I.
# De (Bronze) -> Para (Modelo Treinado)
df_hoje_ajustado = df_hoje_clean \
    .withColumnRenamed("NBORDERO", "id_operacao") \
    .withColumnRenamed("valor_titulo", "vlr_total_sacado") \
    .withColumnRenamed("prazo_medio", "prazo_medio_titulos") \
    .withColumnRenamed("taxa_aquisicao", "taxa") \
    .withColumnRenamed("QTD_TITULOS", "qtd_titulos") \
    .withColumnRenamed("cpf_cnpj_sacado", "cpf_cnpj_sacado")

# Ler o Perfil de Risco (Gold) - Histórico do Cliente
df_perfil = spark.table("LH_Gold.perfil_risco_sacado")

# Join (Enriquecimento)
df_enrich = df_hoje_ajustado.join(df_perfil, on="cpf_cnpj_sacado", how="left")

# Tratamento de Nulos (Clientes novos ganham zero no histórico)
df_enrich = df_enrich.fillna(0, subset=["exposicao_total_d1", "qtd_titulos_aberto", "maior_atraso_atual"])

# Cálculos de Métricas em Tempo Real
df_enrich = df_enrich.withColumn(
    "exposicao_acumulada", 
    F.col("exposicao_total_d1") + F.col("vlr_total_sacado")
).withColumn(
    "concentracao_operacao",
    F.col("vlr_total_sacado") / F.col("exposicao_acumulada")
).fillna(0)

# --- CONVERSÃO PARA PANDAS (Para aplicar o modelo Scikit-Learn) ---
print("2. Convertendo para Pandas para aplicar IA...")
df_pandas = df_enrich.toPandas()

# Lista de colunas de backup (caso a auto-ordenação falhe)
features_backup = [
    'vlr_total_sacado', 'prazo_medio_titulos', 'taxa', 
    'exposicao_acumulada', 'concentracao_operacao', 'qtd_titulos'
]
# Garante que não existem nulos nas features
for col in features_backup:
    if col in df_pandas.columns:
        df_pandas[col] = df_pandas[col].fillna(0)

# ==============================================================================
# 2. BUSCA DINÂMICA DO CÉREBRO DA V.A.I. (MLflow)
# ==============================================================================

# Seus dados configurados
NOME_EXPERIMENTO = "VAI_Treinamento_Semanal"
ID_EXPERIMENTO_NOVO = "8a9354d9-f2de-4a36-b684-9785a8462997"

try:
    print(f"3. Buscando inteligência mais recente em: {NOME_EXPERIMENTO}...")
    
    # Busca a run mais recente com status FINISHED (Sucesso)
    runs = mlflow.search_runs(
        experiment_ids=[ID_EXPERIMENTO_NOVO],
        filter_string="status = 'FINISHED'",
        order_by=["start_time DESC"],
        max_results=1
    )
    
    if len(runs) == 0:
        raise Exception("Nenhum modelo treinado encontrado neste experimento.")
        
    latest_run_id = runs.iloc[0].run_id
    print(f"✅ Cérebro encontrado! Usando Run ID: {latest_run_id}")
    
    # Carrega o modelo
    model_uri = f"runs:/{latest_run_id}/Modelo_Risco_FIDC"
    loaded_model = mlflow.sklearn.load_model(model_uri)
    
    # Aplica a IA (Com auto-ordenação de colunas)
    # Isso evita o erro "Feature names must be in the same order"
    if hasattr(loaded_model, "feature_names_in_"):
        cols_esperadas = loaded_model.feature_names_in_
        # Garante que todas as colunas esperadas existam no DF
        for col in cols_esperadas:
            if col not in df_pandas.columns:
                df_pandas[col] = 0
        df_pandas['anomaly_score'] = loaded_model.predict(df_pandas[cols_esperadas])
    else:
        # Fallback para modelos antigos
        df_pandas['anomaly_score'] = loaded_model.predict(df_pandas[features_backup])

    print("🚀 V.A.I. aplicada com sucesso!")

except Exception as e:
    print(f"❌ Erro/Aviso na IA: {e}")
    print("⚠️ ATENÇÃO: Usando regra manual de contingência.")
    
    # Regra de Contingência (Plano B se o MLflow falhar)
    # Taxa < 1.5% ou Prazo > 60 dias = Risco (-1)
    df_pandas['anomaly_score'] = df_pandas.apply(
        lambda x: -1 if (x['taxa'] < 1.5 or x['prazo_medio_titulos'] > 60) else 1, axis=1
    )

# ==============================================================================
# 3. SALVAR RESULTADO NA TV
# ==============================================================================

# Criação da coluna visual legível
df_pandas['status_ia'] = df_pandas['anomaly_score'].apply(lambda x: "ALTO RISCO" if x == -1 else "NORMAL")
df_pandas['data_processamento'] = pd.Timestamp.now()

# Converter de volta para Spark e Salvar Delta Table
print("4. Salvando resultados na tabela Gold...")
df_final = spark.createDataFrame(df_pandas)
df_final.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("LH_Gold.Alertas_Risco_TV")

print(f"✅ Processo Finalizado! {len(df_pandas)} operações enviadas para a TV.")
display(df_final.select("id_operacao", "vlr_total_sacado", "status_ia"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
