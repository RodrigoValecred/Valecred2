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

import mlflow
import pandas as pd
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# ==============================================================================
# 1. LEITURA E PREPARAÇÃO DOS DADOS (Spark)
# ==============================================================================
print("1. Lendo dados do Lakehouse...")

# Leitura da tabela Bronze (Operações do Dia)
try:
    df_hoje_raw = spark.table("LH_Bronze.Bronze_Operacoes_Intraday")
except:
    df_hoje_raw = spark.table("LH_Bronze.bronze_operacoes_intraday")
# df_hoje_raw.show(5)

# Leitura da Tabela de Produtos (A nossa "biblioteca" de códigos) 🆕
df_dim_produto = spark.table("LH_Silver.dim_produto")

try:
    df_perfil = spark.table("LH_Gold.Perfil_Analitico_Sacado")
except:
    print("⚠️ Perfil não encontrado. Usando dummies.")
    df_perfil = None

# Correção preventiva de tipos (Decimal -> Double)
cols_numericas = ["valor_titulo", "taxa_aquisicao", "prazo_medio"]
df_hoje_clean = df_hoje_raw
for col_name in cols_numericas:
    if col_name in df_hoje_raw.columns:
        df_hoje_clean = df_hoje_clean.withColumn(col_name, F.col(col_name).cast(DoubleType()))
# df_hoje_clean.show(5)

# Renomear colunas para bater com o treinamento da V.A.I.
# ⚠️ IMPORTANTE: Garantir que TTO e STTO venham junto!
df_hoje_ajustado = df_hoje_clean \
    .withColumnRenamed("NBORDERO", "id_operacao") \
    .withColumnRenamed("valor_titulo", "vlr_total_sacado") \
    .withColumnRenamed("prazo_medio", "prazo_medio_titulos") \
    .withColumnRenamed("taxa_aquisicao", "taxa") \
    .withColumnRenamed("QTD_TITULOS", "qtd_titulos") \
    .withColumnRenamed("cpf_cnpj_sacado", "cpf_cnpj_sacado") \
    .withColumnRenamed("TIPO_OPERACAO", "TTO") \
    .withColumnRenamed("SUBTIPO_OPERACAO", "STTO")
    # (Ajuste os nomes "TIPO_OPERACAO" acima se na Bronze eles tiverem outro nome)
# df_hoje_ajustado.show(5)

# ==============================================================================
# 🆕 BLOCO DE ENRIQUECIMENTO DE PRODUTO (O Join Mágico)
# ==============================================================================
# 1. Tratamento de Nulos para o Join (STTO nulo vira vazio, igual na Dimensão)
df_ops_preparada = df_hoje_ajustado.fillna("", subset=["STTO"])

# 2. O Join com a Dimensão para pegar o código numérico
print("🔄 Cruzando com Dimensão Produto...")
df_enrich_produto = df_ops_preparada.join(
    df_dim_produto,
    on=["TTO", "STTO"], 
    how="left"
)

# 3. Se aparecer algum produto novo/desconhecido, vira 0
df_enrich_produto = df_enrich_produto.fillna(0, subset=["cod_produto_ia"])

# ==============================================================================
# CONTINUAÇÃO DO FLUXO NORMAL (Perfil e Cálculos)
# ==============================================================================

if df_perfil:
    df_enrich = df_enrich_produto.join(df_perfil, on="cpf_cnpj_sacado", how="left")
    
    # Preencher nulos para clientes novos (Sem histórico)
    df_enrich = df_enrich.fillna(0, subset=["exposicao_maxima_historica", "prazo_medio_historico"])
    df_enrich = df_enrich.fillna(1.0, subset=["media_pagamento_mensal"])
    
    # Calcular as features na hora (Usando os dados que vieram do perfil)
    df_enrich = df_enrich.withColumn(
        "exposicao_acumulada", 
        F.col("exposicao_maxima_historica") + F.col("vlr_total_sacado")
    ).withColumn(
        "concentracao_operacao",
        F.col("vlr_total_sacado") / F.col("exposicao_acumulada")
    ).withColumn(
        "ratio_cobertura_liquidez",
        F.col("vlr_total_sacado") / F.col("media_pagamento_mensal")
    )
else:
    # Fallback se não tiver tabela Gold (primeira execução da vida)
    df_enrich = df_enrich_produto.withColumn("exposicao_acumulada", F.col("vlr_total_sacado")) \
                             .withColumn("concentracao_operacao", F.lit(1.0)) \
                             .withColumn("ratio_cobertura_liquidez", F.lit(0.0))

# --- CONVERSÃO PARA PANDAS ---
print("2. Convertendo para Pandas para aplicar IA...")
df_pandas = df_enrich.toPandas()

# Lista de Backup (ATUALIZADA com o produto) 🆕
features_backup = [
    'vlr_total_sacado', 'prazo_medio_titulos', 'taxa', 
    'exposicao_acumulada', 'concentracao_operacao', 'qtd_titulos',
    'cod_produto_ia', 'ratio_cobertura_liquidez' # <--- Adicionado aqui!
]

# Garante que não existem nulos nas features
for col in features_backup:
    if col in df_pandas.columns:
        df_pandas[col] = df_pandas[col].fillna(0)
    else:
        # Se a coluna não existir (ex: erro no join), cria zerada para não quebrar
        df_pandas[col] = 0

# ==============================================================================
# 2. BUSCA DINÂMICA DO CÉREBRO DA V.A.I. (MLflow)
# ==============================================================================

NOME_EXPERIMENTO = "VAI_Treinamento_Semanal"
ID_EXPERIMENTO_NOVO = "8a9354d9-f2de-4a36-b684-9785a8462997"

try:
    print(f"3. Buscando inteligência mais recente em: {NOME_EXPERIMENTO}...")
    
    runs = mlflow.search_runs(
        experiment_ids=[ID_EXPERIMENTO_NOVO],
        filter_string="status = 'FINISHED'",
        order_by=["start_time DESC"],
        max_results=1
    )
    
    if len(runs) == 0:
        raise Exception("Nenhum modelo treinado encontrado.")
        
    latest_run_id = runs.iloc[0].run_id
    print(f"✅ Cérebro encontrado! Usando Run ID: {latest_run_id}")
    
    model_uri = f"runs:/{latest_run_id}/Modelo_Risco_FIDC"
    loaded_model = mlflow.sklearn.load_model(model_uri)
    
    # Aplica a IA
    if hasattr(loaded_model, "feature_names_in_"):
        cols_esperadas = loaded_model.feature_names_in_
        for col in cols_esperadas:
            if col not in df_pandas.columns:
                df_pandas[col] = 0
        df_pandas['anomaly_score'] = loaded_model.predict(df_pandas[cols_esperadas])
    else:
        df_pandas['anomaly_score'] = loaded_model.predict(df_pandas[features_backup])

    print("🚀 V.A.I. aplicada com sucesso!")

except Exception as e:
    print(f"❌ Erro/Aviso na IA: {e}")
    print("⚠️ ATENÇÃO: Usando regra manual de contingência.")
    df_pandas['anomaly_score'] = df_pandas.apply(
        lambda x: -1 if (x['taxa'] < 1.5 or x['prazo_medio_titulos'] > 60) else 1, axis=1
    )
# ==============================================================================
# 🆕 XAI: EXPLICAR O MOTIVO DA ANOMALIA (DIAGNÓSTICO)
# ==============================================================================
print("🕵️ Calculando o motivo principal das anomalias...")

features_para_analisar = [
    'vlr_total_sacado', 'prazo_medio_titulos', 'taxa',
    'concentracao_operacao', 'ratio_alavancagem_interna'
]

# Filtrar apenas colunas presentes
present_features = [col for col in features_para_analisar if col in df_pandas.columns]

if not present_features:
    # Se não houver features, fallback seguro
    df_pandas['motivo_principal'] = np.where(df_pandas['anomaly_score'] == 1, "Normal", "Desconhecido")
else:
    # 1. Calcular Z-Scores vetorizados
    df_features = df_pandas[present_features]
    means = df_features.mean()
    stds = df_features.std().replace(0, 1) # Evita divisão por zero
    
    z_scores = ((df_features - means) / stds).abs()
    
    # 2. Identificar coluna com maior desvio
    # idxmax retorna o nome da coluna com maior valor
    culpados_cols = z_scores.idxmax(axis=1)
    max_z_scores = z_scores.max(axis=1)
    
    # 3. Mapear para nomes amigáveis
    mapa_nomes = {
        'vlr_total_sacado': 'Valor Muito Alto',
        'prazo_medio_titulos': 'Prazo Fora do Comum',
        'taxa': 'Taxa Fora do Padrão',
        'concentracao_operacao': 'Concentração Excessiva',
        'ratio_alavancagem_interna': 'Alavancagem (Liquidez)'
    }
    
    culpados_friendly = culpados_cols.map(mapa_nomes).fillna(culpados_cols)

    # 4. Ajustar lógica final
    # Normal -> "Normal"
    # Anomalia mas Z-Score 0 -> "Desconhecido"
    # Anomalia com Z-Score > 0 -> Nome da feature

    # Começa com "Normal"
    df_pandas['motivo_principal'] = "Normal"

    # Máscara de anomalias
    mask_anomaly = df_pandas['anomaly_score'] != 1

    if mask_anomaly.any():
        # Define valores para linhas anômalas
        # Se max_z_score > 0, usa culpado_friendly, senão "Desconhecido"
        reasons = np.where(max_z_scores[mask_anomaly] > 0,
                           culpados_friendly[mask_anomaly],
                           "Desconhecido")

        df_pandas.loc[mask_anomaly, 'motivo_principal'] = reasons
# ==============================================================================
# 3. SALVAR RESULTADO NA TV
# ==============================================================================

df_pandas['status_ia'] = df_pandas['anomaly_score'].apply(lambda x: "ALTO RISCO" if x == -1 else "NORMAL")
df_pandas['data_processamento'] = pd.Timestamp.now()

print("4. Salvando resultados na tabela Gold...")
df_final = spark.createDataFrame(df_pandas)
df_final.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("LH_Gold.Alertas_Risco_TV")

print(f"✅ Processo Finalizado! {len(df_pandas)} operações enviadas para a TV.")
display(df_final.select("id_operacao", "status_ia","motivo_principal"))

# ==============================================================================
# 4. DASHBOARD RÁPIDO DE SAÍDA (UX)
# ==============================================================================
print("\n" + "╔" + "═"*70 + "╗")
print("║                   🤖 V.A.I. MONITORAMENTO ONLINE                     ║")
print("╚" + "═"*70 + "╝")
print(f"📅 Data: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("-" * 72)

if not df_pandas.empty and 'status_ia' in df_pandas.columns:
    total_ops = len(df_pandas)
    risco_alto = len(df_pandas[df_pandas['status_ia'] == 'ALTO RISCO'])
    percent_risco = (risco_alto / total_ops) * 100 if total_ops > 0 else 0

    # Visual Progress Bar for Risk
    bar_length = 40
    filled_length = int(bar_length * percent_risco / 100)
    bar = '█' * filled_length + '░' * (bar_length - filled_length)

    print("📊 RESUMO GERAL")
    print(f"   🔢 Total Analisado: {total_ops:,.0f} operações")
    print(f"   ✅ Aprovado (Normal): {total_ops - risco_alto:,.0f}")
    print(f"   🚨 Risco Detectado: {risco_alto:,.0f} ({percent_risco:.1f}%)")
    print(f"      [{bar}]")

    if risco_alto > 0 and 'motivo_principal' in df_pandas.columns:
        print("\n🔍 TOP 3 FATORES DE RISCO")
        top_motivos = df_pandas[df_pandas['status_ia'] == 'ALTO RISCO']['motivo_principal'].value_counts().head(3)
        for i, (motivo, count) in enumerate(top_motivos.items(), 1):
            print(f"   {i}. {motivo:<30} {count:,.0f}")
else:
    print("⚠️ Nenhum dado processado ou colunas de status ausentes.")

print("-" * 72)
print("✅ Processamento concluído.")
print("\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
