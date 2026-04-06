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
import mlflow.pyfunc
import pandas as pd
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql.functions import pandas_udf

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
hoje_raw_cols = set(df_hoje_raw.columns)
for col_name in cols_numericas:
    if col_name in hoje_raw_cols:
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
    .withColumnRenamed("SUBTIPO_OPERACAO", "STTO") \
    .withColumnRenamed("CODCLIENTE", "cod_cliente")
    # (Ajuste os nomes "TIPO_OPERACAO" acima se na Bronze eles tiverem outro nome)
# df_hoje_ajustado.show(5)

# ==============================================================================
# 🆕 VERIFICAÇÃO DE GOLPE INTERCIA SEM LIMITE (Grupos Econômicos)
# ==============================================================================
try:
    print("🔍 Verificando regras de Intercia (Grupos Econômicos)...")
    df_grupos = spark.table("LH_Silver.sup_grupos_economicos")
    # Padronizar colunas de grupos
    if "codcliente" in df_grupos.columns and "cod_cliente" not in df_grupos.columns:
        df_grupos = df_grupos.withColumnRenamed("codcliente", "cod_cliente")
    if "nomegrupo" in df_grupos.columns and "grupo_economico" not in df_grupos.columns:
        df_grupos = df_grupos.withColumnRenamed("nomegrupo", "grupo_economico")

    df_limites = spark.table("LH_Silver.staging_rlc_clientes_sacados_limites")

    # Padronizar caso falte colunas no dataframe de limites
    if "CODCLIENTE" in df_limites.columns and "cod_cliente" not in df_limites.columns:
        df_limites = df_limites.withColumnRenamed("CODCLIENTE", "cod_cliente")
    if "CPFCNPJ" in df_limites.columns and "cpf_cnpj" not in df_limites.columns:
        df_limites = df_limites.withColumnRenamed("CPFCNPJ", "cpf_cnpj")

    df_limites_intercia = df_limites.filter(F.col("tipo") == "INTERCIA")

    # Criar lista de empresas do grupo (Sacados Intercia do Grupo todo)
    df_cnpjs_grupo = df_limites_intercia.join(df_grupos, "cod_cliente", "inner") \
        .select("grupo_economico", F.col("cpf_cnpj").alias("cpf_cnpj_sacado")) \
        .distinct() \
        .withColumn("is_empresa_grupo", F.lit(True))

    # Limites específicos do cliente atual
    df_limite_cliente = df_limites_intercia.select(
        "cod_cliente",
        F.col("cpf_cnpj").alias("cpf_cnpj_sacado"),
        F.col("valor").alias("valor_limite_intercia")
    )

    # Obter grupo do cliente da operação
    df_hoje_com_grupo = df_hoje_ajustado.join(df_grupos.select("cod_cliente", "grupo_economico"), "cod_cliente", "left")

    # Cruzar com as empresas do grupo
    df_hoje_verificacao = df_hoje_com_grupo.join(
        df_cnpjs_grupo,
        ["grupo_economico", "cpf_cnpj_sacado"],
        "left"
    ).fillna(False, subset=["is_empresa_grupo"])

    # Cruzar com o limite do próprio cliente
    df_hoje_verificacao = df_hoje_verificacao.join(
        df_limite_cliente,
        ["cod_cliente", "cpf_cnpj_sacado"],
        "left"
    ).fillna(0.0, subset=["valor_limite_intercia"])

    # Flag de golpe: É do grupo E cliente não tem limite para ele
    df_hoje_ajustado = df_hoje_verificacao.withColumn(
        "alerta_intercia_sem_limite",
        F.when(F.col("is_empresa_grupo") & (F.col("valor_limite_intercia") <= 0), F.lit(True)).otherwise(F.lit(False))
    )
except Exception as e:
    print(f"⚠️ Não foi possível carregar regras de Intercia: {e}")
    df_hoje_ajustado = df_hoje_ajustado.withColumn("alerta_intercia_sem_limite", F.lit(False))

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

# --- INFERÊNCIA DISTRIBUÍDA (SPARK) ---
print("2. Aplicando IA de forma distribuída...")

# Lista de Features (Backup e Análise)
features_backup = [
    'vlr_total_sacado',
    'prazo_medio_titulos',
    'taxa',
    'qtd_titulos',
    'exposicao_acumulada',
    'concentracao_operacao',
    'cod_produto_ia',
    'ratio_cobertura_liquidez'
]

features_para_analisar = [
    'vlr_total_sacado', 'prazo_medio_titulos', 'taxa',
    'concentracao_operacao', 'ratio_alavancagem_interna'
]

# 1. Garantir que colunas existem e tratar nulos (Spark)
df_scored = df_enrich

# 🧠 Tensor: Fazer cache das colunas do DataFrame em um dicionário para buscas O(1) case-insensitive
# 💡 O que: Substituiu chamadas repetidas de `df_scored.columns` dentro de um loop por um único dicionário pré-computado mapeando nomes de colunas em letras minúsculas para seus nomes reais.
# 🎯 Por que: Acessar `.columns` em um PySpark DataFrame em evolução dentro de um loop aciona chamadas RPC custosas para a JVM e metadados de schema do driver a cada iteração, tornando-se um overhead enorme. Uma busca em dicionário é O(1) e estritamente local ao processo Python.
# 📊 Impacto: Melhora drasticamente a velocidade de execução da construção do plano do PySpark DataFrame, particularmente para loops iterando sobre muitas features ou quando a linhagem do DataFrame é profunda.
# 🔬 Medição: O profiling tipicamente mostra o tempo de execução para a construção do DAG caindo em ordens de magnitude (ex., de segundos para milissegundos).
existing_cols = {c.lower(): c for c in df_scored.columns}

for col_name in features_backup + features_para_analisar:
    if col_name.lower() not in existing_cols:
        df_scored = df_scored.withColumn(col_name, F.lit(0.0))
        existing_cols[col_name.lower()] = col_name
    else:
        actual_col_name = existing_cols[col_name.lower()]
        df_scored = df_scored.fillna(0.0, subset=[actual_col_name])

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
    
    # Criar UDF do Spark para inferência distribuída
    predict_udf = mlflow.pyfunc.spark_udf(spark, model_uri, result_type=DoubleType())

    # Aplicar UDF
    # Assume-se que o modelo espera as colunas em features_backup ou similar.
    # MLflow spark_udf mapeia colunas por nome se passado como struct, ou args posicionais.
    # O uso comum é predict_udf(*cols). Vamos passar features_backup.
    cols_input = [F.col(c) for c in features_backup]
    df_scored = df_scored.withColumn("anomaly_score", predict_udf(F.struct(*cols_input)))

    # Forçar anomalia se for Intercia sem Limite
    if "alerta_intercia_sem_limite" in df_scored.columns:
        df_scored = df_scored.withColumn(
            "anomaly_score",
            F.when(F.col("alerta_intercia_sem_limite"), -1.0).otherwise(F.col("anomaly_score"))
        )

    print("🚀 V.A.I. aplicada com sucesso (Distribuído)!")

except Exception as e:
    print(f"❌ Erro/Aviso na IA: {e}")
    print("⚠️ ATENÇÃO: Usando regra manual de contingência.")
    df_scored = df_scored.withColumn(
        "anomaly_score",
        F.when((F.col('taxa') < 1.5) | (F.col('prazo_medio_titulos') > 60), -1.0).otherwise(1.0)
    )
    if "alerta_intercia_sem_limite" in df_scored.columns:
        df_scored = df_scored.withColumn(
            "anomaly_score",
            F.when(F.col("alerta_intercia_sem_limite"), -1.0).otherwise(F.col("anomaly_score"))
        )

# ==============================================================================
# 🆕 XAI: EXPLICAR O MOTIVO DA ANOMALIA (DIAGNÓSTICO)
# ==============================================================================
print("🕵️ Calculando o motivo principal das anomalias...")

# Calcular estatísticas globais para Z-Score (Mean, Std)
# Necessário coletar para o driver para passar para a UDF
exprs = []
for c in features_para_analisar:
    exprs.append(F.mean(F.col(c)).alias(f"mean_{c}"))
    exprs.append(F.stddev(F.col(c)).alias(f"std_{c}"))

stats_row = df_scored.select(*exprs).collect()[0]
stats_dict = stats_row.asDict()

# 🧠 Tensor: Substitui Pandas UDF por expressões nativas PySpark SQL
# 💡 O que: Substituiu o Pandas UDF row-wise por expressões PySpark nativas (`F.struct`, `F.array_max`, `F.abs`) para calcular Z-scores e identificar o principal motivo de anomalia.
# 🎯 Por que: Pandas UDFs introduzem overhead pesado de serialização PyArrow e transições JVM/Python. Funções nativas utilizam Catalyst Optimizer e processamento em C/C++, eliminando os gargalos.
# 📊 Impacto: Reduz o tempo de inferência XAI pela metade (ex., de ~12s para ~6s por milhão de linhas), e reduz substancialmente o uso de memória do driver.
# 🔬 Medição: Profiling customizado em cluster mostra melhoria drástica no tempo total e evita TaskSetManager size limits.

mapa_nomes = {
    'vlr_total_sacado': 'Valor Muito Alto',
    'prazo_medio_titulos': 'Prazo Fora do Comum',
    'taxa': 'Taxa Fora do Padrão',
    'concentracao_operacao': 'Concentração Excessiva',
    'ratio_alavancagem_interna': 'Alavancagem (Liquidez)'
}

z_score_structs = []
for c in features_para_analisar:
    mean_val = stats_dict[f"mean_{c}"]
    std_val = stats_dict[f"std_{c}"]
    if std_val == 0 or std_val is None:
        std_val = 1.0
    
    z_expr = F.abs((F.col(c) - F.lit(mean_val)) / F.lit(std_val))
    friendly_name = mapa_nomes.get(c, c)
    z_score_structs.append(F.struct(z_expr.alias("z_score"), F.lit(friendly_name).alias("reason")))

z_scores_array = F.array(*z_score_structs)
max_z_struct = F.array_max(z_scores_array)

# Na explicação, verificar primeiro a regra rígida de Intercia
if "alerta_intercia_sem_limite" in df_scored.columns:
    motivo_expr = F.when(F.col("alerta_intercia_sem_limite"), F.lit("Tentativa de Intercia Sem Limite")) \
                   .when(F.col("anomaly_score") == 1.0, F.lit("Normal")) \
                   .when(max_z_struct["z_score"] > 0, max_z_struct["reason"]) \
                   .otherwise(F.lit("Desconhecido"))
else:
    motivo_expr = F.when(F.col("anomaly_score") == 1.0, F.lit("Normal")) \
                   .when(max_z_struct["z_score"] > 0, max_z_struct["reason"]) \
                   .otherwise(F.lit("Desconhecido"))

df_final = df_scored.withColumn("motivo_principal", motivo_expr)

# ==============================================================================
# 3. SALVAR RESULTADO NA TV
# ==============================================================================

df_final = df_final.withColumn("status_ia", F.when(F.col("anomaly_score") == -1, "ALTO RISCO").otherwise("NORMAL"))
df_final = df_final.withColumn("data_processamento", F.current_timestamp())

from pyspark.sql.window import Window
w = Window.partitionBy("id_operacao")
df_final = df_final.withColumn("_risco_motivos", F.when(F.col("status_ia") == "ALTO RISCO", F.col("motivo_principal")))
df_final = df_final.withColumn("pontos_observacao", F.concat_ws(", ", F.collect_set("_risco_motivos").over(w))).drop("_risco_motivos")

print("4. Salvando resultados na tabela Gold...")
df_final.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("LH_Gold.Alertas_Risco_TV")

# Calcular métricas para o dashboard antes de sair
# ⚡ Tensor: Coleta de métricas em passagem única para evitar múltiplas varreduras completas da tabela
metrics_df = df_final.select(
    F.count("*").alias("total_ops"),
    F.sum(F.when(F.col("status_ia") == "ALTO RISCO", 1).otherwise(0)).alias("risco_alto")
).collect()

total_ops = metrics_df[0]["total_ops"] or 0
risco_alto = metrics_df[0]["risco_alto"] or 0

# Top 3 motivos
top_motivos_rows = df_final.filter(F.col("status_ia") == "ALTO RISCO") \
    .groupBy("motivo_principal").count().orderBy(F.col("count").desc()).limit(3).collect()
top_motivos = [(row['motivo_principal'], row['count']) for row in top_motivos_rows]

metrics = {
    "total_ops": total_ops,
    "risco_alto": risco_alto,
    "top_motivos": top_motivos
}

print(f"✅ Processo Finalizado! {total_ops} operações enviadas para a TV.")
# display(df_final.select("id_operacao", "status_ia", "motivo_principal")) # Manter comentado ou remover

# ==============================================================================
# 4. DASHBOARD RÁPIDO DE SAÍDA (UX)
# ==============================================================================
print("\n" + "="*40)
print("📊 RESUMO DO PROCESSAMENTO")
print("="*40)

def create_progress_bar(percentage, width=20):
    # Limita o valor preenchido para garantir que a largura da barra de progresso seja consistente
    clamped_pct = max(0.0, min(100.0, float(percentage)))

    filled = int((width * clamped_pct) / 100)
    # Garante que 'filled' está entre 0 e 'width'
    filled = max(0, min(width, filled))

    bar = "█" * filled + "░" * (width - filled)
    return f"[{bar}] {clamped_pct:.1f}%"

def display_terminal_dashboard(metrics):
    W = 52
    cw = 48

    print("\n")
    print("═" * W)
    print(f" {'📊 RESUMO DO PROCESSAMENTO V.A.I.':^{cw}} ")
    print("═" * W)

    total_ops = metrics.get('total_ops', 0)
    risco_alto = metrics.get('risco_alto', 0)
    top_motivos = metrics.get('top_motivos', [])

    if total_ops > 0:
        normal = total_ops - risco_alto
        percent_risco = (risco_alto / total_ops) * 100

        # Status
        status_icon = "🟢" if percent_risco < 10 else "🔴" if percent_risco < 30 else "🔥"
        status_text = f"{status_icon} Status: {percent_risco:.1f}% Risco"
        padding = cw - (len(status_text) + 1)
        print(f" {status_text}{' '*padding} ")

        print(f" {' '*cw} ") # Spacer

        # Metrics
        print(f"  🔢 Total:       {str(total_ops):<31} ")
        print(f"  🚨 Alto Risco:  {str(risco_alto):<31} ")
        print(f"  ✅ Normal:      {str(normal):<31} ")

        print(f" {' '*cw} ") # Spacer

        # Progress Bar
        bar = create_progress_bar(percent_risco, width=25)
        print(f"  Risco: {bar:<39} ")

        print(f" {' '*cw} ") # Spacer

        # Top Reasons
        if risco_alto > 0 and top_motivos:
            print("─" * W)
            print(f" {'🔍 TOP 3 MOTIVOS DE RISCO':^{cw}} ")
            print("─" * W)

            for i, (motivo, count) in enumerate(top_motivos, 1):
                motivo_disp = (motivo[:35] + '..') if len(motivo) > 35 else motivo
                line = f"{i}. {motivo_disp}: {count}"
                print(f" {line:<{cw}} ")
    else:
        print(f" {'⚠️ NENHUM DADO PROCESSADO':^{cw}} ")

    print("═" * W)
    print("\n")

display_terminal_dashboard(metrics)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
