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
# META           "id": "8f85c372-56ad-4f3f-acf9-3be2e9b99513"
# META         },
# META         {
# META           "id": "ee40705b-0100-49bc-8f35-81d71839f042"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Notebook de Curadoria da Camada Gold (Otimizado)
# **Objetivo:** Aplicar regras de negócio, realizar joins e criar os modelos dimensionais (Fatos e Dimensões) na camada **Gold**.
# **Refatoração:** Este notebook consome dados tratados da camada **Silver**. Dimensões independentes (Gerentes, Sacados, Esteira) foram extraídas para notebooks dedicados.

# MARKDOWN ********************

# ## Seção 0: Configuração e Leitura de Dados (Silver Source)

# CELL ********************

# Célula 0.1: Configuração da Sessão Spark
# ------------------------------------
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

from pyspark.sql.window import Window
from pyspark.sql.functions import (
    row_number, col, when, lit, concat, length, regexp_replace,
    collect_list, concat_ws, upper, greatest, substring, year,
    lead, date_add, lag, max, coalesce, broadcast, dayofweek, dayofmonth, date_sub, trim, to_date,
    datediff, sum, min, count, round, floor, least, current_date, split, pow, xxhash64
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, DoubleType, DateType, BooleanType
from delta.tables import *
import datetime

def safe_read_table(spark, table_name, schema=None, fallback_df=None):
    """
    Tenta ler uma tabela. Se falhar, retorna um DataFrame vazio com o schema fornecido
    ou um DataFrame de fallback.
    """
    try:
        return spark.read.table(table_name)
    except Exception as e:
        print(f"AVISO: Tabela {table_name} não encontrada ({e}).")
        if fallback_df is not None:
             print("Usando dataframe de fallback.")
             return fallback_df
        elif schema is not None:
             print("Criando dataframe vazio com schema fornecido.")
             return spark.createDataFrame([], schema=schema)
        else:
             raise e

def transform_esteira_dates(df_esteira, status_mapping):
    """
    Optimized transformation to get Max and Min dates per status in a single pass.
    Returns (df_pivot_max, df_pivot_min).
    """
    expected_status = list(status_mapping.keys())

    # Combined Pivot (Max and Min)
    df_combined = df_esteira \
        .groupBy("cod_cliente") \
        .pivot("status_do_cliente", expected_status) \
        .agg(
            max("datalog").alias("max"),
            min("datalog").alias("min")
        )

    # Separate and Rename
    # 1. Max Dates (df_esteira_pivot)
    # Expected cols: cod_cliente, pivot_checklist, pivot_assinatura...
    select_max = [col("cod_cliente")]
    for status, clean_name in status_mapping.items():
        col_name = f"{status}_max"
        select_max.append(col(col_name).alias(f"pivot_{clean_name}"))

    df_max = df_combined.select(select_max)

    # 2. Min Dates (df_esteira_min)
    # Expected cols: cod_cliente, CHECKLIST, ASSINATURA...
    select_min = [col("cod_cliente")]
    for status in expected_status:
        col_name = f"{status}_min"
        select_min.append(col(col_name).alias(status))

    df_min = df_combined.select(select_min)

    return df_max, df_min

def deduplicate_clientes_staging(df_base_raw):
    """
    Deduplicates customer staging data by CPF/CNPJ, keeping the most recent record
    based on 'data_inclusao' and 'cod_cliente'.
    """
    w_dedup = Window.partitionBy("cpf_cnpj").orderBy(col("data_inclusao").desc(), col("cod_cliente").desc())
    return df_base_raw.withColumn("rn", row_number().over(w_dedup)).filter(col("rn") == 1).drop("rn")

def calculate_vop_metrics(df_ops_validas):
    """
    Calculates VOP metrics (Top Day of Week and Top Day of Month) for each client.
    Optimized to reuse existing date columns in df_ops_validas.
    """
    # VOP por Dia da Semana (Top 1)
    # Reusing 'dia_da_semana_da_operacao' (1=Sun, 2=Mon...) calculated in Section 1.2
    # Note: 'dia_da_semana_da_operacao' is derived from 'data_deferimento' which is to_date('data_analise').
    # So it is functionally equivalent to dayofweek('data_analise').
    df_vop_semana = df_ops_validas.groupBy("cod_cliente", col("dia_da_semana_da_operacao").alias("dia_semana")) \
        .agg(sum("valor_de_face").alias("vop"))

    w_rank_semana = Window.partitionBy("cod_cliente").orderBy(col("vop").desc())
    df_dia_semana_top = df_vop_semana.withColumn("rn", row_number().over(w_rank_semana)).filter(col("rn") == 1) \
        .select(col("cod_cliente"), col("dia_semana").alias("dia_semana_mais_vop"))

    # VOP por Dia do Mês (Top 1)
    # Reusing 'dia_da_operacao' (Day of Month) calculated in Section 1.2
    # Note: 'dia_da_operacao' is derived from 'data_deferimento' (to_date('data_analise')).
    df_vop_mes = df_ops_validas.groupBy("cod_cliente", col("dia_da_operacao").alias("dia_mes")) \
        .agg(sum("valor_de_face").alias("vop"))

    w_rank_mes = Window.partitionBy("cod_cliente").orderBy(col("vop").desc())
    df_dia_mes_top = df_vop_mes.withColumn("rn", row_number().over(w_rank_mes)).filter(col("rn") == 1) \
        .select(col("cod_cliente"), col("dia_mes").alias("dia_mes_mais_vop"))

    return df_dia_semana_top, df_dia_mes_top

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 0.0: Configurações e Constantes Gerais
# -------------------------------------------

class TableNames:
    # Bronze
    BRONZE_CAD_CLIENTES = "LH_Bronze.cad_clientes"
    BRONZE_CAD_GERAL_ARQUIVOS = "LH_Bronze.cad_geral_arquivos"
    BRONZE_TAB_FERIADOS = "LH_Bronze.tab_feriados"
    BRONZE_TAB_SUBTIPOOPERACAO = "LH_Bronze.tab_subtipooperacao"
    BRONZE_TAB_TIPOOPERACAO = "LH_Bronze.tab_tipooperacao"

    # Silver
    SILVER_BRIDGE_CLIENTE_GERENTE = "LH_Silver.bridge_cliente_gerente"
    SILVER_FACT_ULTIMA_CONFIRMACAO = "LH_Silver.fact_ultima_confirmacao"
    SILVER_RELATORIO_TITULOS_JURIDICO = "LH_Silver.relatorio_titulos_juridico"
    SILVER_STAGING_BAIXAS_LIMPA = "LH_Silver.staging_baixas_limpa"
    SILVER_STAGING_BOLETOS_TITULOS = "LH_Silver.staging_boletos_titulos"
    SILVER_STAGING_CAD_GERAL_PF_PJ_LIMPA = "LH_Silver.staging_cad_geral_pf_pj_limpa"
    SILVER_STAGING_CLIENTES_LIMPA = "LH_Silver.staging_clientes_limpa"
    SILVER_STAGING_CONTRATOS_CLIENTES_LIMPA = "LH_Silver.staging_contratos_clientes_limpa"
    SILVER_STAGING_EMAILS_AGG = "LH_Silver.staging_emails_agg"
    SILVER_STAGING_ENDERECOS_LIMPA = "LH_Silver.staging_enderecos_limpa"
    SILVER_STAGING_ESTUDO_OPERACOES = "LH_Silver.staging_estudo_operacoes"
    SILVER_STAGING_GERENTES = "LH_Silver.staging_gerentes"
    SILVER_STAGING_OPERACOES_DEVOLUCOES_LIMPA = "LH_Silver.staging_operacoes_devolucoes_limpa"
    SILVER_STAGING_OPERACOES_ESCROW = "LH_Silver.staging_operacoes_escrow"
    SILVER_STAGING_OPERACOES_LIMPA = "LH_Silver.staging_operacoes_limpa"
    SILVER_STAGING_OPERACOES_PRORROGACAO_LIMPA = "LH_Silver.staging_operacoes_prorrogacao_limpa"
    SILVER_STAGING_PLATAFORMAS = "LH_Silver.staging_plataformas"
    SILVER_STAGING_PROTESTOS = "LH_Silver.staging_protestos"
    SILVER_STAGING_RLC_CLIENTES_SACADOS_LIMITES = "LH_Silver.staging_rlc_clientes_sacados_limites"
    SILVER_STAGING_TARIFAS_ESPORADICAS = "LH_Silver.staging_tarifas_esporadicas"
    SILVER_STAGING_TELEFONES_AGG = "LH_Silver.staging_telefones_agg"
    SILVER_STAGING_TITULOS_LIMPA = "LH_Silver.staging_titulos_limpa"
    SILVER_STAGING_USUARIOS = "LH_Silver.staging_usuarios"
    SILVER_STG_LIMITES_CONTRATOS_SILVER = "LH_Silver.stg_limites_contratos_silver"
    SILVER_SUP_FORMA_DE_PAGAMENTO = "LH_Silver.sup_forma_de_pagamento"
    SILVER_SUP_GRUPOS_ECONOMICOS = "LH_Silver.sup_grupos_economicos"
    SILVER_SUP_LIMITES_EXTRA_PLUS = "LH_Silver.sup_limites_extra_plus"
    SILVER_SUP_MOTIVO_BAIXA = "LH_Silver.sup_motivo_baixa"
    SILVER_SUP_MOTIVOS_DE_INDEFERIMENTO = "LH_Silver.sup_motivos_de_indeferimento"
    SILVER_SUP_PAGO_PELO = "LH_Silver.sup_pago_pelo"
    SILVER_SUP_STATUS_DE_CLIENTES_DA_ESTEIRA = "LH_Silver.sup_status_de_clientes_da_esteira"
    SILVER_SUP_TIPO_DE_BAIXA = "LH_Silver.sup_tipo_de_baixa"

    # Gold
    GOLD_ANALISE_PRAZOS_ESTEIRA = "LH_Gold.analise_prazos_esteira"
    GOLD_ANALISE_SCORE_CLIENTES = "LH_Gold.analise_score_clientes"
    GOLD_DIM_CALENDARIO = "LH_Gold.dim_calendario"
    GOLD_DIM_CLIENTES = "LH_Gold.dim_clientes"
    GOLD_DIM_PRODUTOS = "LH_Gold.dim_produtos"
    GOLD_ESTEIRA_DE_PROPOSTAS = "LH_Gold.esteira_de_propostas"
    GOLD_FATO_BAIXAS = "LH_Gold.fato_baixas"
    GOLD_FATO_LIMITES_CREDITO = "LH_Gold.fato_limites_credito"
    GOLD_FATO_OPERACOES = "LH_Gold.fato_operacoes"
    GOLD_FATO_OPERACOES_PRORROGACAO = "LH_Gold.fato_operacoes_prorrogacao"
    GOLD_FATO_OPERACOES_RECOMPRA = "LH_Gold.fato_operacoes_recompra"
    GOLD_FATO_PRORROGACOES_DE_TITULOS = "LH_Gold.fato_prorrogacoes_de_titulos"
    GOLD_FATO_TARIFAS_ESPORADICAS = "LH_Gold.fato_tarifas_esporadicas"
    GOLD_FATO_TITULOS = "LH_Gold.fato_titulos"
    GOLD_METRICAS_CARTEIRA_HHI = "LH_Gold.metricas_carteira_hhi"


# Colunas a serem removidas na construção da fato_operacoes_prorrogacao (TTO='PR')
COLS_TO_REMOVE_PR = [
    "stto", "taxa", "tarifa", "tarifa_recompra", "tac", "n_docs", "n_docs_recompra",
    "valor_advalorem", "valor_taxa_adm", "total_de_tarifas", "data_alteracao", "cod_indeferimento",
    "data_aceite", "data_envio_email", "aprovacao1", "contrato_fisico", "taxa_cadastro"
]

# Colunas a serem removidas na construção da fato_prorrogacoes_de_titulos
COLS_TO_REMOVE_PRORROGACAO = [
    "tarifa", "usuainclusao", "dataalteracao", "usuaalteracao", "valordevido", "valorpror", "valorboleto"
]

# Mapeamento de Status da Esteira para nomes limpos (usado na Seção 6.3)
STATUS_ESTEIRA_MAPPING = {
    "CHECKLIST": "checklist", "ASSINATURA": "assinatura", "COMITE": "comite",
    "CONCLUIDO": "concluido", "BIZAGI": "bizagi", "RENOVAÇÃO": "renovacao",
    "RESERVA": "reserva", "START": "start", "CREDITO": "credito",
    "PROPOSTA": "proposta", "REVISÃO COMERCIAL": "revisao_comercial",
    "DIR COMERCIAL": "dir_comercial"
}

def calculate_funnel_dates(df):
    """
    Calculates derived dates for the funnel (Approval, Conclusion, Comite, Reserva, Entrada).
    """
    return df \
        .withColumn("data_aprovacao", greatest(col("pivot_checklist"), col("pivot_assinatura"))) \
        .withColumn("data_conclusao", coalesce(col("pivot_bizagi"), col("pivot_concluido"))) \
        .withColumn("data_comite", col("pivot_comite")) \
        .withColumn("data_reserva", greatest(col("pivot_renovacao"), col("pivot_reserva"))) \
        .withColumn("data_entrada", coalesce(
            greatest(col("pivot_dir_comercial"), col("pivot_proposta"), col("pivot_revisao_comercial")),
            col("data_comite")
        ))

def get_status_risco_expr(col_tto="tto", col_vencimento="data_vencimento_util", current_date_col=None):
    """
    Retorna a expressão Column para cálculo de status_risco.
    Lógica:
      - CRÍTICO: TTO='RN' e Vencimento < Hoje
      - ATENÇÃO: Vencimento < Hoje (e não CRÍTICO)
      - NO PRAZO: Caso contrário
    """
    if current_date_col is None:
        current_date_col = current_date()

    return when((col(col_tto) == "RN") & (col(col_vencimento) < current_date_col), "CRÍTICO") \
           .when(col(col_vencimento) < current_date_col, "ATENÇÃO") \
           .otherwise("NO PRAZO")

def check_incremental_gold(spark):
    """
    Checks if new data exists in Silver layer compared to Gold layer.
    If no new data, exits the notebook.
    """
    try:
        # Silver Tables (Source)
        source_ops = TableNames.SILVER_STAGING_OPERACOES_LIMPA
        source_titulos = TableNames.SILVER_STAGING_TITULOS_LIMPA

        # Gold Tables (Target)
        target_ops = TableNames.GOLD_FATO_OPERACOES
        target_titulos = TableNames.GOLD_FATO_TITULOS

        def get_max_date(table_name, col_name="data_inclusao"):
            try:
                df = spark.read.table(table_name)
                # Check column existence case-insensitive
                cols = [c.lower() for c in df.columns]
                if col_name.lower() not in cols:
                    return None
                # Use the actual column name from df.columns to avoid AnalysisException
                actual_col = [c for c in df.columns if c.lower() == col_name.lower()][0]
                row = df.agg(max(col(actual_col))).collect()[0]
                return row[0]
            except Exception as e:
                # print(f"Warning reading {table_name}: {e}")
                return None

        # Check Ops
        max_silver_ops = get_max_date(source_ops)
        max_gold_ops = get_max_date(target_ops)

        # Check Titulos
        max_silver_titulos = get_max_date(source_titulos)
        max_gold_titulos = get_max_date(target_titulos)

        print(f"Max Date Ops - Silver: {max_silver_ops}, Gold: {max_gold_ops}")
        print(f"Max Date Titulos - Silver: {max_silver_titulos}, Gold: {max_gold_titulos}")

        new_ops = False
        if max_silver_ops:
            # If Gold is None (First Run) or Silver > Gold, we have new data
            if not max_gold_ops or max_silver_ops > max_gold_ops:
                new_ops = True

        new_titulos = False
        if max_silver_titulos:
            if not max_gold_titulos or max_silver_titulos > max_gold_titulos:
                new_titulos = True

        # If both checks failed to find new data (and sources exist), skip.
        # If sources don't exist (max_silver is None), we probably can't run anyway, but let's be safe and proceed (it will likely fail later or handle empty).
        # Actually, if sources are None, we should probably SKIP or Proceed?
        # If Proceed, we hit errors later. If Skip, we save time.
        # Let's assume if source exists and no new data, we SKIP.

        if (max_silver_ops or max_silver_titulos) and (not new_ops and not new_titulos):
            print("Nenhum dado novo detectado em Operações ou Títulos (Silver vs Gold). Pulando execução Gold.")
            from notebookutils import mssparkutils
            mssparkutils.notebook.exit("Skipped")
        else:
            print("Novos dados detectados ou carga inicial. Prosseguindo com a execução Gold.")

    except Exception as e:
        print(f"Erro na verificação incremental: {e}. Prosseguindo por segurança.")

# Execute Incremental Check
check_incremental_gold(spark)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 5.2: Construção da Fato Tarifas Esporádicas
# ---------------------------------------------------
print("\nIniciando construção da fato_tarifas_esporadicas...")

df_tarifas_silver = spark.read.table(TableNames.SILVER_STAGING_TARIFAS_ESPORADICAS)

# Salvar
target_fato_tarifas = TableNames.GOLD_FATO_TARIFAS_ESPORADICAS
df_tarifas_silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_fato_tarifas)
print(f"Tabela '{target_fato_tarifas}' criada com sucesso.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 0.2: Leitura das Tabelas Preparadas (Silver)
# ----------------------------------------------------------------
print("Iniciando leitura da Silver...")

# --- 1. Titulos (Origem: LH_Silver.staging_titulos_limpa) ---
print("Carregando Titulos (Silver)...")
df_titulos_limpa = spark.read.table(TableNames.SILVER_STAGING_TITULOS_LIMPA).cache()
# A tabela já está limpa, desduplicada e com colunas renomeadas para snake_case.

# --- 2. Operacoes (Origem: LH_Silver.staging_operacoes_limpa) ---
print("Carregando Operacoes (Silver)...")
df_operacoes_limpa = spark.read.table(TableNames.SILVER_STAGING_OPERACOES_LIMPA)

# --- 3. Baixas (Origem: LH_Silver.staging_baixas_limpa) ---
print("Carregando Baixas (Silver)...")
df_baixas_staging = spark.read.table(TableNames.SILVER_STAGING_BAIXAS_LIMPA)

# --- 4. Cadastros (Origem: LH_Silver...) ---
print("Carregando Cadastros (Silver)...")
# Clientes
df_clientes_staging = spark.read.table(TableNames.SILVER_STAGING_CLIENTES_LIMPA) # cod_cliente, cpf_cnpj

# Geral PF/PJ
df_geral_pf_pj_limpa = spark.read.table(TableNames.SILVER_STAGING_CAD_GERAL_PF_PJ_LIMPA) # cpf_cnpj, nome, razao_social, nome_fantasia

# Endereços
df_enderecos_limpa = spark.read.table(TableNames.SILVER_STAGING_ENDERECOS_LIMPA).select(
    col("cpf_cnpj"), col("cidade"), col("uf"), col("cep")
)

# Bridge Gerente
df_bridge_gerente = spark.read.table(TableNames.SILVER_BRIDGE_CLIENTE_GERENTE)

# Gerentes e Plataformas
print("Carregando Gerentes e Plataformas (Silver)...")
df_gerentes = spark.read.table(TableNames.SILVER_STAGING_GERENTES)
df_plataformas = spark.read.table(TableNames.SILVER_STAGING_PLATAFORMAS)

# Emails & Telefones Agg
print("Carregando Emails e Telefones (Silver)...")
df_emails_agg = spark.read.table(TableNames.SILVER_STAGING_EMAILS_AGG)
df_telefones_agg = spark.read.table(TableNames.SILVER_STAGING_TELEFONES_AGG)

# --- 5. Support Tables ---
print("Carregando Tabelas de Suporte (Silver)...")
df_dim_pago_por = spark.read.table(TableNames.SILVER_SUP_PAGO_PELO)
df_dim_forma_pagamento = spark.read.table(TableNames.SILVER_SUP_FORMA_DE_PAGAMENTO)
df_dim_tipo_taxa = spark.read.table(TableNames.SILVER_SUP_TIPO_DE_BAIXA)
df_dim_motivo_baixa = spark.read.table(TableNames.SILVER_SUP_MOTIVO_BAIXA)

# --- 6. Other Lookups ---
print("Carregando Lookups (Bronze)...")
df_cad_geral_arquivos = spark.read.table(TableNames.BRONZE_CAD_GERAL_ARQUIVOS)
df_tipo_op_bronze = spark.read.table(TableNames.BRONZE_TAB_TIPOOPERACAO)
df_subtipo_op_bronze = spark.read.table(TableNames.BRONZE_TAB_SUBTIPOOPERACAO)
df_feriados = spark.read.table(TableNames.BRONZE_TAB_FERIADOS)

# Limites (Silver)
print("Carregando Limites (Silver)...")
df_limites = spark.read.table(TableNames.SILVER_STAGING_RLC_CLIENTES_SACADOS_LIMITES)

# Devolucoes (Silver)
print("Carregando Devolucoes (Silver)...")
df_devolucoes = spark.read.table(TableNames.SILVER_STAGING_OPERACOES_DEVOLUCOES_LIMPA)

# Protestos (Silver)
print("Carregando Protestos (Silver)...")
df_protestos = spark.read.table(TableNames.SILVER_STAGING_PROTESTOS)

print("Carregando Ultima Confirmacao (Silver)...")
df_ultima_conf = spark.read.table(TableNames.SILVER_FACT_ULTIMA_CONFIRMACAO)

# Calendario (Gold)
print("Carregando Calendario (Gold)...")
df_dim_calendario = spark.read.table(TableNames.GOLD_DIM_CALENDARIO).cache()

# Contratos (Silver) - Para Limites
print("Carregando Contratos (Silver)...")
df_contratos = spark.read.table(TableNames.SILVER_STAGING_CONTRATOS_CLIENTES_LIMPA)

# Limites Contratos Silver (Regex) - Para colunas faltantes
print("Carregando Limites Contratos Silver (Regex)...")
df_limites_obs_silver = safe_read_table(spark, TableNames.SILVER_STG_LIMITES_CONTRATOS_SILVER, schema=StructType([
    StructField("codcliente", LongType(), True),
    StructField("limite_geral", DoubleType(), True),
    StructField("limite_intercompany", DoubleType(), True),
    StructField("limite_extra_desconto_formal", DoubleType(), True),
    StructField("limite_extra_desconto_informal", DoubleType(), True)
]))

# Cad Clientes (Bronze) - Para Status
print("Carregando Cad Clientes (Bronze)...")
df_cad_clientes_bronze = spark.read.table(TableNames.BRONZE_CAD_CLIENTES)

# Dimensao Produtos (Gold) - Refatorado
print("Carregando Dimensao Produtos (Gold)...")
df_dim_produto = safe_read_table(spark, TableNames.GOLD_DIM_PRODUTOS, schema=StructType([
    StructField("sk_produto", LongType(), True),
    StructField("chave_produto", StringType(), True),
    StructField("produto_informacao_de_mercado", StringType(), True)
])).cache()

# Grupos Economicos (Silver)
print("Carregando Grupos Economicos (Silver)...")
df_grupos_economicos = spark.read.table(TableNames.SILVER_SUP_GRUPOS_ECONOMICOS)

# Limites Extra Plus (Silver)
print("Carregando Limites Extra Plus (Silver)...")
df_limites_extra_plus = safe_read_table(spark, TableNames.SILVER_SUP_LIMITES_EXTRA_PLUS, schema=StructType([
    StructField("nome", StringType(), True),
    StructField("cnpj", StringType(), True),
    StructField("limite", DoubleType(), True),
    StructField("limite_extra", DoubleType(), True),
    StructField("limite_plus", DoubleType(), True)
]))

# Relatorio Juridico (Silver) - Para flag status_enviado_juridico
print("Carregando Relatorio Juridico (Silver)...")
df_relatorio_juridico = safe_read_table(spark, TableNames.SILVER_RELATORIO_TITULOS_JURIDICO, schema=StructType([
    StructField("cod_titulo", LongType(), True)
]))

# Usuarios (Silver)
print("Carregando Usuarios (Silver)...")
df_usuarios = spark.read.table(TableNames.SILVER_STAGING_USUARIOS)

# Motivos Indeferimento (Silver)
print("Carregando Motivos Indeferimento (Silver)...")
df_motivos_indeferimento = safe_read_table(spark, TableNames.SILVER_SUP_MOTIVOS_DE_INDEFERIMENTO, schema=StructType([
    StructField("cod_indeferimento", LongType(), True),
    StructField("motivo_indeferimento", StringType(), True),
    StructField("grupo_motivo_indeferimento", StringType(), True)
]))

# Estudo Operacoes (Silver)
print("Carregando Estudo Operacoes (Silver)...")
df_estudo_operacoes = spark.read.table(TableNames.SILVER_STAGING_ESTUDO_OPERACOES)

# Escrow (Silver)
print("Carregando Escrow (Silver)...")
try:
    df_escrow = spark.read.table(TableNames.SILVER_STAGING_OPERACOES_ESCROW).groupBy("cod_operacao").agg(max("ESCROW").alias("ESCROW"))
except Exception as e:
    print(f"AVISO: Tabela {TableNames.SILVER_STAGING_OPERACOES_ESCROW} não encontrada ({e}). Criando dataframe vazio.")
    df_escrow = spark.createDataFrame([], schema=StructType([StructField("cod_operacao", LongType(), True), StructField("ESCROW", BooleanType(), True)]))

print("Leitura da Silver concluída.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 1: Geração de DataFrames Intermediários (Enriquecimento)
# **Objetivo:** Criar as visões enriquecidas (`cad_geral` e `operacoes`) em memória.

# CELL ********************

# Célula 1.1: Cadastro Geral Enriquecido
# -----------------------------------------------------------------
print("Criando DataFrame intermediário: Cadastro Geral Enriquecido...")
df_cad_geral_enriquecido = df_geral_pf_pj_limpa \
    .join(df_enderecos_limpa, on="cpf_cnpj", how="left") \
    .join(df_emails_agg, on="cpf_cnpj", how="left") \
    .join(df_telefones_agg, on="cpf_cnpj", how="left")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 1.2: Operações Enriquecidas
# -----------------------------------------------------------
print("Criando DataFrame intermediário: Operações Enriquecidas...")
from pyspark.sql.functions import unix_timestamp, ceil, abs, hour, month, weekofyear, dayofmonth, last_day, months_between, floor, trunc

# PRE-CALCULO: Data Primeira Operação por Cliente (para Meses de Idade)
df_first_op = df_operacoes_limpa.filter(col("status_aceite") == 'A') \
    .groupBy("cod_cliente").agg(min("data_analise").alias("data_primeira_operacao_calc"))

# PRE-CALCULO: Taxa Cadastro do Cliente (do Contrato Ativo)
# ⚡ Bolt Optimization: Cache this dataframe to reuse it later (in Section 6.5) avoiding a redundant scan.
df_client_rate = df_contratos.filter(col("status") == 'A') \
    .groupBy("cod_cliente").agg(max("fator").alias("taxa_cadastro_cliente")).cache()

# PRE-CALCULO: Gerente Enriquecido (Nome e Comissão)
# df_gerentes tem cod_broker, cod_usuario, taxa_comissao (added in Silver Prep)
# df_usuarios tem cod_usuario, nome
# Refatorado para incluir fallback de nome via Cadastro Geral (Fix Broker 7/71)
df_gerentes_alias = df_gerentes.alias("g")
df_usuarios_alias = df_usuarios.alias("u")
df_geral_alias = df_geral_pf_pj_limpa.alias("cad")

# Join com Usuarios
df_join_users = df_gerentes_alias.join(df_usuarios_alias, col("g.cod_usuario") == col("u.cod_usuario"), "left")

# Clean CPF/CNPJ for Join
df_join_users_clean = df_join_users.withColumn("cpf_cnpj_clean", regexp_replace(col("g.cpf_cnpj"), "[^0-9]", ""))
df_geral_clean = df_geral_alias.withColumn("cpf_cnpj_clean", regexp_replace(col("cad.cpf_cnpj"), "[^0-9]", ""))

# Join Fallback
df_gerentes_full = df_join_users_clean.join(
    df_geral_clean.select(col("cpf_cnpj_clean"), col("cad.nome").alias("nome_geral")),
    "cpf_cnpj_clean",
    "left"
)

# Join with Plataformas to get Platform Info
df_plataformas_alias = df_plataformas.alias("plat")

df_gerentes_plat = df_gerentes_full.join(df_plataformas_alias, col("g.cod_agencia") == col("plat.cod_agencia"), "left")

df_gerentes_enrich = df_gerentes_plat.select(
    col("g.cod_broker"),
    col("g.taxa_comissao"),
    coalesce(col("u.nome"), col("nome_geral"), lit("GERENTE NÃO IDENTIFICADO")).alias("nome_gerente"),
    col("plat.nome_plataforma"),
    col("plat.gestor_da_plataforma")
).dropDuplicates(["cod_broker"]).alias("gerentes")

# Aliasing other tables for join safety
df_escrow = df_escrow.alias("escrow")
df_first_op = df_first_op.alias("first_op")
df_client_rate = df_client_rate.alias("client_rate")

# PASSO 1: Tratamento de Ambiguidade
# Renomeamos o cod_cliente da bridge para garantir unicidade no join
df_bridge_prep = df_bridge_gerente.withColumnRenamed("cod_cliente", "cod_cliente_bridge")

# Enriquecimento com Gerente (Broker)
df_operacoes_com_historico = df_operacoes_limpa.join(
    df_bridge_prep,
    (df_operacoes_limpa["cod_cliente"] == df_bridge_prep["cod_cliente_bridge"]) &
    (df_operacoes_limpa["data_analise"].cast("date") >= df_bridge_prep["data_inicio_vigencia"]) &
    (df_operacoes_limpa["data_analise"].cast("date") <= df_bridge_prep["data_fim_vigencia"]),
    "left"
).dropDuplicates(["cod_operacao"])

# Fallback Logic (Earliest Manager)
# Para operações antigas (ex: antes de Junho 2025) onde cod_broker é 0 e a bridge não tem histórico da data exata.
w_fallback = Window.partitionBy("cod_cliente_bridge").orderBy(col("data_inicio_vigencia").asc())
df_bridge_fallback = df_bridge_prep.withColumn("rn", row_number().over(w_fallback)) \
    .filter(col("rn") == 1) \
    .select(col("cod_cliente_bridge").alias("cod_cliente_fb"), col("cod_gerente").alias("cod_gerente_fb"))

df_operacoes_com_fallback = df_operacoes_com_historico.join(
    df_bridge_fallback,
    df_operacoes_com_historico.cod_cliente == df_bridge_fallback.cod_cliente_fb,
    "left"
)

# Prioridade Atualizada: 1. Bridge Strict > 2. Broker Original (se válido) > 3. Bridge Fallback
df_operacoes_com_gerente = df_operacoes_com_fallback.withColumn(
    "cod_broker",
    when(col("cod_gerente").isNotNull(), col("cod_gerente"))
    .when((col("cod_broker").isNotNull()) & (col("cod_broker") != 0), col("cod_broker"))
    .otherwise(col("cod_gerente_fb"))
).drop("cod_cliente_bridge", "cod_gerente", "data_inicio_vigencia", "data_fim_vigencia", "cod_cliente_fb", "cod_gerente_fb")

# Identificação de Operações Informais
df_chave_danfe = df_cad_geral_arquivos.filter(col("DESCRICAO") == 'CHAVEDANFE')

# ⚡ Bolt Optimization: Filter operations first to reduce join volume (Filter -> Join) instead of (Join -> Filter)
# This avoids joining millions of non-candidate operations/titles with the DANFE table.
df_operacoes_candidates = df_operacoes_com_gerente.filter(
    (col("nota_servico") == 'N') &
    (col("status_analise") == 'D') &
    (col("cod_empresa") == 14) &
    (col("status_aceite") == 'A') &
    (col("tto").isin(['NO','CM','FC']))
)

# Only join titles for relevant operations
df_titulos_candidates = df_operacoes_candidates.join(df_titulos_limpa, on="cod_operacao", how="inner")

# Only join DANFE keys for relevant titles
df_matches = df_titulos_candidates.join(df_chave_danfe, df_titulos_candidates.cod_titulo == df_chave_danfe.CODTITULO, how="inner")

df_vcount = df_matches.groupBy("cod_operacao").count()
df_com_vcount = df_operacoes_com_gerente.join(df_vcount, on="cod_operacao", how="left")


# Enriquecimento com Usuarios, Motivos e Estudo
# Definindo aliases para tabelas
df_ops = df_com_vcount.alias("ops")
df_u_inc = df_usuarios.alias("u_inc")
df_u_ana = df_usuarios.alias("u_ana")
df_u_trava = df_usuarios.alias("u_trava")
df_motivos = df_motivos_indeferimento.alias("motivos")
df_estudo = df_estudo_operacoes.dropDuplicates(["CODOPERACAO"]).alias("estudo")

# Dynamic Column Resolution: Identificar colunas de Risco e Limite com nomes variáveis
# REMOVIDO: A padronização agora ocorre no Silver (NB_Prepara_Tabela_Operacoes).
# Colunas esperadas: valor_risco_estudo e valor_limite_estudo.

df_ops_enrich_step1 = df_ops \
    .join(df_u_inc, col("ops.usua_inclusao") == col("u_inc.cod_usuario"), "left") \
    .join(df_u_ana, col("ops.usua_st_analise") == col("u_ana.cod_usuario"), "left") \
    .join(df_u_trava, col("ops.usua_trava") == col("u_trava.cod_usuario"), "left") \
    .join(df_motivos, col("ops.cod_indeferimento") == col("motivos.codindeferimento"), "left") \
    .join(df_estudo, col("ops.cod_operacao") == col("estudo.CODOPERACAO"), "left") \
    .join(df_gerentes_enrich, col("ops.cod_broker") == col("gerentes.cod_broker"), "left") \
    .join(df_escrow, col("ops.cod_operacao") == col("escrow.cod_operacao"), "left") \
    .join(df_first_op, col("ops.cod_cliente") == col("first_op.cod_cliente"), "left") \
    .join(df_client_rate, col("ops.cod_cliente") == col("client_rate.cod_cliente"), "left") \
    .select(
        col("ops.*"),
        col("u_inc.nome").alias("usuario_inclusao"),
        col("u_inc.nivel").alias("nivel_usuario_inclusao"),
        col("u_inc.funcao").alias("incluido_por"),
        col("u_ana.nome").alias("analista"),
        col("u_trava.nome").alias("analista_trava"),
        col("motivos.motivo_indeferimento"),
        col("motivos.grupo_motivo_indeferimento"),
        col("estudo.fator").alias("taxa_cadastro"),
        col("estudo.valor_risco_estudo").alias("risco_estudo_op"),
        col("estudo.valor_limite_estudo").alias("limite_estudo_op"),
        col("gerentes.taxa_comissao"),
        col("gerentes.nome_gerente").alias("gestor_da_operacao"),
        col("gerentes.nome_plataforma"),
        col("gerentes.gestor_da_plataforma"),
        col("escrow.ESCROW").alias("flag_escrow"),
        col("first_op.data_primeira_operacao_calc"),
        col("client_rate.taxa_cadastro_cliente")
    )

df_operacoes_enriquecida = df_ops_enrich_step1.withColumn(
    "operacao_informal",
    when(
        ((col("count").isNull()) | (col("count") == 0)) & (col("cod_empresa") == 14) & (col("nota_servico") == 'N'),
        lit(True)
    ).otherwise(lit(False))
).withColumn("data_deferimento", to_date(col("data_analise"))) \
 .withColumn("valor_utilizacao_limite_plus_excedente",
             greatest(lit(0), (coalesce(col("risco_estudo_op"), lit(0)) + coalesce(col("valor_de_face"), lit(0)) - coalesce(col("limite_estudo_op"), lit(0))))) \
 .withColumn("era", when(col("data_deferimento") > lit("2023-08-31"), "VALE S").otherwise("VALE N")) \
 .withColumn("chave_base_cliente", concat(lit("40-"), col("cod_cliente"))) \
 .withColumn("chave_base_operacao", concat(lit("40-"), col("cod_operacao"))) \
 .withColumn("chave_base_empresa", concat(lit("40-"), col("cod_empresa"))) \
 .withColumn("chave_ano_mes_base_empresa", concat(lit("40-"), col("cod_empresa"), lit("-"), year(col("data_deferimento")), lit("-"), month(col("data_deferimento")))) \
 .withColumn("chave_meta", concat(col("chave_ano_mes_base_empresa"), lit("-"), col("gestor_da_operacao"))) \
 .withColumn("ano_do_deferimento", year(col("data_deferimento"))) \
 .withColumn("comissao_das_tarifas", col("taxa_comissao") * col("total_de_tarifas")) \
 .withColumn("data_inicio_do_mes", trunc(col("data_deferimento"), "MM")) \
 .withColumn("dia_da_operacao", dayofmonth(col("data_deferimento"))) \
 .withColumn("dia_da_semana_da_operacao", dayofweek(col("data_deferimento"))) \
 .withColumn("dia_da_semana_da_operacao_por_extenso",
    when(col("dia_da_semana_da_operacao") == 2, "Segunda")
    .when(col("dia_da_semana_da_operacao") == 3, "Terça")
    .when(col("dia_da_semana_da_operacao") == 4, "Quarta")
    .when(col("dia_da_semana_da_operacao") == 5, "Quinta")
    .when(col("dia_da_semana_da_operacao") == 6, "Sexta")
    .otherwise(None)) \
 .withColumn("faixa_de_tempo_de_analise_horas", abs(ceil((unix_timestamp(col("data_analise")) - unix_timestamp(col("data_inclusao")))/3600))) \
 .withColumn("faixa_de_tempo_de_analise_minutos", abs(ceil((unix_timestamp(col("data_analise")) - unix_timestamp(col("data_inclusao")))/60))) \
 .withColumn("tempo_de_analise_minutos", (unix_timestamp(col("data_analise")) - unix_timestamp(col("data_inclusao"))) / 60) \
 .withColumn("hora_da_inclusao", hour(col("data_inclusao"))) \
 .withColumn("meses_de_idade_do_cliente", floor(months_between(col("data_deferimento"), col("data_primeira_operacao_calc")))) \
 .withColumn("semana_do_deferimento", weekofyear(col("data_deferimento"))) \
 .withColumn("status_analisado_no_mesmo_dia", to_date(col("data_inclusao")) == to_date(col("data_analise"))) \
 .withColumn("status_escrow", when(col("flag_escrow").cast("boolean") == True, "sim").otherwise("não")) \
 .withColumn("status_meta", lit("SIM")) \
 .withColumn("status_taxa_majorada",
    when(col("taxa") > col("taxa_cadastro_cliente"), "MAJORADA")
    .when(col("taxa") < col("taxa_cadastro_cliente"), "REDUZIDA")
    .otherwise("MANTIDA")) \
 .withColumn("tarifa_de_recompra", col("tarifa_recompra") * col("n_docs_recompra")) \
 .withColumn("tarifa_de_titulos", col("n_docs") * col("tarifa")) \
 .na.fill(0, subset=["tac", "valor_taxa_adm", "valor_advalorem", "total_de_tarifas", "n_docs_recompra", "valor_pendencias"]) \
 .drop("count").cache()

print("DataFrames intermediários criados e cacheados.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 2: Construção das Tabelas da Camada Gold

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 2.1: Construção da Fato Operações
# ----------------------------------------
print("\nIniciando construção da fato_operacoes (Otimizada)...")

def prepare_operacoes_dataframe(df_operacoes_enriquecida):
    """
    Prepara o dataframe de operações enriquecidas para o join.
    - Cria sk_operacao e data_join_calendario.
    - Filtra TTOs desnecessários.
    """
    # Criamos a coluna de junção ANTES. Isso permite que o Spark entenda a distribuição dos dados.
    # Se 'data_inclusao' for timestamp, to_date corta a hora. Se for string, ele converte.
    df_operacoes_prep = df_operacoes_enriquecida.withColumn(
        "data_join_calendario",
        to_date(col("data_inclusao"))
    ).withColumn("sk_operacao", xxhash64(col("cod_empresa").cast("string"), col("cod_operacao").cast("string")))

    # ⚡ Bolt Optimization: Filter TTOs (PR, RC, RE) BEFORE joins to reduce data volume
    return df_operacoes_prep.filter(~col("tto").isin(["PR", "RC", "RE"]))

def join_operacoes_dimensions(df_operacoes_filtered, df_dim_calendario, df_dim_produto):
    """
    Realiza os joins do fato operações com as dimensões de calendário e produto.
    """
    # Adicionando a sk_data para join com dim_calendario
    # E sk_produto para join com dim_produtos
    return df_operacoes_filtered.join(
        broadcast(df_dim_calendario.select("data", "sk_data")),
        col("data_join_calendario") == col("data"),
        "left"
    ).join(
        broadcast(df_dim_produto.select("chave_produto", "sk_produto")),
        "chave_produto",
        "left"
    )

def select_fato_operacoes_columns(df_fato_operacoes_joined):
    """
    Seleciona e renomeia as colunas finais para a tabela fato_operacoes.
    """
    return df_fato_operacoes_joined.select(
        col("sk_operacao"),
        col("cod_operacao"),
        col("nbordero"),
        col("cod_cliente"),
        col("cod_empresa"),
        col("data_inclusao"),
        col("data_analise"),
        col("status_aceite"),
        col("status_analise"),
        col("cod_broker"),
        col("tto"),
        col("stto"),
        col("chave_produto"),
        col("sk_produto"),
        col("operacao_informal"),
        col("valor_retido"),
        col("valor_desembolsado"),
        col("valor_de_face"),
        col("desagio"),
        col("total_de_tarifas"),
        col("valor_pendencias"),
        col("valor_utilizacao_limite_plus_excedente"),
        col("sk_data"),
        col("valor_recomprado"),
        col("usuario_inclusao"),
        col("nivel_usuario_inclusao"),
        col("analista"),
        col("analista_trava"),
        col("motivo_indeferimento"),
        col("grupo_motivo_indeferimento"),
        col("taxa_cadastro"),
        col("taxa").alias("taxa_operacao"),
        col("era"),
        col("data_deferimento"),
        col("chave_base_cliente"),
        col("chave_base_operacao"),
        col("chave_base_empresa"),
        col("incluido_por"),
        col("tac"),
        col("valor_taxa_adm"),
        col("valor_advalorem"),
        col("n_docs_recompra"),
        col("chave_meta"),
        col("ano_do_deferimento"),
        col("comissao_das_tarifas"),
        col("data_inicio_do_mes"),
        col("dia_da_operacao"),
        col("dia_da_semana_da_operacao"),
        col("dia_da_semana_da_operacao_por_extenso"),
        col("faixa_de_tempo_de_analise_horas"),
        col("faixa_de_tempo_de_analise_minutos"),
        col("tempo_de_analise_minutos"),
        col("hora_da_inclusao"),
        col("meses_de_idade_do_cliente"),
        col("semana_do_deferimento"),
        col("status_analisado_no_mesmo_dia"),
        col("status_escrow"),
        col("status_meta"),
        col("status_taxa_majorada"),
        col("tarifa_de_recompra"),
        col("tarifa_de_titulos"),
        col("gestor_da_operacao"),
        col("nome_plataforma"),
        col("gestor_da_plataforma")
    ).dropDuplicates(["cod_operacao"])

def create_fato_operacoes(df_operacoes_enriquecida, df_dim_calendario, df_dim_produto):
    # 1. PREPARAÇÃO (Engenharia):
    df_operacoes_filtered = prepare_operacoes_dataframe(df_operacoes_enriquecida)

    # 2. JOINS
    df_fato_operacoes_joined = join_operacoes_dimensions(df_operacoes_filtered, df_dim_calendario, df_dim_produto)

    # 3. SELEÇÃO FINAL
    return select_fato_operacoes_columns(df_fato_operacoes_joined)

df_fato_operacoes = create_fato_operacoes(df_operacoes_enriquecida, df_dim_calendario, df_dim_produto).cache()
output_path_fato_operacoes = TableNames.GOLD_FATO_OPERACOES
df_fato_operacoes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_fato_operacoes)
print(f"Tabela 'fato_operacoes' salva em: {output_path_fato_operacoes}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 2.2: Construção da Fato Baixas
# -------------------------------------
print("\nIniciando construção da fato_baixas...")
# Apply manual fixes (Mantido para correções de negócio específicas)
# A correção de juros agora é feita na camada Silver (NB_Preparacao_Silver).
df_baixas_corrigido = df_baixas_staging
df_enriquecido_baixas = df_baixas_corrigido \
    .join(df_titulos_limpa.select("cod_titulo", "cod_operacao"), on="cod_titulo", how="left") \
    .join(broadcast(df_dim_pago_por), df_baixas_corrigido.pago_pelo == df_dim_pago_por.id, how="left") \
    .join(broadcast(df_dim_forma_pagamento), df_baixas_corrigido.forma == df_dim_forma_pagamento.id, how="left") \
    .join(broadcast(df_dim_tipo_taxa), df_baixas_corrigido.tipo_baixa == df_dim_tipo_taxa.id, how="left") \
    .join(broadcast(df_dim_motivo_baixa), df_baixas_corrigido.motivo == df_dim_motivo_baixa.id, how="left")

df_fato_baixas = df_enriquecido_baixas.select(
    "cod_titulo_baixas", "cod_titulo", "data_baixa", "data_baixa_sist", "valor_pago",
    "desconto", "juros", "tarifa_recompra", "data_vencimento", df_baixas_corrigido["cod_operacao"],
    df_dim_pago_por["descricao"].alias("pago_por"), df_dim_forma_pagamento["descricao"].alias("forma"),
    df_dim_tipo_taxa["descricao"].alias("tipo_baixa"), df_dim_motivo_baixa["descricao"].alias("motivo")
)
output_path_fato_baixas = TableNames.GOLD_FATO_BAIXAS
df_fato_baixas.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_fato_baixas)
print(f"Tabela 'fato_baixas' salva em: {output_path_fato_baixas}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 2.3: Construção da Dimensão Produto
# ------------------------------------------
# OBS: A tabela LH_Gold.dim_produtos foi movida para NB_Gold_Dim_Produtos.Notebook
# A leitura agora ocorre na Célula 0.2.
print("Dimensão Produto (dim_produtos) já carregada na inicialização.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 3: Construção da Fato Títulos (Otimizada)

# CELL ********************

print("\nIniciando construção da fato_titulos...")
# 3.1 Preparação e Enriquecimento
# --------------------------------------------------------------
df_titulos_base = df_titulos_limpa.filter(~col("t_doc").isin("BL", "RC")) \
    .withColumn("tipo_documento_sacado", when(length(col("cpf_cnpj_sacado")) == 11, "CPF").when(length(col("cpf_cnpj_sacado")) == 14, "CNPJ").otherwise("Inválido")) \
    .withColumn("raiz_cnpj", when(col("tipo_documento_sacado") == "CNPJ", substring(col("cpf_cnpj_sacado"), 1, 8)).otherwise(col("cpf_cnpj_sacado")))

# Adicionando sk_operacao para join eficiente no Power BI
df_operacoes_enriquecida_sk = df_operacoes_enriquecida.withColumn("sk_operacao", xxhash64(col("cod_empresa").cast("string"), col("cod_operacao").cast("string")))
df_operacoes_small = df_operacoes_enriquecida_sk.select("sk_operacao", "cod_operacao", "cod_cliente", "data_analise", "status_aceite", "status_analise", "chave_produto", "tto", "taxa_comissao").dropDuplicates(["cod_operacao"])
df_limites_small = df_limites.select("chave_cliente_sacado", "tipo").dropDuplicates(["chave_cliente_sacado"])
df_produtos_small = df_dim_produto.select(col("chave_produto"), col("produto_informacao_de_mercado").alias("produto_temp")).dropDuplicates(["chave_produto"])
df_devolucoes_small = df_devolucoes.select(col("cod_titulo"), col("cod_operacao").alias("cod_operacao_recompra")).dropDuplicates(["cod_titulo"])
df_ultima_conf_small = df_ultima_conf.select(col("cod_titulo"), col("confirmacao").alias("confirmado_por")).dropDuplicates(["cod_titulo"])
df_protestos_small = df_protestos.select("cod_titulo", "status_protesto").dropDuplicates(["cod_titulo"])

# Flag Juridico
df_juridico_flag = df_relatorio_juridico.select("cod_titulo").distinct().withColumn("status_enviado_juridico", lit(True))

df_titulos_com_chave_sacado = df_titulos_base.join(broadcast(df_operacoes_small), "cod_operacao", "left").withColumn("chave_cliente_sacado", concat(col("cod_cliente").cast("string"), lit("-"), col("raiz_cnpj")))

df_enriquecido = df_titulos_com_chave_sacado \
    .join(broadcast(df_limites_small), "chave_cliente_sacado", "left") \
    .join(broadcast(df_produtos_small), "chave_produto", "left") \
    .join(broadcast(df_devolucoes_small), "cod_titulo", "left") \
    .join(broadcast(df_ultima_conf_small), "cod_titulo", "left") \
    .join(broadcast(df_protestos_small), "cod_titulo", "left") \
    .join(broadcast(df_juridico_flag), "cod_titulo", "left") \
    .na.fill({"amortizacoes": 0}) \
    .withColumn("status_enviado_juridico", coalesce(col("status_enviado_juridico"), lit(False)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3.2 Cálculos de Negócio
# -----------------------
df_com_calcs = df_enriquecido \
    .withColumn("intercompany", when(col("tipo") == "INTERCIA", "SIM").otherwise("NÃO")) \
    .withColumn("status_protesto", coalesce(col("status_protesto"), lit("NÃO PROTESTADO"))) \
    .withColumn("valor_vezes_prazo", col("prazo") * col("valor")) \
    .withColumn("produto_com_intercia", when((col("intercompany") == "SIM") & (col("chave_produto").isin("NO", "CM")), "INTERCOMPANY").otherwise(col("produto_temp"))) \
    .withColumn("custo_financeiro", (col("valor") - col("desagio")) * (pow(lit(1.015), col("prazo") / 30) - 1)) \
    .withColumn("spread", col("desagio") - col("custo_financeiro")) \
    .withColumn("comissao_spread", col("spread") * coalesce(col("taxa_comissao"), lit(0.025)))

# Data Vencimento Útil
try:
    df_dim_cal_dates = df_dim_calendario.select(col("data"), col("proximo_dia_util"))
    df_dates_final = df_com_calcs.join(broadcast(df_dim_cal_dates), df_com_calcs.venc_prorrogado == df_dim_cal_dates.data, "left").withColumnRenamed("proximo_dia_util", "data_vencimento_util").drop("data")
except Exception as e:
    print(f"AVISO: Erro ao ler dim_calendario: {e}.")
    df_dates_final = df_com_calcs.withColumn("data_vencimento_util", col("venc_prorrogado"))

# Classificação de Risco e Atraso
df_classificacao = df_dates_final.withColumn("dias_atraso", datediff(current_date(), col("data_vencimento_util"))) \
    .withColumn("status_risco", get_status_risco_expr())

df_status_1 = df_classificacao.withColumn("status_deferimento", when((col("aceito") == "S") & (col("status_aceite") == "A") & (col("status_analise") == "D"), "Sim").otherwise("Não"))
df_status_2 = df_status_1.withColumn("status_clean", when(col("produto_com_intercia") == "DESCONTO", "NORMAL").otherwise("CLEAN"))

# Confirmacao Logic using Bronze column or Fallback
df_conf = df_status_2.withColumn("confirmacao", when(col("doc_confirmado") == "N", "Atenção").when(col("doc_confirmado") == "S", None).when(col("doc_confirmado") == "C", "Positivo").when(col("doc_confirmado") == "P", "Problema").when(col("doc_confirmado") == "A", "Alerta").when(col("doc_confirmado").isNull(), "Não Contatado").when(col("doc_confirmado").isin("E", "AZ"), "Eletrônico").otherwise(col("doc_confirmado")))
df_ordem = df_conf.withColumn("ordem_confirmacao", when(col("confirmacao") == "Não Contatado", 5).when(col("confirmacao") == "Atenção", 2).when(col("confirmacao") == "Eletrônico", 0).when(col("confirmacao") == "Positivo", 1).when(col("confirmacao") == "Alerta", 3).when(col("confirmacao") == "Problema", 4).otherwise(None))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3.3 Seleção Final e Persistência
# ---------------------------------
df_fato_titulos_final = df_ordem.select(
    col("sk_operacao"), col("cod_titulo"), col("cod_operacao"), col("t_doc"), col("n_doc"), col("cpf_cnpj_sacado"), col("vencimento"), col("venc_prorrogado"), col("valor"),
    col("prazo"), col("aceito"), col("data_inclusao"), col("usua_conf").alias("usua_inclusao"), col("data_alteracao"), col("amortizacoes"),
    "chave_produto", "status_protesto", "tipo_documento_sacado", "raiz_cnpj", "valor_vezes_prazo",
    "produto_com_intercia", "data_vencimento_util", "status_deferimento", "status_clean",
    "confirmacao", "ordem_confirmacao", "cod_operacao_recompra", "confirmado_por", "intercompany",
    col("liquidacao"), col("valor_devido"), col("motivo"),
    col("status_risco"), col("dias_atraso"), col("status_enviado_juridico"),
    col("custo_financeiro"),
    col("spread"), 
    col("comissao_spread"),
    col("cod_cliente")
).cache()
output_path_titulos_final = TableNames.GOLD_FATO_TITULOS
df_fato_titulos_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_titulos_final)
print(f"Tabela 'fato_titulos' salva em: {output_path_titulos_final}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 5.1: Construção da Fato Prorrogações de Títulos (Antiga Fato Operações Prorrogação)
# ---------------------------------------------------
print("\nIniciando construção da fato_prorrogacoes_de_titulos...")

# Leitura da Staging Limpa (Silver)
df_prorrogacao_silver = spark.read.table(TableNames.SILVER_STAGING_OPERACOES_PRORROGACAO_LIMPA)

# Leitura das tabelas auxiliares (Silver/Gold) já carregadas no início (df_titulos_limpa, df_operacoes_limpa)
# Mas garantindo a seleção correta
df_titulos_join = df_titulos_limpa.select(col("cod_titulo"), col("valor").alias("VALOR_TITULO"))
# PREPARAÇÃO PARA JOIN DE OPERAÇÕES (Evitando Duplicidade de Colunas)
# Identificamos colunas que já existem na origem (prorrogação) para não duplicar no join
cols_origem = df_prorrogacao_silver.columns
cols_desejadas_ops = ["cod_cliente", "status_analise", "status_aceite", "nbordero"]

# Selecionamos apenas as colunas que AINDA NÃO EXISTEM na origem (exceto a chave cod_operacao)
cols_to_select = ["cod_operacao"] + [c for c in cols_desejadas_ops if c not in cols_origem]

df_operacoes_join = df_operacoes_limpa.select(*[col(c) for c in cols_to_select])

# Join
# Etapa 2: Mesclar dados de títulos
df_joined_titulos = df_prorrogacao_silver.join(df_titulos_join, "cod_titulo", "left_outer")

# Etapa 4: Mesclar dados de operações
df_joined_full = df_joined_titulos.join(df_operacoes_join, "cod_operacao", "left_outer")

# Etapa 6: Remover colunas desnecessárias
# Nota: As colunas originais do bronze foram convertidas para lower case no Silver (staging_operacoes_prorrogacao_limpa)
# Portanto, removemos as versões lower case.

df_cleaned = df_joined_full.drop(*COLS_TO_REMOVE_PRORROGACAO)

# Tratamento do campo VALOR (Prioridade para o valor vindo de Títulos, se expandido)
# Se existir 'valor' na tabela original, ele pode conflitar ou ser substituído.
# No M script: Table.ExpandTableColumn(..., {"VALOR"}, {"VALOR"}) sugere que usamos o valor do título.
if "valor" in df_cleaned.columns:
    df_cleaned = df_cleaned.drop("valor")
df_cleaned = df_cleaned.withColumnRenamed("VALOR_TITULO", "valor")

# Etapas 7, 8, 9: Transformações (Removido colunas legado: base e chave_base_titulo)
df_final_prorrogacao = df_cleaned \
    .withColumn("data", to_date(col("data_inclusao"))) \
    .withColumn("dias_prorrogados", datediff(col("vencimentonov"), col("vencimentoant")))

target_fato_prorrogacao = TableNames.GOLD_FATO_PRORROGACOES_DE_TITULOS
df_final_prorrogacao.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_fato_prorrogacao)
print(f"Tabela '{target_fato_prorrogacao}' criada com sucesso.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 5.3: Construção da Fato Operações Prorrogação (Nova)
# -----------------------------------------------------------
print("\nIniciando construção da fato_operacoes_prorrogacao (NOVA)...")

# Fonte = stg_operacoes (df_operacoes_limpa)
# ⚡ Bolt Optimization: Reuse dataframe loaded in Section 0.2
# df_operacoes_source = spark.read.table(TableNames.SILVER_STAGING_OPERACOES_LIMPA)
df_operacoes_source = df_operacoes_limpa

# Filtrar TTO = 'PR'
df_ops_pr = df_operacoes_source.filter(col("tto") == "PR")

# Remover colunas (Mapeamento M Script -> Snake Case)
# M: DATAACEITE, DATAENVIOEMAIL, STTO, FATOR, TARIFA, TARIFARECOMPRA, TAC, NDOCS, NDOCSRECOMPRA, TOTADVAL, TOTTAXAADM, TOTTAR,
# APROVACAO1, DATAALTERACAO, CODINDEFERIMENTO, CONTRATOFISICO, TAXACADASTRO, MOTIVO INDEFERIMENTO, GRUPO MOTIVO INDEFERIMENTO
# Colunas que podem não existir no DF Silver (Ignorando erro se não existirem)
df_ops_pr_clean = df_ops_pr.drop(*COLS_TO_REMOVE_PR)

# Join com Boletos (LH_Silver.staging_boletos_titulos)
# Precisamos carregar a tabela boletos, pois não foi carregada no início explicitamente (apenas df_titulos_limpa)
print("Carregando Boletos (Silver)...")
df_boletos_titulos = safe_read_table(spark, TableNames.SILVER_STAGING_BOLETOS_TITULOS, fallback_df=df_titulos_limpa.filter(col("t_doc") == "BL"))

# Selecionar colunas de interesse do boleto antes do join para evitar duplicação/ambiguidade
# M: {"CODTITULO", "NDOC", "CPFCNPJSACADO", "CPFCNPJCEDENTE", "VALOR",  "AMORTIZACOES", "LIQUIDACAO"}
df_boletos_select = df_boletos_titulos.select(
    col("cod_operacao"),
    col("cod_titulo"),
    col("n_doc"),
    col("cpf_cnpj_sacado"),
    col("cpf_cnpj_cedente"),
    col("valor"),
    col("amortizacoes"),
    col("liquidacao")
)

# Join Left Outer
df_joined_pr = df_ops_pr_clean.join(df_boletos_select, "cod_operacao", "left")

# Expandido já feito pelo select.
# Remover colunas finais: STATUSANALISE, STATUSACEITE, TTO
df_final_pr = df_joined_pr.drop("status_analise", "status_aceite", "tto")

target_nova_fato_prorrogacao = TableNames.GOLD_FATO_OPERACOES_PRORROGACAO
df_final_pr.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_nova_fato_prorrogacao)
print(f"Tabela '{target_nova_fato_prorrogacao}' criada com sucesso.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 5.4: Construção da Fato Operações Recompra
# -------------------------------------------------
print("\nIniciando construção da fato_operacoes_recompra...")

# Fonte = stg_operacoes
# Optimization: Reuse dataframe loaded in Section 0.2 or 5.3
# if "df_operacoes_source" not in locals():
#     df_operacoes_source = spark.read.table(TableNames.SILVER_STAGING_OPERACOES_LIMPA)
df_operacoes_source = df_operacoes_limpa

# Filter TTO = 'RC' or 'RE' AND status_analise = 'D' AND status_aceite = 'A'
df_ops_rc = df_operacoes_source.filter(
    (col("tto").isin(["RC", "RE"])) &
    (col("status_analise") == "D") &
    (col("status_aceite") == "A")
)

# Remover colunas: TOTADVAL, TOTTAXAADM, DATAACEITE
# Mapeamento: valor_advalorem, valor_taxa_adm, data_aceite
df_ops_rc_clean = df_ops_rc.drop("valor_advalorem", "valor_taxa_adm", "data_aceite")

# Join com Boletos (Mesma tabela df_boletos_select)
df_joined_rc = df_ops_rc_clean.join(df_boletos_select, "cod_operacao", "left")

# Remover: STATUSANALISE, STATUSACEITE
df_joined_rc_clean = df_joined_rc.drop("status_analise", "status_aceite")

# Renomear: chave_base_operacao -> chave_base_operacao_recompra
# Nota: chave_base_operacao não existe nativamente no df_operacoes_limpa (é criada no enriched).
# Mas vamos criar se não existir ou renomear se existir.
# Se não existir, criamos: "40-" + CODOPERACAO
if "chave_base_operacao" in df_joined_rc_clean.columns:
    df_final_rc = df_joined_rc_clean.withColumnRenamed("chave_base_operacao", "chave_base_operacao_recompra")
else:
    df_final_rc = df_joined_rc_clean.withColumn("chave_base_operacao_recompra", concat(lit("40-"), col("cod_operacao")))

target_fato_recompra = TableNames.GOLD_FATO_OPERACOES_RECOMPRA
df_final_rc.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_fato_recompra)
print(f"Tabela '{target_fato_recompra}' criada com sucesso.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 6: Construção da Dimensão Clientes Enriquecida
# **Objetivo:** Unificar dados cadastrais com métricas agregadas de Operações, Títulos e Esteira.

# CELL ********************

print("\nIniciando construção da dim_clientes enriquecida...")
from pyspark.sql.functions import sum, min, count, current_date, round, floor, dayofmonth

# 6.0: Preparação de Dados Auxiliares
# -----------------------------------
# Sup Status Clientes (Silver)
df_sup_status = spark.read.table(TableNames.SILVER_SUP_STATUS_DE_CLIENTES_DA_ESTEIRA)

# Prepare Cad Clientes Status
# Bronze: CODSTATUSCLIENTE
# Silver: codstatuscliente (normalized from CODSTATUSCLIENTE by manual upload loader)
df_status_cad_prep = df_cad_clientes_bronze.join(
    df_sup_status,
    df_cad_clientes_bronze.CODSTATUSCLIENTE == df_sup_status.codstatuscliente,
    "left"
).select(
    col("CODCLIENTE").alias("cod_cliente_status"),
    col("status_do_cliente").alias("status_do_cliente_cad")
)

# Grupos Economicos
df_grupos_prep = df_grupos_economicos.withColumnRenamed("nomegrupo", "grupo_economico")
if "cod_cliente" not in df_grupos_prep.columns and "codcliente" in df_grupos_prep.columns:
     df_grupos_prep = df_grupos_prep.withColumnRenamed("codcliente", "cod_cliente")
df_grupos_prep = df_grupos_prep.select("cod_cliente", "grupo_economico")

# 6.0.1: Info Gestor (Para join final)
df_bridge_atual = df_bridge_gerente.filter(col("data_fim_vigencia") == "9999-12-31")
# Join com df_gerentes_enrich (criada na Seção 1.2) para obter nome_gerente correto
df_info_gestor = df_bridge_atual \
    .join(df_gerentes_enrich, df_bridge_atual.cod_gerente == df_gerentes_enrich.cod_broker, "left") \
    .select(
        df_bridge_atual.cod_cliente,
        df_gerentes_enrich.gestor_da_plataforma,
        df_gerentes_enrich.nome_gerente,
        df_bridge_atual.cod_gerente.alias('cod_broker'),
        df_gerentes_enrich.taxa_comissao
    )

# 6.1: Métricas de Operações
# --------------------------
# Usamos df_fato_operacoes criada na Seção 2.1
# Optimization: Reuse cached DataFrame to avoid I/O and deserialization overhead
df_ops_validas = df_fato_operacoes.filter(col("status_analise") == "D")

# ⚡ Bolt Optimization: Calculate VOP metrics reusing existing columns
df_dia_semana_top, df_dia_mes_top = calculate_vop_metrics(df_ops_validas)

# Métricas Gerais Operações
df_metrics_ops = df_ops_validas.groupBy("cod_cliente").agg(
    max("data_analise").alias("data_ultima_operacao"),
    max("cod_operacao").alias("bordero_ultima_operacao"),
    min(when(col("status_aceite") == "A", col("data_analise"))).alias("data_primeira_operacao"),
    min(when((col("status_aceite") == "A") & (col("status_analise") == "D"), col("data_analise"))).alias("data_cliente_desde")
)

df_metrics_ops_final = df_metrics_ops.join(df_dia_semana_top, "cod_cliente", "left").join(df_dia_mes_top, "cod_cliente", "left")

# 6.2: Métricas de Títulos (Risco)
# --------------------------------
# Usamos df_fato_titulos_final criada na Seção 3.3
# Join com Operações para pegar cod_cliente
# OTIMIZAÇÃO: cod_cliente foi adicionado à fato_titulos na Seção 3.3, evitando este join.
# Optimization: Reuse cached DataFrame to avoid I/O and deserialization overhead
df_titulos_cliente = df_fato_titulos_final

today_date = current_date()

# Filtro Base Risco: Aceito=S, StatusAnalise=D (status_deferimento=Sim), Liquidacao=Null
df_risco_base = df_titulos_cliente.filter((col("status_deferimento") == "Sim") & (col("liquidacao").isNull()))

df_metrics_titulos = df_risco_base.groupBy("cod_cliente").agg(
    sum("valor_devido").alias("risco"),
    sum(when(col("produto_com_intercia") != "COMISSÁRIA", col("valor_devido")).otherwise(0)).alias("risco_exceto_comissaria"),
    sum(when(col("produto_com_intercia") == "COMISSÁRIA", col("valor_devido")).otherwise(0)).alias("risco_comissaria"),
    sum(when(col("confirmacao") == "Atenção", col("valor_devido")).otherwise(0)).alias("confirmado_atencao"),
    sum(when(col("confirmacao") == "Positivo", col("valor_devido")).otherwise(0)).alias("confirmado_positivo"),
    sum(when(col("confirmacao") == "Problema", col("valor_devido")).otherwise(0)).alias("problemas_checagem"),
    # Inadimplencia: VencProrrogado < Hoje-14
    sum(when(col("venc_prorrogado") < date_sub(today_date, 14), col("valor_devido")).otherwise(0)).alias("inadimplencia"),
    # Dates
    min(when(datediff(today_date, col("venc_prorrogado")) >= 15, col("venc_prorrogado"))).alias("data_vencido_mais_antigo"),
    max(when(datediff(today_date, col("venc_prorrogado")) >= 15, col("venc_prorrogado"))).alias("data_vencido_mais_recente"),
    max(when(col("status_risco") == "CRÍTICO", 1).otherwise(0)).alias("has_critico"),
    max(when(col("status_risco") == "ATENÇÃO", 1).otherwise(0)).alias("has_atencao"),

    # NOVAS COLUNAS: Qualidade do Cliente (Risco Clean, Renegociacao, etc.)
    sum(when(col("status_clean") == "CLEAN", col("valor_devido")).otherwise(0)).alias("risco_clean"),
    sum(when(col("produto_com_intercia") == "RENEGOCIAÇÃO", col("valor_devido")).otherwise(0)).alias("risco_renegociacao"),
    sum(when(col("produto_com_intercia") != "RENEGOCIAÇÃO", col("valor_devido")).otherwise(0)).alias("risco_sem_renegociacao"),
    sum(when(col("data_vencimento_util") < today_date, col("valor_devido")).otherwise(0)).alias("vencidos")
)

# Perdas (VOP - VlrPagoLiquido) where Motivo='PR' (Assume proxy for LIQUIDEZ='L5')
# Need a separate aggregation from Fato Baixas or Titulos if column exists
# Assuming 'motivo' in fato_titulos (from previous steps) can be used.
# Logic: sum(valor) - sum(liquidacao value? No, liquidacao is date).
# We need `valor_pago` from somewhere. `fato_titulos` usually has `valor` and `valor_pago` comes from `baixas`.
# But `fato_titulos` in Seção 3 doesn't explicitly have `valor_pago`. It has `amortizacoes`.
# Let's use `amortizacoes` as proxy for `valor_pago_liquido` if strict. Or better, check if we joined Baixas.
# In `Seção 3`, `df_fato_titulos_final` has `amortizacoes`. Let's use that.
# Perdas = (Valor - Amortizacoes) where motivo='PR'.
df_perdas_agg = df_titulos_cliente.filter(col("motivo") == "PR") \
    .groupBy("cod_cliente").agg(
        (sum("valor") - sum("amortizacoes")).alias("perdas")
    )

df_metrics_titulos_final = df_metrics_titulos.join(df_perdas_agg, "cod_cliente", "left").na.fill({"perdas": 0})

# Risco Grupo e Risco Comissaria Grupo
# Precisamos das metricas individuais antes
df_risco_ind = df_metrics_titulos_final.select("cod_cliente", "risco", "risco_comissaria")
df_risco_grupo_agg = df_risco_ind.join(df_grupos_prep, "cod_cliente", "inner") \
    .groupBy("grupo_economico").agg(
        sum("risco").alias("risco_grupo"),
        sum("risco_comissaria").alias("risco_comissaria_grupo")
    )

# 6.3: Esteira Dates e Funnel
# ---------------------------
df_esteira = spark.read.table(TableNames.GOLD_ESTEIRA_DE_PROPOSTAS)

# Normalização de colunas (Compatibilidade com Legado/Schema antigo)
# Se a tabela não foi regerada (incremental = 0), ela pode estar com colunas em UpperCase.
# Forçamos o rename para snake_case esperado.
if "CODCLIENTE" in df_esteira.columns:
    df_esteira = df_esteira.withColumnRenamed("CODCLIENTE", "cod_cliente")
if "STATUS_DO_CLIENTE" in df_esteira.columns:
    df_esteira = df_esteira.withColumnRenamed("STATUS_DO_CLIENTE", "status_do_cliente")
if "DATALOG" in df_esteira.columns:
    df_esteira = df_esteira.withColumnRenamed("DATALOG", "datalog")

# Status esperados para o Pivot (para evitar erros de coluna inexistente)
# Refatorado para usar constante global
expected_status = list(STATUS_ESTEIRA_MAPPING.keys())

# Pivot Simples das Datas Maximas e Minimas por Status (Otimizado - Single Pass)
# ⚡ Bolt Optimization: Use single pass pivot for both Max and Min dates
df_esteira_pivot, df_esteira_min = transform_esteira_dates(df_esteira, STATUS_ESTEIRA_MAPPING)


# 6.3.1: Latest Status Esteira (Power BI Requirement)
w_latest = Window.partitionBy("cod_cliente").orderBy(col("datalog").desc())
df_esteira_latest = df_esteira.withColumn("rn", row_number().over(w_latest)).filter(col("rn") == 1) \
    .select(
        col("cod_cliente").alias("cod_cliente_latest"),
        col("status_do_cliente").alias("status_do_cliente"),
        col("macroprocesso").alias("MACROPROCESSO"),
        col("fase").alias("FASE"),
        col("datalog").alias("data_status")
    )

# 6.4: Limites (Contratos e Extra/Plus)
# ------------
# 6.4.1 Limites Contratos (Legado)
df_limites_agg = df_contratos.filter(col("status") == "A") \
    .withColumn("limite_total", coalesce(col("limite_fomento"), lit(0)) + coalesce(col("limite_comissaria"), lit(0))) \
    .groupBy("cod_cliente").agg(
        sum("limite_total").alias("limite"),
        sum("limite_comissaria").alias("limite_comissaria_contrato"),
        max("validade_limite").alias("vencimento_limite"),
        max("tranche").alias("tranche"),
        max("perc_confirmacao").alias("percentual_exigido")
    )

# 6.4.2 Limites Extra e Plus (Desduplicação por Grupo)
# ----------------------------------------------------
# 1. Normalizar CNPJ para Join com Clientes
df_limites_ep_prep = df_limites_extra_plus.withColumn("cnpj_clean", regexp_replace(col("cnpj"), "[^0-9]", ""))

# 2. Join com Staging Clientes para obter cod_cliente
df_limites_ep_clientes = df_limites_ep_prep.join(
    df_clientes_staging.select(col("cpf_cnpj").alias("cnpj_clean"), "cod_cliente"),
    "cnpj_clean",
    "inner"
)

# 3. Join com Grupos Economicos para obter nome do grupo
# df_grupos_prep tem 'cod_cliente' e 'grupo_economico'
df_limites_ep_grupos = df_limites_ep_clientes.join(
    df_grupos_prep,
    "cod_cliente",
    "inner"
)

# 4. Desduplicação por Grupo (Max Limites)
# Os limites são repetidos por CNPJ no arquivo. Queremos o limite ÚNICO do GRUPO.
df_limites_grupo_dedup = df_limites_ep_grupos.groupBy("grupo_economico").agg(
    max("limite").alias("limite_grupo_manual"),
    max("limite_extra").alias("limite_extra_grupo"),
    max("limite_plus").alias("limite_plus_grupo")
)

# 6.4.3: Construção da Fato Limites de Crédito (Consolidada)
# ----------------------------------------------------------
# Objetivo: Criar uma tabela única de limites onde a chave é a Entidade (Grupo ou Cliente),
# resolvendo a duplicação de limites de grupo cadastrados em múltiplos clientes.

print("\nConstruindo Fato Limites de Crédito (Consolidada)...")

# 1. Preparar Base Contratos (Limits per Client)
# df_contratos tem: cod_cliente, limite_fomento, limite_comissaria, validade_limite...
# df_limites_obs_silver tem: CODCLIENTE, limite_geral, limite_intercompany, limite_extra_desconto_formal, limite_extra_desconto_informal

# Normalizar nomes de colunas para join
if "CODCLIENTE" in df_limites_obs_silver.columns:
    df_limites_obs_silver = df_limites_obs_silver.withColumnRenamed("CODCLIENTE", "cod_cliente")

# Seleção explícita de colunas do regex
df_limites_obs_select = df_limites_obs_silver.select(
    col("cod_cliente"),
    coalesce(col("limite_geral"), lit(0)).alias("limite_geral_obs"),
    coalesce(col("limite_intercompany"), lit(0)).alias("limite_intercompany_obs"),
    coalesce(col("limite_extra_desconto_formal"), lit(0)).alias("limite_extra_desconto_formal_obs"),
    coalesce(col("limite_extra_desconto_informal"), lit(0)).alias("limite_extra_desconto_informal_obs")
)

# Join Contratos + Obs
df_limites_base = df_contratos.join(df_limites_obs_select, "cod_cliente", "left") \
    .select(
        col("cod_cliente"),
        coalesce(col("limite_fomento"), lit(0)).alias("limite_fomento"),
        coalesce(col("limite_comissaria"), lit(0)).alias("limite_comissaria"),
        col("validade_limite"),
        coalesce(col("limite_geral_obs"), lit(0)).alias("limite_geral"),
        coalesce(col("limite_intercompany_obs"), lit(0)).alias("limite_intercompany"),
        coalesce(col("limite_extra_desconto_formal_obs"), lit(0)).alias("limite_extra_desconto_formal"),
        coalesce(col("limite_extra_desconto_informal_obs"), lit(0)).alias("limite_extra_desconto_informal")
    )

# 2. Join com Grupos (df_grupos_prep: cod_cliente, grupo_economico)
df_limites_base_grp = df_limites_base.join(df_grupos_prep, "cod_cliente", "left")

# 3. Separar em Grupos vs Clientes Individuais
df_com_grupo = df_limites_base_grp.filter(col("grupo_economico").isNotNull())
df_sem_grupo = df_limites_base_grp.filter(col("grupo_economico").isNull())

# 4. Tratamento Grupo (Deduplicação + Enriquecimento Manual)
# Passo 4.1: Deduplicar Contratos (MAX) - Assumindo que limites de grupo são duplicados identicamente nos clientes
df_grupo_contract_agg = df_com_grupo.groupBy("grupo_economico").agg(
    max("limite_fomento").alias("limite_fomento_auto"),
    max("limite_comissaria").alias("limite_comissaria_auto"),
    max("validade_limite").alias("validade_limite_auto"),
    max("limite_geral").alias("limite_geral_auto"),
    max("limite_intercompany").alias("limite_intercompany_auto"),
    max("limite_extra_desconto_formal").alias("limite_extra_desconto_formal_auto"),
    max("limite_extra_desconto_informal").alias("limite_extra_desconto_informal_auto")
)

# Passo 4.2: Join com Manual (df_limites_grupo_dedup já calculado na 6.4.2)
# Colunas em df_limites_grupo_dedup: limite_grupo_manual, limite_extra_grupo, limite_plus_grupo
df_grupo_final = df_grupo_contract_agg.join(df_limites_grupo_dedup, "grupo_economico", "full_outer") \
    .select(
        coalesce(col("grupo_economico"), col("grupo_economico")).alias("nome_entidade"),
        lit("GRUPO").alias("tipo_entidade"),
        concat(lit("G-"), upper(trim(coalesce(col("grupo_economico"), col("grupo_economico"))))).alias("id_limite_credito"),
        # Lógica de Consolidação: Limite Geral = Greatest(Auto, Manual)
        greatest(coalesce(col("limite_fomento_auto"), lit(0)), coalesce(col("limite_grupo_manual"), lit(0))).alias("limite_fomento"),
        coalesce(col("limite_comissaria_auto"), lit(0)).alias("limite_comissaria"),
        coalesce(col("limite_extra_grupo"), lit(0)).alias("limite_extra"),
        coalesce(col("limite_plus_grupo"), lit(0)).alias("limite_plus"),
        col("validade_limite_auto").alias("validade_limite"),
        coalesce(col("limite_geral_auto"), lit(0)).alias("limite_geral"),
        coalesce(col("limite_intercompany_auto"), lit(0)).alias("limite_intercompany"),
        coalesce(col("limite_extra_desconto_formal_auto"), lit(0)).alias("limite_extra_desconto_formal"),
        coalesce(col("limite_extra_desconto_informal_auto"), lit(0)).alias("limite_extra_desconto_informal")
    ).filter(col("nome_entidade").isNotNull())

# 5. Tratamento Clientes Individuais
# Recuperar nome do cliente para 'nome_entidade'
# df_base (Clientes Staging) tem: cod_cliente
# Precisamos do nome. df_geral_pf_pj_limpa tem cpf_cnpj, nome. df_base tem cpf_cnpj.
# df_base já foi carregado na 6.5 (mas ainda não executamos a 6.5). Vamos reutilizar df_cad_geral_enriquecido ou ler de novo.
# df_clientes_staging tem cpf_cnpj. df_cad_geral_enriquecido tem nome.
# Vamos fazer um join rapido para pegar o nome.
df_nomes_clientes = df_clientes_staging.join(df_geral_pf_pj_limpa, "cpf_cnpj", "left").select("cod_cliente", "nome")

df_sem_grupo_named = df_sem_grupo.join(df_nomes_clientes, "cod_cliente", "left")

df_cliente_final = df_sem_grupo_named.select(
    coalesce(col("nome"), concat(lit("CLIENTE "), col("cod_cliente"))).alias("nome_entidade"),
    lit("CLIENTE").alias("tipo_entidade"),
    concat(lit("C-"), col("cod_cliente")).alias("id_limite_credito"),
    col("limite_fomento"),
    col("limite_comissaria"),
    lit(0.0).alias("limite_extra"),
    lit(0.0).alias("limite_plus"),
    col("validade_limite"),
    col("limite_geral"),
    col("limite_intercompany"),
    col("limite_extra_desconto_formal"),
    col("limite_extra_desconto_informal")
)

# 6. Union e Calculo Total
df_fato_limites = df_grupo_final.unionByName(df_cliente_final, allowMissingColumns=True) \
    .withColumn("limite_total_calculado",
        coalesce(col("limite_fomento"), lit(0)) +
        coalesce(col("limite_extra"), lit(0)) +
        coalesce(col("limite_plus"), lit(0))
    )

# 7. Salvar
output_path_fato_limites = TableNames.GOLD_FATO_LIMITES_CREDITO
df_fato_limites.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_fato_limites)
print(f"Tabela 'fato_limites_credito' criada em: {output_path_fato_limites}")

# 6.5: Join Final e Colunas Calculadas
# ------------------------------------
# Base: Clientes Staging
# Atualização: Incluindo data_inclusao (requeridas para idade_cliente e idade_cliente_em_dias)
df_base_raw = df_clientes_staging.select("cod_cliente", "cpf_cnpj", "data_inclusao", "cod_atividade")

# Verificação e Remoção de Duplicados (CNPJ)
# Objetivo: Garantir que a dim_clientes tenha chave única por CPF/CNPJ.
# Regra: Se houver duplicidade, mantemos o cadastro com data_inclusao mais recente (ou cod_cliente maior).
# Optimization: Always apply deduplication to avoid expensive count() action.
df_base = deduplicate_clientes_staging(df_base_raw)

# Prepare Esteira Min Dates for Funnel (Joining back to main flow)
# Renaming for clarity
df_esteira_min_renamed = df_esteira_min \
    .withColumnRenamed("PROPOSTA", "min_proposta") \
    .withColumnRenamed("REVISÃO COMERCIAL", "min_revisao") \
    .withColumnRenamed("DIR COMERCIAL", "min_dir_comercial") \
    .withColumnRenamed("CREDITO", "min_credito") \
    .withColumnRenamed("CHECKLIST", "min_checklist") \
    .withColumnRenamed("CONCLUIDO", "min_concluido") \
    .select("cod_cliente", "min_proposta", "min_revisao", "min_dir_comercial", "min_credito", "min_checklist", "min_concluido")

# Renomeando chaves para evitar ambiguidade nos joins
df_esteira_pivot_prep = df_esteira_pivot.withColumnRenamed("cod_cliente", "cod_cliente_pivot")
df_esteira_min_prep = df_esteira_min_renamed.withColumnRenamed("cod_cliente", "cod_cliente_min")

# Join Chain

# Taxa Cadastro (Power BI Requirement)
# ⚡ Bolt Optimization: Reuse cached df_client_rate from Section 1.2 to avoid scanning df_contratos again.
if "df_client_rate" in locals():
    df_client_rate_gold = df_client_rate.select(
        col("cod_cliente").alias("cod_cliente_rate"),
        col("taxa_cadastro_cliente").alias("taxa_cadastro")
    )
else:
    # Fallback if run interactively out of order
    df_client_rate_gold = df_contratos.filter(col("status") == 'A').groupBy("cod_cliente").agg(max("fator").alias("taxa_cadastro")).withColumnRenamed("cod_cliente", "cod_cliente_rate")

def join_cliente_dimensions(
    df_base,
    df_cad_geral_enriquecido,
    df_metrics_ops_final,
    df_metrics_titulos_final,
    df_esteira_pivot_prep,
    df_esteira_min_prep,
    df_esteira_latest,
    df_limites_agg,
    df_grupos_prep,
    df_limites_grupo_dedup,
    df_risco_grupo_agg,
    df_info_gestor,
    df_client_rate_gold,
    df_status_cad_prep
):
    """
    Realiza o join de todas as dimensões e métricas para compor a tabela final de clientes.
    Dividido em etapas para clareza e manutenibilidade.
    """
    # 1. Enriquecimento Básico e Métricas
    df_metrics = df_base.join(df_cad_geral_enriquecido, "cpf_cnpj", "left") \
        .join(df_metrics_ops_final, "cod_cliente", "left") \
        .join(df_metrics_titulos_final, "cod_cliente", "left")

    # 2. Informações de Esteira (Pivoted, Min Dates, Latest Status)
    df_esteira = df_metrics \
        .join(df_esteira_pivot_prep, df_base.cod_cliente == df_esteira_pivot_prep.cod_cliente_pivot, "left").drop("cod_cliente_pivot") \
        .join(df_esteira_min_prep, df_base.cod_cliente == df_esteira_min_prep.cod_cliente_min, "left").drop("cod_cliente_min") \
        .join(df_esteira_latest, df_base.cod_cliente == df_esteira_latest.cod_cliente_latest, "left").drop("cod_cliente_latest")

    # 3. Limites e Grupos Econômicos
    # Nota: df_grupos_prep introduz 'grupo_economico' usado nos joins seguintes
    df_groups = df_esteira \
        .join(df_limites_agg, "cod_cliente", "left") \
        .join(df_grupos_prep, "cod_cliente", "left") \
        .join(df_limites_grupo_dedup, "grupo_economico", "left") \
        .join(df_risco_grupo_agg, "grupo_economico", "left")

    # 4. Informações Adicionais (Gestor, Taxas, Status Cadastro)
    df_final_join = df_groups \
        .join(df_info_gestor, "cod_cliente", "left") \
        .join(df_client_rate_gold, df_base.cod_cliente == df_client_rate_gold.cod_cliente_rate, "left").drop("cod_cliente_rate") \
        .join(df_status_cad_prep, df_base.cod_cliente == df_status_cad_prep.cod_cliente_status, "left").drop("cod_cliente_status")

    return df_final_join

df_join_1 = join_cliente_dimensions(
    df_base,
    df_cad_geral_enriquecido,
    df_metrics_ops_final,
    df_metrics_titulos_final,
    df_esteira_pivot_prep,
    df_esteira_min_prep,
    df_esteira_latest,
    df_limites_agg,
    df_grupos_prep,
    df_limites_grupo_dedup,
    df_risco_grupo_agg,
    df_info_gestor,
    df_client_rate_gold,
    df_status_cad_prep
)

# Implementando Lógica Funnel Sequencial (Aproximação)
# Data 1: Primeira Proposta Comercial = MIN(Proposta, Revisao, Diretoria)
# Data 2: Credito (Min data credito >= data 1) - Aqui assumimos Min Credito geral, pois PySpark SQL row-level logic é complexa.
# Data 3: Formalizacao (Checklist >= Credito)
# Data 4: Concluido (Concluido >= Formalizacao)

df_funnel = df_join_1 \
    .withColumn("data_primeira_proposta_comercial", least(col("min_proposta"), col("min_revisao"), col("min_dir_comercial"))) \
    .withColumn("data_primeira_proposta_credito",
        when(col("min_credito") >= col("data_primeira_proposta_comercial"), col("min_credito"))
    ) \
    .withColumn("data_primeira_proposta_formalizacao",
        when(col("min_checklist") >= col("data_primeira_proposta_credito"), col("min_checklist"))
    ) \
    .withColumn("data_primeira_proposta_concluida",
        when(col("min_concluido") >= col("data_primeira_proposta_formalizacao"), col("min_concluido"))
    )

# Colunas Calculadas
df_final = calculate_funnel_dates(df_funnel) \
    .withColumn("risco", coalesce(col("risco"), lit(0))) \
    .withColumn("risco_grupo", coalesce(col("risco_grupo"), lit(0))) \
    .withColumn("risco_comissaria_grupo", coalesce(col("risco_comissaria_grupo"), lit(0))) \
    .withColumn("limite_contrato", coalesce(col("limite"), lit(0))) \
    .withColumn("limite_grupo_manual", coalesce(col("limite_grupo_manual"), lit(0))) \
    .withColumn("limite_extra_grupo", coalesce(col("limite_extra_grupo"), lit(0))) \
    .withColumn("limite_plus_grupo", coalesce(col("limite_plus_grupo"), lit(0))) \
    .withColumn("limite", greatest(col("limite_contrato"), col("limite_grupo_manual"))) \
    .withColumn("nome_do_grupo", coalesce(col("grupo_economico"), col("nome"))) \
    .withColumn("limite_comissaria", coalesce(col("limite_comissaria_contrato"), lit(0))) \
    .withColumn("limite_comissaria_contrato", coalesce(col("limite_comissaria_contrato"), lit(0))) \
    .withColumn("percentual_exigido_de_confirmacao", coalesce(col("percentual_exigido"), lit(0))) \
    .withColumn("risco_comissaria", coalesce(col("risco_comissaria"), lit(0))) \
    .withColumn("risco_exceto_comissaria", coalesce(col("risco_exceto_comissaria"), lit(0))) \
    .withColumn("risco_total", col("risco") + col("risco_grupo")) \
    .withColumn("limite_disponivel", (col("limite") + col("limite_extra_grupo") + col("limite_plus_grupo")) - col("risco_total")) \
    .withColumn("risco_subtotal_comissaria", col("risco_comissaria") + col("risco_comissaria_grupo")) \
    .withColumn("disponivel_comissaria", greatest(
        least(col("limite_disponivel"), col("limite_comissaria_contrato") - col("risco_subtotal_comissaria")),
        lit(0)
    )) \
    .withColumn("limite_maximo_disponivel", greatest(col("disponivel_comissaria"), col("limite_disponivel"))) \
    .withColumn("dias_sem_operar", datediff(today_date, greatest(coalesce(col("data_ultima_operacao"), lit("1900-01-01")), coalesce(col("data_conclusao"), lit("1900-01-01"))))) \
    .withColumn("dias_vencidos", datediff(today_date, col("data_vencido_mais_antigo"))) \
    .withColumn("inadimplencia", coalesce(col("inadimplencia"), lit(0))) \
    .withColumn("faixa_pdd",
        when(col("dias_vencidos") > 180, 1)
        .when(col("dias_vencidos") > 150, 0.7)
        .when(col("dias_vencidos") > 120, 0.4)
        .when(col("dias_vencidos") > 90, 0.2)
        .when(col("dias_vencidos") > 60, 0.1)
        .when(col("dias_vencidos") > 30, 0.05)
        .otherwise(0)
    ) \
    .withColumn("pdd", col("faixa_pdd") * col("inadimplencia")) \
    .withColumn("status_atividade",
        when(col("dias_sem_operar") > 120, "INATIVO")
        .when(col("data_ultima_operacao").isNull(), "NUNCA OPEROU")
        .otherwise("ATIVO")
    ) \
    .withColumn("status_limite",
        when((col("limite").isNull()) | (col("limite") == 0), "SEM LIMITE")
        .when(col("vencimento_limite") < today_date, "LIMITE VENCIDO")
        .when(col("risco") == 0, "LIMITE INATIVO")
        .when(col("risco") > col("limite"), "LIMITE EXCEDIDO")
        .otherwise("LIMITE DISPONIVEL")
    ) \
    .withColumn("percentual_cm", col("limite_comissaria_contrato") / col("limite")) \
    .withColumn("falta_checar", greatest(
        (coalesce(col("percentual_exigido"), lit(0.5)) * (col("risco_exceto_comissaria") + col("risco_grupo") - col("risco_subtotal_comissaria")))
        - coalesce(col("confirmado_positivo"), lit(0)) - coalesce(col("confirmado_atencao"), lit(0)),
        lit(0)
    )) \
    .withColumn("data_primeira_operacao_apos_aprovacao",
        when(col("data_primeira_operacao") >= col("data_aprovacao"), col("data_primeira_operacao"))
    ) \
    .withColumn("dias_proposta_comercial", datediff(col("data_primeira_proposta_credito"), col("data_primeira_proposta_comercial"))) \
    .withColumn("dias_proposta_credito", datediff(col("data_primeira_proposta_formalizacao"), col("data_primeira_proposta_credito"))) \
    .withColumn("dias_proposta_formalizacao", datediff(col("data_primeira_proposta_concluida"), col("data_primeira_proposta_formalizacao"))) \
    .withColumn("tempo_conclusao", datediff(col("data_conclusao"), col("data_aprovacao"))) \
    .withColumn("tempo_analise", datediff(col("data_aprovacao"), col("data_entrada"))) \
    .withColumn("idade_cliente", floor(datediff(today_date, to_date(substring(col("data_inclusao").cast("string"), 1, 10))) / 365)) \
    .withColumn("idade_cliente_em_dias", coalesce(datediff(today_date, col("data_primeira_operacao")), lit(0))) \
    .withColumn("status_do_cliente", coalesce(col("status_do_cliente_cad").cast("string"), col("status_do_cliente").cast("string"))) \
    .withColumn("tipo_proposta",
        when(col("dias_sem_operar") > 120, "REATIVAÇÃO")
        .when(col("data_ultima_operacao").isNull(), "PROSPECÇÃO")
        .when(col("idade_cliente_em_dias") > 90, "RENOVAÇÃO")
        .otherwise("PROSPECÇÃO")
    ) \
    .withColumn("pais", lit("Brasil")) \
    .withColumn("primeiro_nome_gerente", split(col("gestor_da_plataforma"), " ")[0]) \
    .withColumn("id_limite_credito",
        when(col("grupo_economico").isNotNull(), concat(lit("G-"), upper(trim(col("grupo_economico")))))
        .otherwise(concat(lit("C-"), col("cod_cliente")))
    ) \
    .withColumn("status_operando_vencido",
        when(
            (col("data_ultima_operacao") > col("vencimento_limite")) &
            (col("vencimento_limite").isNotNull()) &
            (date_add(col("data_ultima_operacao"), 120) >= today_date),
            "OPERANDO VENCIDO"
        ).otherwise("OPERANDO NORMAL")
    ) \
    .withColumn("taxa_minima_exigida",
        (when(col("faixa_pdd") == 0.05, 0.0025) # Rating A (0.05 PDD)
         .when(col("faixa_pdd") == 0.7, 0.0075) # Rating C (0.7 PDD?) - DAX Logic vague on ratingPdd mapping, using best guess from PDD
         .otherwise(0.0050) # Rating B
        ) + 0.0150 + 0.0010
    ) \
    .withColumn("pendencias_desc", concat_ws(", ",
        when(col("status_atividade") == "INATIVO", "Cliente inativo"),
        when(col("falta_checar") > 0, "Confirmação desenquadrada"),
        when(col("vencimento_limite") < today_date, "Limite vencido"),
        when(col("limite_maximo_disponivel") <= 0, "Sem limite disponível"),
        when(col("problemas_checagem") > 0, "Problemas de checagem"),
        when(col("inadimplencia") > 0, "Títulos vencidos")
    )) \
    .withColumn("qtd_pendencias",
        (when(col("status_atividade") == "INATIVO", 1).otherwise(0) +
         when(col("falta_checar") > 0, 1).otherwise(0) +
         when(col("vencimento_limite") < today_date, 1).otherwise(0) +
         when(col("limite_maximo_disponivel") <= 0, 1).otherwise(0) +
         when(col("problemas_checagem") > 0, 1).otherwise(0) +
         when(col("inadimplencia") > 0, 1).otherwise(0))
    ) \
    .withColumn("perc_risco_clean", col("risco_clean") / col("risco_sem_renegociacao")) \
    .withColumn("penalidade_clean", when(col("perc_risco_clean") > 0.5, 1).otherwise(0)) \
    .withColumn("penalidade_inativo",
        when(col("dias_sem_operar") > 120, 3)
        .when(col("dias_sem_operar") > 60, 2)
        .when(col("dias_sem_operar") > 30, 1)
        .otherwise(0)
    ) \
    .withColumn("penalidade_perdas", when(col("perdas") > 0, 10).otherwise(0)) \
    .withColumn("perc_vencidos_risco", col("vencidos") / col("risco")) \
    .withColumn("penalidade_inadimplencia",
        when(col("perc_vencidos_risco") > 0.50, 4)
        .when(col("perc_vencidos_risco") > 0.25, 3)
        .when(col("perc_vencidos_risco") > 0.10, 2)
        .when(col("perc_vencidos_risco") > 0.05, 1)
        .otherwise(0)
    ) \
    .withColumn("perc_risco_renegociacao", col("risco_renegociacao") / col("risco")) \
    .withColumn("penalidade_renegociacao",
        when(col("perc_risco_renegociacao") == 1, 3)
        .when(col("perc_risco_renegociacao") > 0.4, 2)
        .when(col("perc_risco_renegociacao") > 0.01, 1)
        .otherwise(0)
    ) \
    .withColumn("qualidade_cliente",
        when(col("penalidade_perdas") > 0, 0)
        .otherwise(
            round(
                lit(10) - (
                    col("penalidade_inadimplencia") +
                    col("penalidade_renegociacao") +
                    col("penalidade_clean") +
                    col("penalidade_inativo")
                ), 0
            )
        )
    )

# Optimization: Cache df_final before splitting to avoid recomputing the massive join DAG multiple times
df_final.cache()
# Removed expensive count() action to avoid unnecessary eager evaluation
# print(f"Total de registros na dim_clientes (Intermediário): {df_final.count()}")

# -------------------------------------------------------------
# Refatoração: Separação da Tabela de Score de Clientes
# Objetivo: Mover colunas de cálculo de score para tabela dedicada
# -------------------------------------------------------------
cols_score = [
    "perc_risco_clean", "penalidade_clean",
    "penalidade_inativo", "penalidade_perdas",
    "perc_vencidos_risco", "penalidade_inadimplencia",
    "perc_risco_renegociacao", "penalidade_renegociacao",
    "qualidade_cliente"
]

print("Criando tabela 'analise_score_clientes' e removendo colunas da dim_clientes...")
# Selecionar apenas colunas de score + chave
df_score_clientes = df_final.select("cod_cliente", *cols_score)
df_score_clientes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TableNames.GOLD_ANALISE_SCORE_CLIENTES)
print(f"Tabela '{TableNames.GOLD_ANALISE_SCORE_CLIENTES}' criada com sucesso.")

# Remover colunas de score do DataFrame principal (dim_clientes)
df_final = df_final.drop(*cols_score)

# -------------------------------------------------------------
# Refatoração: Separação da Análise de Prazos da Esteira
# Objetivo: Mover colunas de datas e tempos da esteira para tabela dedicada
# -------------------------------------------------------------
cols_esteira_prazos = [
    "pivot_checklist", "pivot_assinatura", "pivot_comite", "pivot_concluido",
    "pivot_bizagi", "pivot_renovacao", "pivot_reserva", "pivot_start",
    "pivot_credito", "pivot_proposta", "pivot_revisao_comercial", "pivot_dir_comercial",
    "min_proposta", "min_revisao", "min_dir_comercial", "min_credito",
    "min_checklist", "min_concluido",
    "data_primeira_proposta_comercial", "data_primeira_proposta_credito",
    "data_primeira_proposta_formalizacao", "data_primeira_proposta_concluida",
    "data_aprovacao", "data_conclusao", "data_comite", "data_reserva", "data_entrada",
    "data_primeira_operacao_apos_aprovacao",
    "dias_proposta_comercial", "dias_proposta_credito", "dias_proposta_formalizacao",
    "tempo_conclusao", "tempo_analise"
]

print("Criando tabela 'analise_prazos_esteira' e removendo colunas da dim_clientes...")
# Selecionar apenas colunas de esteira + chave
# Verificar quais colunas realmente existem no df_final para evitar erro
existing_cols = [c for c in cols_esteira_prazos if c in df_final.columns]
missing_cols = set(cols_esteira_prazos) - set(existing_cols)
if missing_cols:
    print(f"AVISO: As seguintes colunas de esteira não foram encontradas em df_final e serão ignoradas: {missing_cols}")

df_analise_prazos = df_final.select("cod_cliente", *existing_cols)
df_analise_prazos.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TableNames.GOLD_ANALISE_PRAZOS_ESTEIRA)
print(f"Tabela '{TableNames.GOLD_ANALISE_PRAZOS_ESTEIRA}' criada com sucesso.")

# Remover colunas de esteira do DataFrame principal (dim_clientes)
df_final = df_final.drop(*existing_cols)

# Salvar
output_path_dim_clientes = TableNames.GOLD_DIM_CLIENTES

# Apply Power BI Adjustments (New Columns Only)
df_final_adjusted = df_final \
    .withColumn("desconsiderar_pdd", lit(False)) \
    .withColumn("status_risco",
        when(col("has_critico") == 1, "CRÍTICO")
        .when(col("has_atencao") == 1, "ATENÇÃO")
        .otherwise("NO PRAZO")
    ).drop(
        "limite_contrato",
        "limite_grupo_manual",
        "limite_extra_grupo",
        "limite_plus_grupo",
        "limite_comissaria_contrato",
        "limite_disponivel",
        "limite_maximo_disponivel",
        "disponivel_comissaria",
        "percentual_cm"
    )

df_final_adjusted.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_dim_clientes)
print(f"Tabela 'dim_clientes' recriada em: {output_path_dim_clientes}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 7: Métricas de Saúde da Carteira (HHI)
# **Objetivo:** Calcular o Índice Herfindahl-Hirschman (HHI) para medir a concentração da carteira por Cedente e Sacado.

# CELL ********************

print("\nIniciando cálculo do HHI da Carteira...")

# Garantindo acesso aos DataFrames base (caso a execução não seja sequencial na sessão interativa)
if "df_fato_titulos_final" not in locals():
    # Fallback only if run interactively/out of order
    df_fato_titulos_final = spark.read.table(TableNames.GOLD_FATO_TITULOS)
if "df_fato_operacoes" not in locals():
    # Fallback only if run interactively/out of order
    df_fato_operacoes = spark.read.table(TableNames.GOLD_FATO_OPERACOES)

# Join para obter cod_cliente para cada título
# OTIMIZAÇÃO: cod_cliente foi adicionado à fato_titulos na Seção 3.3, evitando este join.
df_titulos_carteira = df_fato_titulos_final

# Filtro de Risco Ativo (Carteira em Aberto)
# Critério: Título aceito (status_deferimento='Sim') e em aberto (liquidacao is Null)
df_carteira_ativa = df_titulos_carteira.filter(
    (col("status_deferimento") == "Sim") &
    (col("liquidacao").isNull())
)

# Valor Total da Carteira
total_portfolio_row = df_carteira_ativa.agg(sum("valor_devido").alias("total")).collect()
total_portfolio_value = total_portfolio_row[0]["total"] if total_portfolio_row else 0

if total_portfolio_value > 0:
    # --- HHI Cedente ---
    # s_i = (Volume Cedente / Total) * 100
    df_cedente_shares = df_carteira_ativa.groupBy("cod_cliente") \
        .agg(sum("valor_devido").alias("valor_cedente")) \
        .withColumn("share_pct", (col("valor_cedente") / lit(total_portfolio_value)) * 100)

    # HHI = Sum(s^2)
    hhi_cedente_row = df_cedente_shares.select(sum(col("share_pct") * col("share_pct"))).collect()
    hhi_cedente = hhi_cedente_row[0][0] if hhi_cedente_row else 0.0

    # --- HHI Sacado ---
    # s_j = (Volume Sacado / Total) * 100
    df_sacado_shares = df_carteira_ativa.groupBy("cpf_cnpj_sacado") \
        .agg(sum("valor_devido").alias("valor_sacado")) \
        .withColumn("share_pct", (col("valor_sacado") / lit(total_portfolio_value)) * 100)

    hhi_sacado_row = df_sacado_shares.select(sum(col("share_pct") * col("share_pct"))).collect()
    hhi_sacado = hhi_sacado_row[0][0] if hhi_sacado_row else 0.0
else:
    hhi_cedente = 0.0
    hhi_sacado = 0.0

# Preparando resultado
today_py = datetime.date.today()
data_hhi = [
    (today_py, "CEDENTE", float(hhi_cedente)),
    (today_py, "SACADO", float(hhi_sacado))
]

df_hhi = spark.createDataFrame(data_hhi, ["data_referencia", "tipo_concentracao", "hhi"])

# Interpretação
df_hhi_final = df_hhi.withColumn("interpretacao",
    when(col("hhi") < 1500, "Carteira diversificada (Saudável)")
    .when(col("hhi") > 2500, "Risco alto de quebra se um grande player falhar")
    .otherwise("Concentração Moderada")
)

# Salvar
output_path_hhi = TableNames.GOLD_METRICAS_CARTEIRA_HHI
df_hhi_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_hhi)
print(f"Métricas HHI calculadas e salvas em: {output_path_hhi}")
print(f"HHI Cedente: {hhi_cedente}")
print(f"HHI Sacado: {hhi_sacado}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
