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
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Notebook de Preparação Silver - Operações
# **Objetivo:** Processamento da tabela `tab_operacoes`, `tab_operacoes_devolucoes` e `tab_operacoes_tarifas_extras`.
# **Estratégia:** Implementa carga incremental para tabelas principais.

# MARKDOWN ********************

# ## Seção 0: Configuração do ambiente

# CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# --- CONFIGURATION ---
# Set to True to force a Full Load (useful for cleaning up deleted records from source)
FULL_LOAD = True
# ---------------------

from pyspark.sql.window import Window
from pyspark.sql.functions import (
    row_number, col, when, lit, concat, length, regexp_replace,
    collect_list, concat_ws, upper, greatest, substring, year,
    lead, date_add, lag, max, coalesce, date_sub, array_contains, create_map, split,
    to_date, trim, udf, pandas_udf
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from delta.tables import *
from notebookutils import mssparkutils
import datetime
import re
import unicodedata
import pandas as pd

def normalize_col(col_name):
    nfkd_form = unicodedata.normalize('NFKD', str(col_name))
    col_name = u"".join([c for c in nfkd_form if not unicodedata.combining(c)])
    if col_name.isupper():
        col_name = col_name.lower()
    else:
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', col_name)
        col_name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    col_name = col_name.lower()
    col_name = re.sub(r'[^a-z0-9_]+', '_', col_name)
    col_name = re.sub(r'_+', '_', col_name)
    return col_name.strip('_')

def check_is_incremental(spark, output_path, required_col):
    if DeltaTable.isDeltaTable(spark, output_path):
        try:
            if required_col in spark.read.format("delta").load(output_path).columns:
                return True
            else:
                print(f"Schema mismatch for {output_path}. Forcing Full Load.")
        except Exception:
             print(f"Error checking delta table schema for {output_path}. Forcing Full Load.")
    return False

def check_should_skip(spark, source_table, target_table_path, watermark_col="data_inclusao", target_watermark_col=None):
    try:
        if FULL_LOAD:
            return False

        if target_watermark_col is None:
            target_watermark_col = watermark_col

        if not DeltaTable.isDeltaTable(spark, target_table_path):
            return False # Target doesn't exist, proceed

        # Check source max
        df_source = spark.read.table(source_table)
        # 🧠 Tensor: Cache columns metadata in O(1) dictionary to prevent multiple driver fetch calls
        cols_source_map = {c.lower(): c for c in df_source.columns}
        if watermark_col.lower() not in cols_source_map:
             return False # Cannot check, proceed

        actual_col_source = cols_source_map[watermark_col.lower()]
        # 🧠 Tensor: Replace .collect()[0][0] with .first()[0] to preserve predicate pushdown and avoid list materialization
        max_source = df_source.agg(max(col(actual_col_source))).first()[0]

        # Check target max
        df_target = spark.read.format("delta").load(target_table_path)
        cols_target_map = {c.lower(): c for c in df_target.columns}
        if target_watermark_col.lower() not in cols_target_map:
             return False # Cannot check, proceed

        actual_col_target = cols_target_map[target_watermark_col.lower()]
        max_target = df_target.agg(max(col(actual_col_target))).first()[0]

        if max_source and max_target and max_source <= max_target:
            return True # Source is not newer than target
        return False
    except Exception as e:
        print(f"Warning in check_should_skip: {e}")
        return False

source_lakehouse = "LH_Bronze"
target_lakehouse = "LH_Silver"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 1: Limpeza de `tab_operacoes` (Incremental)

# CELL ********************

def get_operacoes_schema(df):
    return df.select(
        col("CODOPERACAO").alias("cod_operacao"),
        col("CODCLIENTE").alias("cod_cliente"),
        col("CODEMPRESA").alias("cod_empresa"),
        col("DATAINCLUSAO").alias("data_inclusao"),
        col("DATAALTERACAO").alias("data_alteracao"),
        col("DATAANALISE").alias("data_analise"),
        col("STATUSACEITE").alias("status_aceite"),
        col("STATUSANALISE").alias("status_analise"),
        col("CODBROKER").alias("cod_broker"),
        col("NBORDERO").alias("nbordero"),
        col("NOTASERVICO").alias("nota_servico"),
        col("TTO").alias("tto"),
        col("STTO").alias("stto"),
        col("chave_produto"),
        col("TOTRETENCAO").alias("valor_retido"),
        col("TOTDES").alias("valor_desembolsado"),
        col("TOTFAC").alias("valor_de_face"),
        col("TOTDCP").alias("desagio"),
        col("TOTTAR").alias("total_de_tarifas"),
        col("TOTPENDENCIAS").alias("valor_pendencias"),
        col("TOTRECOMPRA").alias("valor_recomprado"),
        col("FATOR").alias("taxa"),
        col("CODINDEFERIMENTO").alias("cod_indeferimento"),
        col("USUAINCLUSAO").alias("usua_inclusao"),
        col("USUASTANALISE").alias("usua_st_analise"),
        col("USUATRAVA").alias("usua_trava"),
        col("TAC").alias("tac"),
        col("TOTTAXAADM").alias("valor_taxa_adm"),
        col("TOTADVAL").alias("valor_advalorem"),
        col("NDOCSRECOMPRA").alias("n_docs_recompra"),
        col("TARIFA").alias("tarifa"),
        col("NDOCS").alias("n_docs"),
        col("TARIFARECOMPRA").alias("tarifa_recompra"),
        col("FLOATING").alias("floating"),
        col("PMP").alias("prazo_medio_ponderado_dias")
    )

def transform_operacoes(df, key_columns_operacoes):
    df_corrigido = df.withColumn("TTO_corrigido", when(col("CODOPERACAO").isin(3042074, 6048450, 6048449), lit("CS")).otherwise(col("TTO"))).drop("TTO").withColumnRenamed("TTO_corrigido", "TTO")

    windowSpec = Window.partitionBy([col(c) for c in key_columns_operacoes]).orderBy(col("DATAALTERACAO").desc())
    df_ranked = df_corrigido.withColumn("row_num", row_number().over(windowSpec))
    df_dedup = df_ranked.filter(col("row_num") == 1).drop("row_num")

    df_com_chave = df_dedup.withColumn("chave_produto", concat(col("TTO"), coalesce(col("STTO"),lit(""))))
    return get_operacoes_schema(df_com_chave)

def process_incremental_operacoes(source_table, output_path, key_columns_operacoes):
    print("Modo Incremental: Operações")
    delta_table_ops = DeltaTable.forPath(spark, output_path)

    # 1. Watermark (Optimized: Avoid collect())
    df_watermark = spark.read.format("delta").load(output_path) \
        .select(greatest(max("data_inclusao"), max("data_alteracao")).alias("max_date")) \
        .select(coalesce(col("max_date"), lit("1900-01-01")).alias("last_watermark"))

    print("Calculando Watermark Operações distribuído...")

    # 2. Read Bronze Filtered
    df_bronze_ops = spark.read.table(source_table) \
        .crossJoin(df_watermark) \
        .filter((col("DATAINCLUSAO") >= col("last_watermark")) | (col("DATAALTERACAO") >= col("last_watermark"))) \
        .drop("last_watermark")

    # 🧠 Tensor Optimization: Replace count() > 0 with not df.isEmpty() to avoid full data scan
    if not df_bronze_ops.isEmpty():
        # 3. Transform & Deduplicate Batch
        df_final_batch = transform_operacoes(df_bronze_ops, key_columns_operacoes)

        # 4. Merge
        # Compatibility check for merge condition (handles schema migration if target is still old schema)
        merge_condition = "t.cod_operacao = s.cod_operacao"

        delta_table_ops.alias("t").merge(
            df_final_batch.alias("s"),
            merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print("Merge Operações concluído.")
    else:
        print("Sem novas operações.")

def process_full_operacoes(source_table, output_path, key_columns_operacoes):
    print("Modo Full Load: Operações")
    df_bronze_ops = spark.read.table(source_table)

    df_final = transform_operacoes(df_bronze_ops, key_columns_operacoes)

    df_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path)

def process_operacoes():
    source_table_operacoes = f"{source_lakehouse}.tab_operacoes"
    target_table_operacoes = "staging_operacoes_limpa"
    output_path_operacoes = f"{target_lakehouse}.{target_table_operacoes}"

    print(f"Iniciando processamento de {target_table_operacoes}...")

    key_columns_operacoes = ["CODOPERACAO"]

    if not FULL_LOAD and check_is_incremental(spark, output_path_operacoes, "cod_operacao"):
        process_incremental_operacoes(source_table_operacoes, output_path_operacoes, key_columns_operacoes)
    else:
        if FULL_LOAD:
            print("Forcing Full Load (FULL_LOAD = True)...")
        process_full_operacoes(source_table_operacoes, output_path_operacoes, key_columns_operacoes)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 2: Limpeza de `tab_operacoes_devolucoes` (Incremental)

# CELL ********************

def transform_devolucoes(df):
    window_devolucoes = Window.partitionBy("CODTITULO").orderBy(col("DATAALTERACAO").desc())
    df_dedup = df.withColumn("row_num", row_number().over(window_devolucoes)) \
        .filter(col("row_num") == 1).drop("row_num") \
        .drop("USUAINCLUSAO", "DATAALTERACAO", "USUAALTERACAO", "CODTITULOBAIXA") \
        .withColumnRenamed("CODTITULO", "cod_titulo") \
        .withColumnRenamed("DATAINCLUSAO", "data_inclusao") \
        .withColumnRenamed("CODOPERACAO", "cod_operacao")

    # Garantir snake_case em todas as colunas
    return df_dedup.select([col(c).alias(c.lower()) for c in df_dedup.columns])

def process_incremental_devolucoes(source_table, output_path):
    print("Modo Incremental: Devoluções")
    delta_table_dev = DeltaTable.forPath(spark, output_path)

    # 🧠 Tensor Optimization: Avoid collect() by using crossJoin
    df_watermark = spark.read.format("delta").load(output_path) \
        .agg(coalesce(max("data_inclusao"), lit("1900-01-01")).alias("last_watermark"))

    print("Calculando Watermark Devoluções distribuído...")

    df_bronze_dev = spark.read.table(source_table) \
        .crossJoin(df_watermark) \
        .filter((col("DATAINCLUSAO") >= col("last_watermark")) | (col("DATAALTERACAO") >= col("last_watermark"))) \
        .drop("last_watermark")

    # 🧠 Tensor Optimization: Replace count() > 0 with not df.isEmpty() to avoid full data scan
    if not df_bronze_dev.isEmpty():
        df_final = transform_devolucoes(df_bronze_dev)

        delta_table_dev.alias("t").merge(
            df_final.alias("s"),
            "t.cod_titulo = s.cod_titulo"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print("Merge Devoluções concluído.")
    else:
        print("Sem novas devoluções.")

def process_full_devolucoes(source_table, output_path):
    print("Modo Full Load: Devoluções")
    df_bronze_devolucoes = spark.read.table(source_table)
    df_transformed_devolucoes = transform_devolucoes(df_bronze_devolucoes)
    df_transformed_devolucoes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path)

def process_devolucoes():
    source_table_devolucoes = f"{source_lakehouse}.tab_operacoes_devolucoes"
    target_table_devolucoes = "staging_operacoes_devolucoes_limpa"
    output_path_devolucoes = f"{target_lakehouse}.{target_table_devolucoes}"

    print(f"Iniciando processamento de {target_table_devolucoes}...")

    if check_is_incremental(spark, output_path_devolucoes, "cod_titulo"):
        try:
            process_incremental_devolucoes(source_table_devolucoes, output_path_devolucoes)
        except Exception as e:
            print(f"Erro no incremental (provavelmente falta de coluna de data): {e}. Fallback para Full Load.")
            process_full_devolucoes(source_table_devolucoes, output_path_devolucoes)
    else:
        process_full_devolucoes(source_table_devolucoes, output_path_devolucoes)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 3: TAC M (Full Load)
# **Estratégia:** Tabela pequena e filtrada por ano (2024+).

# CELL ********************

def get_tac_variations():
    return ["TAC  M", "TAC MOP", "TAC M.", "TACM", "TACA M", "TAC M 300,00", "TAC"]

def transform_tac_m(df, tac_variations):
    # Optimize: Use native trim instead of regex, and filter with isin instead of conditional replace
    df = df.withColumn("descricao", trim(upper(col("descricao"))))
    df = df.filter(col("descricao").isin(tac_variations + ["TAC M"]))
    df = df.withColumn("descricao", lit("TAC M"))
    return df.orderBy(col("data_inclusao").desc())

def process_tac_m():
    print("Processando TAC M...")
    source_table = f"{source_lakehouse}.tab_operacoes_tarifas_extras"
    target_path = f"{target_lakehouse}.staging_tac_m"

    if check_should_skip(spark, source_table, target_path, "DATAINCLUSAO"):
        print("Skipping TAC M (No new data)")
        return

    df_tac = spark.read.table(source_table)
    df_tac_renamed = df_tac \
        .filter(year(col("DATAINCLUSAO")) >= 2024) \
        .select(
            col("CODTARIFAEXTRA").alias("cod_tarifa_extra"), col("CODOPERACAO").alias("cod_operacao"), col("DESCRICAO").alias("descricao"), col("TOTAL").alias("total"), col("DATAINCLUSAO").alias("data_inclusao"), col("USUAINCLUSAO").alias("usua_inclusao")
        )

    tac_variations = get_tac_variations()
    df_tac_cleaned = transform_tac_m(df_tac_renamed, tac_variations)

    df_tac_cleaned.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.staging_tac_m")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 4: Estudo Op
# **Objetivo:** Ingestão simples de `tab_estudo_op`.

# CELL ********************

def standardize_estudo_columns(df):
    """
    Padroniza as colunas de Risco e Limite da tabela de estudo de operações.
    Procura por candidatos conhecidos e renomeia para um padrão único.
    """
    # Candidates must be in snake_case (normalized)
    risk_candidates = ["valoremabertort", "risco", "vl_risco", "valor_risco", "total_risco", "risco_total", "saldo_devedor", "tot_risco"]
    limit_candidates = ["limitefomento", "limite", "vl_limite", "valor_limite", "total_limite", "limite_total", "limite_global", "tot_limite", "limite_credito"]

    def rename_first_match(df, candidates, target_name):
        existing_cols = df.columns
        # Check if target already exists (e.g. if source already has 'valor_risco_estudo')
        if target_name in existing_cols:
             return df

        for cand in candidates:
            if cand in existing_cols:
                return df.withColumnRenamed(cand, target_name)

        # If not found, create with 0
        print(f"AVISO: Coluna padrão '{target_name}' não encontrada nos candidatos {candidates}. Criando com 0.")
        return df.withColumn(target_name, lit(0))

    df = rename_first_match(df, risk_candidates, "valor_risco_estudo")
    df = rename_first_match(df, limit_candidates, "valor_limite_estudo")
    return df

def process_estudo_op():
    print("Processando Estudo Op...")
    source_table = f"{source_lakehouse}.tab_estudo_op"
    target_path = f"{target_lakehouse}.staging_estudo_operacoes"

    if check_should_skip(spark, source_table, target_path, "DATAINCLUSAO"):
        print("Skipping Estudo Op (No new data)")
        return

    df_estudo = spark.read.table(source_table)

    # Apply normalization
    new_cols = [col(c).alias(normalize_col(c)) for c in df_estudo.columns]
    df_estudo_clean = df_estudo.select(new_cols)

    # Standardize columns (Risco & Limite)
    df_estudo_standard = standardize_estudo_columns(df_estudo_clean)

    df_estudo_standard.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.staging_estudo_operacoes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 5: Pareceres de Operações
# **Objetivo:** Limpeza e extração de flags de alçada e HTML.

# CELL ********************

# 1. Função Pandas UDF para decodificar entidades HTML (&Ccedil; -> Ç, &nbsp; -> espaço)
# Isso substitui aquele dicionário manual gigante e cobre TODOS os casos possíveis com melhor performance (Arrow).

@pandas_udf(StringType())
def unescape_udf(text: pd.Series) -> pd.Series:
    import html
    # Usa Arrow (Pandas UDF) e métodos vetorizados do Pandas para evitar overhead de serialização Python linha a linha.
    # Aplica html.unescape apenas nas entidades encontradas (preserva None/NaN de forma nativa).
    return text.str.replace(r'&[a-zA-Z0-9#]+;', lambda m: html.unescape(m.group(0)), regex=True)

# Registra a função para o Spark usar (já feito pelo decorator)
# unescape_udf já é invocável no contexto do Spark

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def process_pareceres_operacoes():
    print("Processando Pareceres Operações...")
    source_table = f"{source_lakehouse}.cad_geral_pareceres"
    target_path = f"{target_lakehouse}.staging_pareceres_operacoes"

    if check_should_skip(spark, source_table, target_path, "DATAINCLUSAO"):
        print("Skipping Pareceres Operações (No new data)")
        return

    df_pareceres = spark.read.table(source_table).alias("cgp")
    df_usuarios = spark.read.table(f"{source_lakehouse}.cad_usuarios").alias("cu")
    df_operacoes_ref = spark.read.table(f"{target_lakehouse}.staging_operacoes_limpa").alias("to2")

# Filtrar
    df_pareceres_filtered = df_pareceres.filter(
        col("OBS").isNotNull() & col("CODOPERACAO").isNotNull() &
        (col("CODTIPOPARECER") == 10) & (year(col("DATAINCLUSAO")) >= 2024) &
        (~col("OBS").like("%<img alt=%"))
    )

# Join
    df_joined = df_pareceres_filtered \
        .join(df_usuarios, col("cgp.USUAINCLUSAO") == col("cu.CODUSUARIO")) \
        .join(df_operacoes_ref, col("cgp.CODOPERACAO") == col("to2.cod_operacao")) \
        .select(
            col("cgp.CODOPERACAO").alias("cod_operacao"),
            col("cgp.DATAINCLUSAO").alias("data_inclusao"),
            col("cu.APELIDO").alias("apelido_usuario"),
            col("cgp.OBS").cast("string").alias("parecer_original") # Mantemos a original a pedido do usuario (cast para string)
        )

    # HTML Cleaning Logic (Replicating Power Query ReplaceValues)
    placeholder = "__NEWLINE__"

    # Optimização: Encadeamento de transformações para reduzir nós no plano lógico e overhead
    obs_col = col("parecer_original")
    # 1. Marcar quebras de linha (<br>, </p>, </div>, </li>, </tr>) - Regex case-insensitive
    obs_col = regexp_replace(obs_col, "(?i)<br\\s*/?>|</p>|</div>|</li>|</tr>", placeholder)
    # 2. Remover TODAS as tags HTML restantes
    obs_col = regexp_replace(obs_col, "<[^>]+>", " ")
    # 3. Decodificar caracteres especiais (Pandas UDF)
    obs_col = unescape_udf(obs_col)
    # 4. Normalizar espaços (Squash spaces)
    obs_col = regexp_replace(obs_col, "\\s+", " ")
    # 5. Restaurar quebras de linha (substituir placeholder por \n) e trim
    obs_col = trim(regexp_replace(obs_col, placeholder, "\n"))

    df_cleaned = df_joined.withColumn("Parecer", obs_col)

# --- LÓGICA DE FLAGS (Aplicada já no texto limpo) ---
    # Dica: Use (?i) no rlike para ignorar maiúscula/minúscula (case insensitive)
    
    df_final_pareceres = df_cleaned.withColumn("ESCROW", when(col("Parecer").rlike("(?i)#?ESCROW"), True).otherwise(False)) \
        .withColumn("ALCADA_SPENCER", when(col("Parecer").rlike("(?i)SPENCER"), "sim").otherwise("não")) \
        .withColumn("ALCADA_CAIO", when(col("Parecer").rlike("(?i)CAIO"), "sim").otherwise("não")) \
        .withColumn("ALCADA_DAIANE", when(col("Parecer").rlike("(?i)DAIANE"), "sim").otherwise("não")) \
        .withColumn("IS_LIMITE_PLUS", when(col("Parecer").rlike("(?i)#PLUS"), "SIM").otherwise("NAO"))

    # Gravação
    df_final_pareceres.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.staging_pareceres_operacoes")
    print("Pareceres processados com sucesso!")

# Executar
# process_pareceres_operacoes()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 6: Operações Escrow
# **Objetivo:** Identificar operações Escrow cruzando TTO e Pareceres.

# CELL ********************

def process_escrow():
    print("Processando Escrow...")
    df_ops_cm = spark.read.table(f"{target_lakehouse}.staging_operacoes_limpa") \
        .filter((col("TTO") == "CM") & (col("cod_operacao") != 6031344)) \
        .select("cod_operacao", "TTO", "STTO")

    df_pareceres_ops = spark.read.table(f"{target_lakehouse}.staging_pareceres_operacoes")

    df_escrow = df_ops_cm.withColumn("produtoEscrow", when(col("STTO").isin(["EB", "ED", "ET"]), 1).otherwise(0)) \
        .join(df_pareceres_ops, "cod_operacao", "left") \
        .select(
            "cod_operacao", "data_inclusao", "ESCROW", "ALCADA_SPENCER", "ALCADA_CAIO", "ALCADA_DAIANE", "produtoEscrow"
        ) \
        .withColumn("ESCROW", greatest(col("ESCROW").cast("int"), col("produtoEscrow")).cast("boolean")) \
        .drop("produtoEscrow") \
        .orderBy(col("cod_operacao").desc())

    df_escrow.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.staging_operacoes_escrow")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 7: Prorrogações
# **Objetivo:** Join de Operações (PR) com Boletos.

# CELL ********************

def process_prorrogacoes():
    print("Processando Prorrogações...")
    source_table = f"{target_lakehouse}.staging_operacoes_limpa"
    target_path = f"{target_lakehouse}.staging_prorrogacoes"

    if check_should_skip(spark, source_table, target_path, "data_inclusao"):
        print("Skipping Prorrogações (No new data)")
        return

    # Removed try-except to ensure fail-fast if dependencies are missing
    df_boletos = spark.read.table(f"{target_lakehouse}.staging_boletos_titulos")
    df_ops_pr = spark.read.table(source_table).filter(col("TTO") == "PR")

    df_prorrogacoes = df_ops_pr.join(df_boletos, "cod_operacao", "left") \
        .select(
            df_ops_pr["cod_operacao"], "cod_titulo", "n_doc", "cpf_cnpj_sacado", "cpf_cnpj_cedente",
            "valor", "amortizacoes", "liquidacao"
        )
    df_prorrogacoes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.staging_prorrogacoes")

def process_tab_operacoes_prorrogacao():
    print("Processando Tab Operações Prorrogação...")
    source_table = f"{source_lakehouse}.tab_operacoes_prorrogacao"
    target_table = "staging_operacoes_prorrogacao_limpa"
    output_path = f"{target_lakehouse}.{target_table}"

    if check_should_skip(spark, source_table, output_path, "DATAINCLUSAO", "data_inclusao"):
        print(f"Skipping {target_table} (No new data)")
        return

    # 1. Source Data
    df_prorrogacao = spark.read.table(source_table)

    # 2. Dependencies (Silver)
    # Using Silver tables for enrichment as they are cleaner
    # Get VALOR from titulos
    df_titulos = spark.read.table(f"{target_lakehouse}.staging_titulos_limpa") \
        .select(col("cod_titulo"), col("valor"))

    # Get STATUS from operacoes
    df_operacoes = spark.read.table(f"{target_lakehouse}.staging_operacoes_limpa") \
        .select(col("cod_operacao"), col("status_analise"), col("status_aceite"))

    # 3. Standardize Source to match Silver keys
    # Normalize source columns to snake_case first for consistency
    # (Assuming the source has CamelCase or UPPERCASE columns as per usual Bronze)
    df_prorrogacao_norm = df_prorrogacao.select(
        [col(c).alias(c.lower()) for c in df_prorrogacao.columns]
    ).withColumnRenamed("codtitulo", "cod_titulo") \
     .withColumnRenamed("codoperacao", "cod_operacao") \
     .withColumnRenamed("datainclusao", "data_inclusao")

    # 4. Joins
    # Left Join with Titulos
    df_joined_1 = df_prorrogacao_norm.join(df_titulos, "cod_titulo", "left_outer")

    # Left Join with Operacoes
    df_joined_2 = df_joined_1.join(df_operacoes, "cod_operacao", "left_outer")

    # 5. Transformations
    # Extract Data (Date part of data_inclusao)
    df_transformed = df_joined_2.withColumn("data", to_date(col("data_inclusao")))

    # 6. Select and Drop Columns
    # User requested to remove: TARIFA, USUAINCLUSAO, DATAALTERACAO, USUAALTERACAO, VALORDEVIDO, VALORPROR, VALORBOLETO
    # Columns are lowercased (but not snake_cased with underscores) by step 3: tarifa, usuainclusao, dataalteracao, usuaalteracao, valordevido, valorpror, valorboleto
    columns_to_drop = [
        "tarifa", "usuainclusao", "dataalteracao", "usuaalteracao",
        "valordevido", "valorpror", "valorboleto"
    ]
    df_final = df_transformed.drop(*columns_to_drop)

    target_table = "staging_operacoes_prorrogacao_limpa"
    df_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.{target_table}")
    print(f"Tabela {target_table} criada com sucesso (enriched).")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 8: Recompras
# **Objetivo:** Join de Operações (RC/RE) com Boletos.

# CELL ********************

def process_recompras():
    print("Processando Recompras...")
    source_table = f"{target_lakehouse}.staging_operacoes_limpa"
    target_path = f"{target_lakehouse}.staging_operacoes_recompras"

    if check_should_skip(spark, source_table, target_path, "data_inclusao"):
        print("Skipping Recompras (No new data)")
        return

    # Removed try-except to ensure fail-fast if dependencies are missing
    df_boletos = spark.read.table(f"{target_lakehouse}.staging_boletos_titulos")
    df_ops_rc = spark.read.table(source_table) \
        .filter(col("TTO").isin(["RC", "RE"])) \
        .filter((col("status_analise") == "D") & (col("status_aceite") == "A"))

    df_recompras = df_ops_rc.join(df_boletos, "cod_operacao", "left") \
        .select(
            df_ops_rc["cod_operacao"], "cod_titulo", "n_doc", "cpf_cnpj_sacado", "cpf_cnpj_cedente",
            "valor", "amortizacoes", "liquidacao",
            concat(lit("40-"), col("cod_operacao")).alias("chave_base_operacao_recompra")
        )
    df_recompras.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.staging_operacoes_recompras")


def process_tarifas_esporadicas():
    print("Processando Tarifas Esporádicas...")

    df_tarifas = spark.read.table(f"{source_lakehouse}.tab_operacoes_tarifas_extras")
    df_usuarios = spark.read.table(f"{source_lakehouse}.cad_usuarios")

    # Filter Year >= 2024
    df_filtered = df_tarifas.filter(year(col("DATAINCLUSAO")) >= 2024)

    # Join Users (Left) & Filter Analyst
    # Note: Using aliases to avoid ambiguity if USUAINCLUSAO exists in both (though here we join on it)
    df_joined = df_filtered.alias("t").join(df_usuarios.alias("u"), col("t.USUAINCLUSAO") == col("u.CODUSUARIO"), "left") \
        .filter((col("u.NOME") != "RONALDO DANILO UREI GOBBI") | col("u.NOME").isNull())

    # Select & Transform
    df_final = df_joined.select(
        col("t.CODOPERACAO").alias("cod_operacao"),
        col("t.DESCRICAO").alias("descricao"),
        col("t.TOTAL").alias("total"),
        col("t.DATAINCLUSAO").alias("data_inclusao"),
        col("u.NOME").alias("analista")
    ).distinct()

    target_table = "staging_tarifas_esporadicas"
    df_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.{target_table}")
    print(f"Tabela {target_table} criada com sucesso.")

# Execução
process_operacoes()
process_devolucoes()
process_tac_m()
process_estudo_op()
process_pareceres_operacoes()
process_escrow()
process_prorrogacoes()
process_recompras()
process_tab_operacoes_prorrogacao()
process_tarifas_esporadicas()

print("Limpeza Silver - Operações finalizada.")
mssparkutils.notebook.exit("Success")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
