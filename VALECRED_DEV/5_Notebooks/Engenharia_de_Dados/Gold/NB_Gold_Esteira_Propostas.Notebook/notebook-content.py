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

# MARKDOWN ********************

# # Notebook Gold: Esteira de Propostas
# **Objetivo:** Processamento incremental de pareceres para reconstruir a esteira de propostas (`LH_Gold.esteira_de_propostas`).
# **Dependências:** `NB_Prepara_Tabela_Cadastros` (Silver) e Tabelas Bronze.

# CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

from pyspark.sql.window import Window
from pyspark.sql.functions import (
    row_number, col, when, lit, concat, greatest, substring, year,
    lag, max, coalesce, trim
)
from pyspark.sql.types import LongType
from delta.tables import *
import datetime
import logging

# Configuração de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula de Leitura de Dependências
logger.info("Lendo tabelas necessárias...")
df_pareceres_raw = spark.read.table("LH_Bronze.cad_geral_pareceres")
df_clientes_staging = spark.read.table("LH_Silver.staging_clientes_limpa")
df_usuarios_raw = spark.read.table("LH_Bronze.cad_usuarios")
df_status_clientes_esteira = spark.read.table("LH_Silver.sup_status_de_clientes_da_esteira")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 4.1: Configuração e Watermark
# ------------------------------------------------
logger.info("Iniciando o processamento incremental de pareceres...")
target_pareceres_status_table_name = "LH_Silver.pareceres_de_alteracao_de_status"
target_esteira_table_name = "LH_Gold.esteira_de_propostas"
watermark_table_name = "LH_Silver.etl_watermark_control"
DEFAULT_WATERMARK = datetime.datetime(1900, 1, 1)
notebook_name = "NB_Gold_Esteira_Propostas"

try:
    df_watermark = spark.read.table(watermark_table_name)
    last_watermark_str = df_watermark.filter(col("TableName") == notebook_name).select("LastWatermarkValue").collect()
    if last_watermark_str:
        last_watermark = datetime.datetime.strptime(last_watermark_str[0][0].split('.')[0], "%Y-%m-%d %H:%M:%S")
        logger.info(f"Watermark encontrado: {last_watermark}")
    else:
        # Se não achou com nome novo, tenta com nome antigo para migração suave?
        # Melhor não, vamos reprocessar tudo para garantir consistencia neste novo notebook.
        last_watermark = DEFAULT_WATERMARK
        logger.info(f"Watermark não encontrado. Usando padrão: {last_watermark}.")
except Exception:
    last_watermark = DEFAULT_WATERMARK
    logger.info(f"Usando watermark padrão (erro na leitura): {last_watermark}.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 4.2: Leitura e Processamento Incremental
# ------------------------------------------------
df_pareceres_incremental = df_pareceres_raw.filter((col("DATAINCLUSAO") > last_watermark) | (col("DATAALTERACAO") > last_watermark)).cache()

logger.info(f"Colunas da df_pareceres_incremental: {df_pareceres_incremental.columns}")

# ⚡ Bolt Optimization: Substitui count() por isEmpty()
# 💡 O que: Substituição de `record_count = df_pareceres_incremental.count()` e verificação `> 0` por `has_new_records = not df_pareceres_incremental.isEmpty()`.
# 🎯 Por que: A ação `.count()` força a avaliação de todo o DataFrame, resultando em um full table scan que atrasa a execução se houver muitos dados ou até mesmo quando há nenhum dado mas muitas partições a serem varridas. `.isEmpty()` realiza apenas uma operação leve (equivalente a `limit(1)`) para checar a presença de registros, contornando a varredura integral.
# 📊 Impacto: Melhora significativa no tempo de execução do check inicial para incremental, que ocorre diariamente, reduzindo chamadas supérfluas de action no Catalyst Optimizer.
# 🔬 Medição: Ação avaliada localmente, economizando os segundos da materialização completa do DAG no momento do `count()`.
has_new_records = not df_pareceres_incremental.isEmpty()

if has_new_records:
    new_watermark = df_pareceres_incremental.agg(max(greatest(coalesce(col("DATAINCLUSAO"), lit(DEFAULT_WATERMARK)), coalesce(col("DATAALTERACAO"), lit(DEFAULT_WATERMARK))))).collect()[0][0]
    logger.info(f"Novos registros encontrados. Novo watermark: {new_watermark}")

    df_replica_pareceres_delta = df_pareceres_incremental.filter(year(col("DATAINCLUSAO")) >= 2024).drop("ENCAMINHAR", "ALERTA", "CODPASTA", "CODTAREFA", "USUAALTERACAO", "DATAALTERACAO").withColumn("OBS", col("OBS").substr(1, 255)).withColumn("codTipoParecer", col("CODTIPOPARECER").cast(LongType())).filter((col("codTipoParecer") == 1) & (col("CPFCNPJ").isNotNull()) & (col("CPFCNPJ") != "") & (col("OBS").isNotNull()) & (col("OBS") != "") & (col("USUAINCLUSAO").isNotNull()) & (col("DATAINCLUSAO").isNotNull())).filter(col("OBS").startswith("STATUS ALTERADO PARA ")).withColumn("STATUS_DO_CLIENTE", trim(substring(col("OBS"), 22, 100))).withColumn("BASE", lit(40).cast(LongType())).select("CODPARECER", "CPFCNPJ", "CODOPERACAO", "DATAINCLUSAO", "USUAINCLUSAO", "STATUS_DO_CLIENTE", "BASE")

    # Recuperar MAX INDICE por Cliente da tabela alvo, se existir, para continuar a sequencia
    if spark.catalog.tableExists(target_pareceres_status_table_name):
        df_max_indices = spark.read.table(target_pareceres_status_table_name).groupBy("CODCLIENTE").agg(max("INDICE").alias("max_indice"))
        # Garantir snake_case para join
        if "CODCLIENTE" in df_max_indices.columns:
            df_max_indices = df_max_indices.withColumnRenamed("CODCLIENTE", "cod_cliente")
    else:
        df_max_indices = None

    # Enriquecimento e Calculo de Indice
    df_joined = df_replica_pareceres_delta.join(df_clientes_staging.select("cpf_cnpj", "cod_cliente"), df_replica_pareceres_delta.CPFCNPJ == df_clientes_staging.cpf_cnpj, "left")

    if df_max_indices:
        df_joined = df_joined.join(df_max_indices, "cod_cliente", "left").withColumn("start_index", coalesce(col("max_indice"), lit(0)))
    else:
        df_joined = df_joined.withColumn("start_index", lit(0))

    window_cliente_data_delta = Window.partitionBy("cod_cliente").orderBy(col("DATAINCLUSAO").asc())

    df_pareceres_enriquecidos_delta = df_joined \
        .withColumn("chave_base_cliente", concat(col("BASE").cast("string"), lit("-"), col("cod_cliente").cast("string"))) \
        .join(df_usuarios_raw.select("CODUSUARIO", "NOME"), col("USUAINCLUSAO") == col("CODUSUARIO"), "left") \
        .withColumnRenamed("NOME", "USUARIO") \
        .join(df_status_clientes_esteira, "STATUS_DO_CLIENTE", "left") \
        .filter(col("cod_cliente").isNotNull() & (col("cod_cliente") != "")) \
        .withColumn("INDICE", col("start_index") + row_number().over(window_cliente_data_delta)) \
        .withColumn("chave_original", (col("INDICE") * 1000000000 + col("cod_cliente")).cast(LongType())) \
        .withColumnRenamed("DATAINCLUSAO", "DATALOG") \
        .select(
            col("CODPARECER"),
            col("cod_cliente").alias("CODCLIENTE"), 
            col("STATUS_DO_CLIENTE"),
            col("DATALOG"),
            col("BASE"),
            col("USUARIO"),
            col("chave_base_cliente"),
            col("INDICE"),
            col("chave_original"),
            col("MACROPROCESSO"),
            col("FASE")
        )

    if spark.catalog.tableExists(target_pareceres_status_table_name):
        logger.info(f"Executando Merge na tabela {target_pareceres_status_table_name}...")
        # Schema Evolution: Se houver colunas novas, permite a evolução
        delta_table = DeltaTable.forName(spark, target_pareceres_status_table_name)
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        delta_table.alias("t").merge(
            df_pareceres_enriquecidos_delta.alias("s"), 
            "t.CODPARECER = s.CODPARECER"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    else:
        logger.info(f"Criando tabela {target_pareceres_status_table_name} pela primeira vez...")
        df_pareceres_enriquecidos_delta.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_pareceres_status_table_name)
else:
    new_watermark = last_watermark
    logger.info("Nenhum dado novo encontrado.")

if 'df_pareceres_incremental' in locals():
    df_pareceres_incremental.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 4.3: Reconstrução da Esteira e Atualização do Watermark
# -------------------------------------------------------------
if has_new_records or not spark.catalog.tableExists(target_esteira_table_name):
    logger.info("Reconstruindo esteira_de_propostas...")
    df_pareceres_completa = spark.read.table(target_pareceres_status_table_name)
    window_lag = Window.partitionBy("CODCLIENTE").orderBy("DATALOG")
    df_com_lag = df_pareceres_completa.withColumn("STATUS_DO_CLIENTE_ANTERIOR", lag("STATUS_DO_CLIENTE").over(window_lag)).withColumn("DATALOG_ANTERIOR", lag("DATALOG").over(window_lag)).withColumn("MACROPROCESSO_ANTERIOR", lag("MACROPROCESSO").over(window_lag)).withColumn("FASE_ANTERIOR", lag("FASE").over(window_lag))
    df_transicoes = df_com_lag.filter(col("STATUS_DO_CLIENTE") != col("STATUS_DO_CLIENTE_ANTERIOR")).na.drop(subset=["STATUS_DO_CLIENTE_ANTERIOR"])
    df_esteira_final = df_transicoes.withColumn("DEVOLUCAO", when((col("MACROPROCESSO_ANTERIOR") == "CREDITO") & (col("MACROPROCESSO") == "COMERCIAL"), True).otherwise(False)).withColumn("RECEBIDA", when((col("MACROPROCESSO_ANTERIOR") == "COMERCIAL") & (col("MACROPROCESSO") == "CREDITO"), True).otherwise(False)).select(col("INDICE").alias("indice"), col("CODCLIENTE").alias("cod_cliente"), col("BASE").alias("base"), col("DATALOG_ANTERIOR").alias("datalog_anterior"), col("DATALOG").alias("datalog"), "chave_base_cliente", col("STATUS_DO_CLIENTE_ANTERIOR").alias("status_do_cliente_anterior"), col("STATUS_DO_CLIENTE").alias("status_do_cliente"), col("MACROPROCESSO_ANTERIOR").alias("macroprocesso_anterior"), col("MACROPROCESSO").alias("macroprocesso"), col("FASE_ANTERIOR").alias("fase_anterior"), col("FASE").alias("fase"), col("USUARIO").alias("usuario"), col("DEVOLUCAO").alias("devolucao"), col("RECEBIDA").alias("recebida"))
    df_esteira_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_esteira_table_name)
    logger.info("Esteira reconstruída.")

    logger.info("Atualizando watermark...")
    df_new_watermark = spark.createDataFrame([(notebook_name, new_watermark.strftime("%Y-%m-%d %H:%M:%S.%f"))], ["TableName", "LastWatermarkValue"])
    if spark.catalog.tableExists(watermark_table_name):
        DeltaTable.forName(spark, watermark_table_name).alias("t").merge(df_new_watermark.alias("s"), "t.TableName = s.TableName").whenMatchedUpdate(set={"LastWatermarkValue": "s.LastWatermarkValue"}).whenNotMatchedInsert(values={"TableName": "s.TableName", "LastWatermarkValue": "s.LastWatermarkValue"}).execute()
    else:
        df_new_watermark.write.mode("overwrite").saveAsTable(watermark_table_name)

logger.info("Processo Esteira Propostas Gold concluído.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
