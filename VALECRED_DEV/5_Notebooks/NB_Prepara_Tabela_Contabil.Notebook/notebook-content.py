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

#
#
# # Notebook de Preparação Silver - Contábil
# **Objetivo:** Processamento da tabela `tab_lancamentos_contabeis`.
#
# **Estratégia:** Carga incremental baseada em `DATAINCLUSAO` e `DATAALTERACAO`.

# MARKDOWN ********************

# ## Seção 0: Configuração do ambiente

# CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

from pyspark.sql.window import Window
from pyspark.sql.functions import (
    row_number, col, when, lit, greatest, max
)
from pyspark.sql.utils import AnalysisException
from delta.tables import *
from notebookutils import mssparkutils

source_lakehouse = "LH_Bronze"
target_lakehouse = "LH_Silver"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 1: Limpeza de `tab_lancamentos_contabeis` (Incremental)
# **Objetivo:** Desduplicar, renomear para snake_case e atualizar na Silver.

# CELL ********************

source_table = "tab_lancamentos_contabeis"
target_table = "staging_lancamentos_contabeis"
target_table_full_name = f"{target_lakehouse}.{target_table}"

print(f"Iniciando processamento de {target_table}...")

# Função de seleção e renomeação de colunas
def select_lancamentos(df):
    # Lista de colunas esperadas e mapeamento para snake_case
    # Ajuste conforme o schema real da tabela de origem
    return df.select(
        col("CODLANCAMENTO").alias("cod_lancamento"),
        col("CODOPERACAO").alias("cod_operacao"),
        col("CODTITULO").alias("cod_titulo"),
        col("DATA").alias("data_lancamento"),
        col("HISTORICO").alias("historico"),
        col("DEBITO").alias("debito"),
        col("CREDITO").alias("credito"),
        col("VALOR").alias("valor"),
        col("DATAINCLUSAO").alias("data_inclusao"),
        col("DATAALTERACAO").alias("data_alteracao"),
        col("USUARIO").alias("usuario"),
        col("CODBANCO").alias("cod_banco"),
        col("CONTACONTABIL").alias("conta_contabil")
        # Adicione outras colunas conforme necessário
    )

key_columns = ["CODLANCAMENTO"]

# Verifica se a tabela de origem existe antes de prosseguir
try:
    spark.read.table(f"{source_lakehouse}.{source_table}").limit(1).collect()
except Exception as e:
    print(f"Tabela de origem {source_table} não encontrada ou inacessível.")
    print(f"Erro: {e}")
    # Se a tabela não existe, encerramos com sucesso (para não quebrar pipeline) mas avisando
    mssparkutils.notebook.exit("Source Table Not Found - Skipped")

# Verifica se a tabela destino existe e é compatível para incremental
is_incremental_possible = False
if spark.catalog.tableExists(target_table_full_name):
    try:
        target_cols = spark.read.table(target_table_full_name).columns
        if "cod_lancamento" in target_cols and "data_alteracao" in target_cols:
            is_incremental_possible = True
        else:
            print("Schema mismatch. Forcing Full Load.")
            is_incremental_possible = False
    except AnalysisException:
        print("Error accessing target table. Forcing Full Load.")
        is_incremental_possible = False
else:
    print("Tabela destino não existe. Forçando Full Load.")
    is_incremental_possible = False

if is_incremental_possible:
    print("Modo Incremental: Detectando alterações...")
    delta_table = DeltaTable.forName(spark, target_table_full_name)

    # 1. Obter Watermark
    watermark_row = spark.read.table(target_table_full_name) \
        .select(greatest(max("data_inclusao"), max("data_alteracao")).alias("max_date")) \
        .collect()

    last_watermark = "1900-01-01"
    if watermark_row and watermark_row[0][0]:
        last_watermark = watermark_row[0][0]

    print(f"Watermark aplicado: {last_watermark}")

    # 2. Ler Bronze filtrado
    df_bronze = spark.read.table(f"{source_lakehouse}.{source_table}") \
        .filter((col("DATAINCLUSAO") >= last_watermark) | (col("DATAALTERACAO") >= last_watermark))

    if df_bronze.count() > 0:
        # 3. Desduplicar
        df_with_latest = df_bronze.withColumn(
            "DATA_MAIS_RECENTE",
            greatest(col("DATAALTERACAO"), col("DATAINCLUSAO"))
        )
        windowSpec = Window.partitionBy([col(c) for c in key_columns]).orderBy(col("DATA_MAIS_RECENTE").desc())
        df_dedup = df_with_latest.withColumn("row_num", row_number().over(windowSpec)) \
            .filter(col("row_num") == 1).drop("row_num", "DATA_MAIS_RECENTE")

        df_final_batch = select_lancamentos(df_dedup)

        # 4. Merge
        print("Executando Merge...")
        delta_table.alias("t").merge(
            df_final_batch.alias("s"),
            "t.cod_lancamento = s.cod_lancamento"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print("Merge concluído.")
    else:
        print("Nenhum dado novo encontrado.")

else:
    print("Modo Full Load: Carga Inicial ou Atualização de Schema.")
    df_bronze = spark.read.table(f"{source_lakehouse}.{source_table}")

    df_with_latest = df_bronze.withColumn(
        "DATA_MAIS_RECENTE",
        greatest(col("DATAALTERACAO"), col("DATAINCLUSAO"))
    )
    windowSpec = Window.partitionBy([col(c) for c in key_columns]).orderBy(col("DATA_MAIS_RECENTE").desc())
    df_dedup = df_with_latest.withColumn("row_num", row_number().over(windowSpec)) \
        .filter(col("row_num") == 1).drop("row_num", "DATA_MAIS_RECENTE")

    df_final = select_lancamentos(df_dedup).orderBy(col("data_inclusao").desc())

    df_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table_full_name)
    print("Carga Full concluída.")

mssparkutils.notebook.exit("Success")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
