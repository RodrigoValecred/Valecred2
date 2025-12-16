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

# Parâmetro para forçar carga full (pode ser sobrescrito por pipeline ou widget se disponível)
# Em execução manual, altere o valor abaixo para "true"
force_full_load = "true"
p_force_full_load = str(force_full_load).lower() == "true"

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
    # Ajuste conforme o schema real da tabela de origem:
    # [CODCTBLAN, CODEMPRESA, CODTRANSACAO, DEBITO, CREDITO, CODFUNDO, CODCCUSTO, TIPO, DATA, VALOR, COMPLEMENTO, SISTEMA, DATAINCLUSAO, USUAINCLUSAO, DATAALTERACAO, USUAALTERACAO]
    return df.select(
        col("CODCTBLAN").alias("cod_lancamento"),
        col("CODEMPRESA").alias("cod_empresa"),
        col("CODTRANSACAO").alias("cod_transacao"),
        col("DEBITO").alias("debito"),
        col("CREDITO").alias("credito"),
        col("CODFUNDO").alias("cod_fundo"),
        col("CODCCUSTO").alias("cod_ccusto"),
        col("TIPO").alias("tipo"),
        col("DATA").alias("data_lancamento"),
        col("VALOR").alias("valor"),
        col("COMPLEMENTO").alias("complemento"),
        col("SISTEMA").alias("sistema"),
        col("DATAINCLUSAO").alias("data_inclusao"),
        col("USUAINCLUSAO").alias("usuario_inclusao"),
        col("DATAALTERACAO").alias("data_alteracao"),
        col("USUAALTERACAO").alias("usuario_alteracao")
    )

key_columns = ["CODCTBLAN"]

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

if p_force_full_load:
    print("PARAMETRO 'force_full_load' ATIVADO: Ignorando verificação incremental e forçando Carga Full.")
    is_incremental_possible = False
elif spark.catalog.tableExists(target_table_full_name):
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
    
    print(f"DEBUG: Watermark aplicado: {last_watermark}")
    
    # 2. Ler Bronze filtrado
    df_bronze = spark.read.table(f"{source_lakehouse}.{source_table}") \
        .filter((col("DATAINCLUSAO") >= last_watermark) | (col("DATAALTERACAO") >= last_watermark))
    
    bronze_count = df_bronze.count()
    print(f"DEBUG: Registros encontrados no Bronze (delta >= {last_watermark}): {bronze_count}")

    if bronze_count > 0:
        # 3. Desduplicar
        df_with_latest = df_bronze.withColumn(
            "DATA_MAIS_RECENTE",
            greatest(col("DATAALTERACAO"), col("DATAINCLUSAO"))
        )
        windowSpec = Window.partitionBy([col(c) for c in key_columns]).orderBy(col("DATA_MAIS_RECENTE").desc())
        df_dedup = df_with_latest.withColumn("row_num", row_number().over(windowSpec)) \
            .filter(col("row_num") == 1).drop("row_num", "DATA_MAIS_RECENTE")
            
        dedup_count = df_dedup.count()
        print(f"DEBUG: Registros após deduplicação: {dedup_count}")

        df_final_batch = select_lancamentos(df_dedup)
        
        # 4. Merge
        print("Executando Merge...")
        delta_table.alias("t").merge(
            df_final_batch.alias("s"),
            "t.cod_lancamento = s.cod_lancamento"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print("Merge concluído.")
    else:
        print("DEBUG: Nenhum dado novo encontrado para merge.")
        
else:
    print("Modo Full Load: Carga Inicial ou Atualização de Schema.")
    df_bronze = spark.read.table(f"{source_lakehouse}.{source_table}")
    
    raw_count = df_bronze.count()
    print(f"DEBUG: Registros lidos do Bronze (Total): {raw_count}")
    
    df_with_latest = df_bronze.withColumn(
        "DATA_MAIS_RECENTE",
        greatest(col("DATAALTERACAO"), col("DATAINCLUSAO"))
    )
    windowSpec = Window.partitionBy([col(c) for c in key_columns]).orderBy(col("DATA_MAIS_RECENTE").desc())
    df_dedup = df_with_latest.withColumn("row_num", row_number().over(windowSpec)) \
        .filter(col("row_num") == 1).drop("row_num", "DATA_MAIS_RECENTE")
    
    dedup_count = df_dedup.count()
    print(f"DEBUG: Registros após deduplicação: {dedup_count}")

    df_final = select_lancamentos(df_dedup).orderBy(col("data_inclusao").desc())

    final_count = df_final.count()
    print(f"DEBUG: Registros finais para escrita: {final_count}")

    try:
        # FIXED: Variable name typo corrected from f_final to df_final
        df_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table_full_name)
        print("Operação de escrita (overwrite) concluída com sucesso.")
        
        # Verificação pós-escrita
        actual_count = spark.read.table(target_table_full_name).count()
        print(f"DEBUG: Contagem final na tabela destino ({target_table_full_name}): {actual_count}")
        
        if actual_count != final_count:
            print(f"ALERTA: Discrepância detectada! Esperado: {final_count}, Encontrado: {actual_count}")
            
    except Exception as e:
        print(f"ERRO FATAL ao escrever na tabela destino: {e}")
        mssparkutils.notebook.exit(f"Write Failed: {e}")


mssparkutils.notebook.exit("Success")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
