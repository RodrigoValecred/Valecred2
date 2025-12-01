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

from pyspark.sql.window import Window
from pyspark.sql.functions import (
    row_number, col, when, lit, concat, length, regexp_replace,
    collect_list, concat_ws, upper, greatest, substring, year,
    lead, date_add, lag, max, coalesce, date_sub
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from delta.tables import *
from notebookutils import mssparkutils
import datetime

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

source_table_operacoes = "tab_operacoes"
target_table_operacoes = "staging_operacoes_limpa"
output_path_operacoes = f"{target_lakehouse}.{target_table_operacoes}"

print(f"Iniciando processamento de {target_table_operacoes}...")

key_columns_operacoes = ["CODOPERACAO"]
# Schema de Seleção
def select_operacoes(df):
    return df.select(
        "CODOPERACAO",
        "CODCLIENTE",
        "CODEMPRESA",
        "DATAINCLUSAO",
        "DATAALTERACAO",
        "DATAANALISE",
        "STATUSACEITE",
        "STATUSANALISE",
        "CODBROKER",
        "NOTASERVICO",
        "TTO",
        "STTO",
        "chave_produto",
        "TOTRETENCAO",
        "TOTDES",
        "TOTFAC",
        "TOTDCP",
        "TOTTAR",
        "TOTRECOMPRA"
    )

if DeltaTable.isDeltaTable(spark, output_path_operacoes):
    print("Modo Incremental: Operações")
    delta_table_ops = DeltaTable.forPath(spark, output_path_operacoes)
    
    # 1. Watermark
    watermark_row = spark.read.format("delta").load(output_path_operacoes) \
        .select(greatest(max("DATAINCLUSAO"), max("DATAALTERACAO")).alias("max_date")) \
        .collect()
        
    last_watermark = "1900-01-01"
    if watermark_row and watermark_row[0][0]:
        last_watermark = watermark_row[0][0]
        
    print(f"Watermark Operações: {last_watermark}")
    
    # 2. Read Bronze Filtered
    df_bronze_ops = spark.read.table(f"{source_lakehouse}.{source_table_operacoes}") \
        .filter((col("DATAINCLUSAO") >= last_watermark) | (col("DATAALTERACAO") >= last_watermark))
        
    if df_bronze_ops.count() > 0:
        # 3. Transform & Deduplicate Batch
        df_corrigido = df_bronze_ops.withColumn("TTO_corrigido", when(col("CODOPERACAO") == 3042074, lit("CS")).otherwise(col("TTO"))).drop("TTO").withColumnRenamed("TTO_corrigido", "TTO")
        
        windowSpec = Window.partitionBy([col(c) for c in key_columns_operacoes]).orderBy(col("DATAALTERACAO").desc())
        df_ranked = df_corrigido.withColumn("row_num", row_number().over(windowSpec))
        df_dedup = df_ranked.filter(col("row_num") == 1).drop("row_num")
        
        df_com_chave = df_dedup.withColumn("chave_produto", concat(col("TTO"), col("STTO")))
        df_final_batch = select_operacoes(df_com_chave)
        
        # 4. Merge
        delta_table_ops.alias("t").merge(
            df_final_batch.alias("s"),
            "t.CODOPERACAO = s.CODOPERACAO"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print("Merge Operações concluído.")
    else:
        print("Sem novas operações.")

else:
    print("Modo Full Load: Operações")
    df_bronze_ops = spark.read.table(f"{source_lakehouse}.{source_table_operacoes}")
    
    df_corrigido = df_bronze_ops.withColumn("TTO_corrigido", when(col("CODOPERACAO") == 3042074, lit("CS")).otherwise(col("TTO"))).drop("TTO").withColumnRenamed("TTO_corrigido", "TTO")
    windowSpec = Window.partitionBy([col(c) for c in key_columns_operacoes]).orderBy(col("DATAALTERACAO").desc())
    df_ranked = df_corrigido.withColumn("row_num", row_number().over(windowSpec))
    df_dedup = df_ranked.filter(col("row_num") == 1).drop("row_num")
    
    df_com_chave = df_dedup.withColumn("chave_produto", concat(col("TTO"), col("STTO")))
    df_final = select_operacoes(df_com_chave)
    
    df_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_operacoes)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 2: Limpeza de `tab_operacoes_devolucoes` (Incremental)

# CELL ********************


source_table_devolucoes = "tab_operacoes_devolucoes"
target_table_devolucoes = "staging_operacoes_devolucoes_limpa"
output_path_devolucoes = f"{target_lakehouse}.{target_table_devolucoes}"

print(f"Iniciando processamento de {target_table_devolucoes}...")

if DeltaTable.isDeltaTable(spark, output_path_devolucoes):
    print("Modo Incremental: Devoluções")
    delta_table_dev = DeltaTable.forPath(spark, output_path_devolucoes)
    
    # Watermark: Usar DATAINCLUSAO ou DATAALTERACAO
    # A tabela target DROPPA DATAALTERACAO e USUAINCLUSAO. Precisamos de cuidado.
    # Mas podemos ler o LOG de transação do Delta para saber o último commit? Não, muito complexo.
    # Vamos usar DATAINCLUSAO se existir, ou se a tabela não tiver colunas de tempo, Full Load é mais seguro?
    # O schema de saida é: sem DATAALTERACAO.
    # Se eu não guardo DATAALTERACAO na Silver, não posso filtrar por ela na próxima execução incremental baseada na Silver.
    # ALTERNATIVA: Adicionar DATAALTERACAO à Silver.
    # VAMOS ADICIONAR DATAALTERACAO AGORA.
    
    # Mas se a tabela já existe sem a coluna, o merge falha (Schema Mismatch).
    # O Spark Delta Schema Evolution pode lidar com isso se 'mergeSchema' estiver ativo.
    
    # Assumindo que queremos manter o padrão, vamos usar Full Overwrite para Devolucoes se não pudermos alterar o schema facilmente.
    # Devoluções costumam ser voláteis?
    # Vou manter INCREMENTAL mas vou ter que mudar a lógica de leitura da watermark.
    # Se não tem coluna de data na Silver, não dá pra fazer incremental baseado em data.
    # Vou mudar para Full Load para Devolucoes POR ENQUANTO, para evitar quebrar schema existente.
    # O usuário pediu "tratar menos dados", mas sem mudar schema é arriscado.
    # Porém, vou implementar Full Load otimizado (apenas colunas necessárias).
    
    # Revisitando: A tabela original dropa: USUAINCLUSAO, DATAALTERACAO, USUAALTERACAO, CODTITULOBAIXA.
    # Provavelmente mantém DATAINCLUSAO?
    # Bronze Schema: ...
    # Original code dropped explicit list. Se DATAINCLUSAO não estava na lista de drop, ela ficou.
    # Vou assumir que DATAINCLUSAO está lá.
    
    try:
        watermark_row = spark.read.format("delta").load(output_path_devolucoes) \
            .agg(max("DATAINCLUSAO").alias("max_date")).collect()
        last_watermark = watermark_row[0][0] if watermark_row and watermark_row[0][0] else "1900-01-01"
        
        print(f"Watermark Devoluções: {last_watermark}")
        
        df_bronze_dev = spark.read.table(f"{source_lakehouse}.{source_table_devolucoes}") \
            .filter((col("DATAINCLUSAO") >= last_watermark) | (col("DATAALTERACAO") >= last_watermark))
            
        if df_bronze_dev.count() > 0:
            window_devolucoes = Window.partitionBy("CODTITULO").orderBy(col("DATAALTERACAO").desc())
            df_dedup = df_bronze_dev.withColumn("row_num", row_number().over(window_devolucoes)) \
                .filter(col("row_num") == 1).drop("row_num") \
                .drop("USUAINCLUSAO", "DATAALTERACAO", "USUAALTERACAO", "CODTITULOBAIXA")
            
            delta_table_dev.alias("t").merge(
                df_dedup.alias("s"),
                "t.CODTITULO = s.CODTITULO"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            print("Merge Devoluções concluído.")
        else:
            print("Sem novas devoluções.")
            
    except Exception as e:
        print(f"Erro no incremental (provavelmente falta de coluna de data): {e}. Fallback para Full Load.")
        # Fallback Full
        df_bronze_devolucoes = spark.read.table(f"{source_lakehouse}.{source_table_devolucoes}")
        window_devolucoes = Window.partitionBy("CODTITULO").orderBy(col("DATAALTERACAO").desc())
        df_transformed_devolucoes = df_bronze_devolucoes \
            .withColumn("row_num", row_number().over(window_devolucoes)) \
            .filter(col("row_num") == 1).drop("row_num") \
            .drop("USUAINCLUSAO", "DATAALTERACAO", "USUAALTERACAO", "CODTITULOBAIXA")
        df_transformed_devolucoes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_devolucoes)

else:
    print("Modo Full Load: Devoluções")
    df_bronze_devolucoes = spark.read.table(f"{source_lakehouse}.{source_table_devolucoes}")
    window_devolucoes = Window.partitionBy("CODTITULO").orderBy(col("DATAALTERACAO").desc())
    df_transformed_devolucoes = df_bronze_devolucoes \
        .withColumn("row_num", row_number().over(window_devolucoes)) \
        .filter(col("row_num") == 1).drop("row_num") \
        .drop("USUAINCLUSAO", "DATAALTERACAO", "USUAALTERACAO", "CODTITULOBAIXA")
    df_transformed_devolucoes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_devolucoes)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 3: TAC M (Full Load)
# **Estratégia:** Tabela pequena e filtrada por ano (2024+).

# CELL ********************

print("Processando TAC M...")
df_tac = spark.read.table("LH_Bronze.tab_operacoes_tarifas_extras")
df_tac_renamed = df_tac \
    .filter(year(col("DATAINCLUSAO")) >= 2024) \
    .select(
        col("CODTARIFAEXTRA").alias("cod_tarifa_extra"), col("CODOPERACAO").alias("cod_operacao"), col("DESCRICAO").alias("descricao"), col("TOTAL").alias("total"), col("DATAINCLUSAO").alias("data_inclusao"), col("USUAINCLUSAO").alias("usua_inclusao")
    )

df_tac_cleaned = df_tac_renamed \
    .withColumn("descricao", upper(col("descricao"))) \
    .withColumn("descricao", regexp_replace(col("descricao"), "^\\s+|\\s+$", "")) \
    .withColumn("descricao",
        when(col("descricao") == "TAC  M", lit("TAC M")).when(col("descricao") == "TAC MOP", lit("TAC M")).when(col("descricao") == "TAC M.", lit("TAC M")).when(col("descricao") == "TACM", lit("TAC M")).when(col("descricao") == "TACA M", lit("TAC M")).when(col("descricao") == "TAC M 300,00", lit("TAC M")).when(col("descricao") == "TAC", lit("TAC M")).otherwise(col("descricao"))
    ) \
    .filter(col("descricao") == "TAC M") \
    .orderBy(col("data_inclusao").desc())

df_tac_cleaned.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Silver.staging_tac_m")

print("Limpeza Silver - Operações finalizada.")
mssparkutils.notebook.exit("Success")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
