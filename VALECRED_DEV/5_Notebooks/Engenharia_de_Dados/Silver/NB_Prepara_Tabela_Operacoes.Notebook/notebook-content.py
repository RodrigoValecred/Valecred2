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
    lead, date_add, lag, max, coalesce, date_sub, array_contains, create_map, split,
    to_date
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

def process_operacoes():
    source_table_operacoes = "tab_operacoes"
    target_table_operacoes = "staging_operacoes_limpa"
    output_path_operacoes = f"{target_lakehouse}.{target_table_operacoes}"

    print(f"Iniciando processamento de {target_table_operacoes}...")

    key_columns_operacoes = ["CODOPERACAO"]

    # Schema de Seleção
    def select_operacoes(df):
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
            col("NOTASERVICO").alias("nota_servico"),
            col("TTO").alias("tto"),
            col("STTO").alias("stto"),
            col("chave_produto"),
            col("TOTRETENCAO").alias("valor_retido"),
            col("TOTDES").alias("valor_desembolsado"),
            col("TOTFAC").alias("valor_de_face"),
            col("TOTDCP").alias("desagio"),
            col("TOTTAR").alias("total_de_tarifas"),
            col("TOTRECOMPRA").alias("valor_recomprado"),
            col("FATOR").alias("taxa")
        )

    is_incremental_ops = False
    if DeltaTable.isDeltaTable(spark, output_path_operacoes):
        if "cod_operacao" in spark.read.format("delta").load(output_path_operacoes).columns:
            is_incremental_ops = True
        else:
            print("Schema mismatch for Operacoes. Forcing Full Load.")

    if is_incremental_ops:
        print("Modo Incremental: Operações")
        delta_table_ops = DeltaTable.forPath(spark, output_path_operacoes)
        
        # 1. Watermark
        watermark_row = spark.read.format("delta").load(output_path_operacoes) \
            .select(greatest(max("data_inclusao"), max("data_alteracao")).alias("max_date")) \
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

            df_com_chave = df_dedup.withColumn("chave_produto", concat(col("TTO"), coalesce(col("STTO"),lit(""))))
            df_final_batch = select_operacoes(df_com_chave)

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

    else:
        print("Modo Full Load: Operações")
        df_bronze_ops = spark.read.table(f"{source_lakehouse}.{source_table_operacoes}")
        
        # Tratamento TTO específico
        df_corrigido = df_bronze_ops.withColumn("TTO_corrigido", when(col("CODOPERACAO") == 3042074, lit("CS")).otherwise(col("TTO"))).drop("TTO").withColumnRenamed("TTO_corrigido", "TTO")
        
        # Deduplicação
        windowSpec = Window.partitionBy([col(c) for c in key_columns_operacoes]).orderBy(col("DATAALTERACAO").desc())
        df_ranked = df_corrigido.withColumn("row_num", row_number().over(windowSpec))
        df_dedup = df_ranked.filter(col("row_num") == 1).drop("row_num")
        
        df_com_chave = df_dedup.withColumn("chave_produto", concat(col("TTO"), coalesce(col("STTO"), lit(""))))
        
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

def process_devolucoes():
    source_table_devolucoes = "tab_operacoes_devolucoes"
    target_table_devolucoes = "staging_operacoes_devolucoes_limpa"
    output_path_devolucoes = f"{target_lakehouse}.{target_table_devolucoes}"

    print(f"Iniciando processamento de {target_table_devolucoes}...")

    is_incremental_dev = False
    if DeltaTable.isDeltaTable(spark, output_path_devolucoes):
        if "cod_titulo" in spark.read.format("delta").load(output_path_devolucoes).columns:
            is_incremental_dev = True
        else:
            print("Schema mismatch for Devolucoes. Forcing Full Load.")

    if is_incremental_dev:
        print("Modo Incremental: Devoluções")
        delta_table_dev = DeltaTable.forPath(spark, output_path_devolucoes)
        
        try:
            watermark_row = spark.read.format("delta").load(output_path_devolucoes) \
                .agg(max("data_inclusao").alias("max_date")).collect()
            last_watermark = watermark_row[0][0] if watermark_row and watermark_row[0][0] else "1900-01-01"

            print(f"Watermark Devoluções: {last_watermark}")
            
            df_bronze_dev = spark.read.table(f"{source_lakehouse}.{source_table_devolucoes}") \
                .filter((col("DATAINCLUSAO") >= last_watermark) | (col("DATAALTERACAO") >= last_watermark))

            if df_bronze_dev.count() > 0:
                window_devolucoes = Window.partitionBy("CODTITULO").orderBy(col("DATAALTERACAO").desc())
                df_dedup = df_bronze_dev.withColumn("row_num", row_number().over(window_devolucoes)) \
                    .filter(col("row_num") == 1).drop("row_num") \
                    .drop("USUAINCLUSAO", "DATAALTERACAO", "USUAALTERACAO", "CODTITULOBAIXA") \
                    .withColumnRenamed("CODTITULO", "cod_titulo") \
                    .withColumnRenamed("DATAINCLUSAO", "data_inclusao") \
                    .withColumnRenamed("CODOPERACAO", "cod_operacao")

                # Garantir snake_case em todas as colunas
                df_dedup = df_dedup.select([col(c).alias(c.lower()) for c in df_dedup.columns])

                delta_table_dev.alias("t").merge(
                    df_dedup.alias("s"),
                    "t.cod_titulo = s.cod_titulo"
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
                .drop("USUAINCLUSAO", "DATAALTERACAO", "USUAALTERACAO", "CODTITULOBAIXA") \
                .withColumnRenamed("CODTITULO", "cod_titulo") \
                .withColumnRenamed("DATAINCLUSAO", "data_inclusao") \
                .withColumnRenamed("CODOPERACAO", "cod_operacao")

            # Garantir snake_case em todas as colunas
            df_transformed_devolucoes = df_transformed_devolucoes.select([col(c).alias(c.lower()) for c in df_transformed_devolucoes.columns])

            df_transformed_devolucoes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_devolucoes)

    else:
        print("Modo Full Load: Devoluções")
        df_bronze_devolucoes = spark.read.table(f"{source_lakehouse}.{source_table_devolucoes}")
        window_devolucoes = Window.partitionBy("CODTITULO").orderBy(col("DATAALTERACAO").desc())
        df_transformed_devolucoes = df_bronze_devolucoes \
            .withColumn("row_num", row_number().over(window_devolucoes)) \
            .filter(col("row_num") == 1).drop("row_num") \
            .drop("USUAINCLUSAO", "DATAALTERACAO", "USUAALTERACAO", "CODTITULOBAIXA") \
            .withColumnRenamed("CODTITULO", "cod_titulo") \
            .withColumnRenamed("DATAINCLUSAO", "data_inclusao") \
            .withColumnRenamed("CODOPERACAO", "cod_operacao")

        # Garantir snake_case em todas as colunas
        df_transformed_devolucoes = df_transformed_devolucoes.select([col(c).alias(c.lower()) for c in df_transformed_devolucoes.columns])

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

def process_tac_m():
    print("Processando TAC M...")
    df_tac = spark.read.table(f"{source_lakehouse}.tab_operacoes_tarifas_extras")
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

def process_estudo_op():
    print("Processando Estudo Op...")
    df_estudo = spark.read.table(f"{source_lakehouse}.tab_estudo_op")
    df_estudo.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.staging_estudo_operacoes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 5: Pareceres de Operações
# **Objetivo:** Limpeza e extração de flags de alçada e HTML.

# CELL ********************

def process_pareceres_operacoes():
    print("Processando Pareceres Operações...")

    df_pareceres = spark.read.table(f"{source_lakehouse}.cad_geral_pareceres").alias("cgp")
    df_usuarios = spark.read.table(f"{source_lakehouse}.cad_usuarios").alias("cu")
    # Usa target_lakehouse explicitamente
    df_operacoes_ref = spark.read.table(f"{target_lakehouse}.staging_operacoes_limpa").alias("to2")

    # Filtrar e Join
    df_pareceres_filtered = df_pareceres.filter(
        col("OBS").isNotNull() & col("CODOPERACAO").isNotNull() &
        (col("CODTIPOPARECER") == 10) & (year(col("DATAINCLUSAO")) >= 2024) &
        (~col("OBS").like("%<img alt=%"))
    )

    df_joined = df_pareceres_filtered \
        .join(df_usuarios, col("cgp.USUAINCLUSAO") == col("cu.CODUSUARIO")) \
        .join(df_operacoes_ref, col("cgp.CODOPERACAO") == col("to2.cod_operacao")) \
        .select(
            col("cgp.CODOPERACAO").alias("cod_operacao"),
            col("cgp.DATAINCLUSAO").alias("data_inclusao"),
            col("cu.APELIDO").alias("apelido_usuario"),
            col("cgp.OBS").alias("obs")
        )

    # HTML Cleaning Logic (Replicating Power Query ReplaceValues)
    replacements = {
        "&Ccedil;": "Ç", "&Atilde;": "Ã", "&Aacute;": "Á", "&Eacute;": "É", "&Iacute;": "Í",
        "&Otilde;": "Õ", "&Uacute;": "Ú", "&Oacute;": "Ó", "<div id=\"cke_pastebin\">\r\n\t": "",
        "&nbsp;</div>\r": " ", "&nbsp;": " ", "</div>\r\n": "", "\n \n": "\n",
        "<span style=\"background-color:#ffff00;\">": "", "</span>": " ", "<div>\r": "",
        "<p>\r\n": "", "</p>\r": "", "&#39;": "'", "\t": "", "<span style=\"background-color:#fff;\">": "",
        "<span style=\"color: rgb(0, 0, 0); font-family: Arial, Helvetica, sans-serif; font-weight: 700;\">": "",
        "<span style=\"background-color: rgb(255, 255, 0);\">": "", "<u>": "", "&quot;": "\"",
        "<span style=\"color: rgb(0, 0, 0); font-family: Arial, Helvetica, sans-serif; font-weight: 700; background-color: rgb(255, 255, 0);\">": ""
    }

    df_cleaned = df_joined.withColumn("Parecer", col("obs"))
    for old, new in replacements.items():
        df_cleaned = df_cleaned.withColumn("Parecer", regexp_replace(col("Parecer"), old, new))

    # Flags Logic
    df_final_pareceres = df_cleaned.withColumn("ESCROW", when(col("obs").like("%#ESCROW%"), True).otherwise(False)) \
        .withColumn("ALCADA_SPENCER", when(col("obs").like("%SPENCER%"), "sim").otherwise("não")) \
        .withColumn("ALCADA_CAIO", when(col("obs").like("%CAIO%"), "sim").otherwise("não")) \
        .withColumn("ALCADA_DAIANE", when(col("obs").like("%DAIANE%"), "sim").otherwise("não")) \
        .drop("obs")

    df_final_pareceres.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.staging_pareceres_operacoes")


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
    # Removed try-except to ensure fail-fast if dependencies are missing
    df_boletos = spark.read.table(f"{target_lakehouse}.staging_boletos_titulos")
    df_ops_pr = spark.read.table(f"{target_lakehouse}.staging_operacoes_limpa").filter(col("TTO") == "PR")

    df_prorrogacoes = df_ops_pr.join(df_boletos, "cod_operacao", "left") \
        .select(
            df_ops_pr["cod_operacao"], "cod_titulo", "n_doc", "cpf_cnpj_sacado", "cpf_cnpj_cedente",
            "valor", "amortizacoes", "liquidacao"
        )
    df_prorrogacoes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.staging_prorrogacoes")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 8: Recompras
# **Objetivo:** Join de Operações (RC/RE) com Boletos.

# CELL ********************

def process_tab_operacoes_prorrogacao():
    print("Processando Tab Operações Prorrogação...")

    # Fonte Principal
    df_prorrogacao = spark.read.table(f"{source_lakehouse}.tab_operacoes_prorrogacao")

    # Fontes Auxiliares (Silver para garantir unicidade e dados mais recentes)
    # Titulos: CODTITULO -> VALOR
    df_titulos = spark.read.table(f"{target_lakehouse}.staging_titulos_limpa") \
        .select(col("cod_titulo"), col("valor").alias("VALOR_TITULO"))

    # Operacoes: CODOPERACAO -> STATUSANALISE, STATUSACEITE
    df_operacoes = spark.read.table(f"{target_lakehouse}.staging_operacoes_limpa") \
        .select(col("cod_operacao"), col("status_analise").alias("STATUSANALISE"), col("status_aceite").alias("STATUSACEITE"))

    # Joins
    df_joined = df_prorrogacao \
        .join(df_titulos, col("CODTITULO") == col("cod_titulo"), "left") \
        .join(df_operacoes, col("CODOPERACAO") == col("cod_operacao"), "left")

    # Remover colunas desnecessárias (e as chaves duplicadas do join)
    cols_to_drop = [
        "TARIFA", "USUAINCLUSAO", "DATAALTERACAO", "USUAALTERACAO",
        "VALORDEVIDO", "VALORPROR", "VALORBOLETO",
        "cod_titulo", "cod_operacao"
    ]
    df_cleaned = df_joined.drop(*cols_to_drop)

    # Renomear VALOR trazido de titulos (se necessário) ou garantir que VALOR é o de títulos
    if "VALOR" in df_prorrogacao.columns:
        df_cleaned = df_cleaned.drop("VALOR")

    df_cleaned = df_cleaned.withColumnRenamed("VALOR_TITULO", "VALOR")

    # Transformações
    df_transformed = df_cleaned \
        .withColumn("BASE", lit(40)) \
        .withColumn("chave_base_titulo", concat(lit("40-"), col("CODTITULO").cast("string"))) \
        .withColumn("Data", to_date(col("DATAINCLUSAO")))

    # Padronização para snake_case
    df_final = df_transformed.select([col(c).alias(c.lower()) for c in df_transformed.columns])

    # Salvar
    target_table = "staging_operacoes_prorrogacao"
    df_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.{target_table}")
    print(f"Tabela {target_table} criada com sucesso.")


def process_recompras():
    print("Processando Recompras...")
    # Removed try-except to ensure fail-fast if dependencies are missing
    df_boletos = spark.read.table(f"{target_lakehouse}.staging_boletos_titulos")
    df_ops_rc = spark.read.table(f"{target_lakehouse}.staging_operacoes_limpa") \
        .filter(col("TTO").isin(["RC", "RE"])) \
        .filter((col("status_analise") == "D") & (col("status_aceite") == "A"))

    df_recompras = df_ops_rc.join(df_boletos, "cod_operacao", "left") \
        .select(
            df_ops_rc["cod_operacao"], "cod_titulo", "n_doc", "cpf_cnpj_sacado", "cpf_cnpj_cedente",
            "valor", "amortizacoes", "liquidacao",
            concat(lit("40-"), col("cod_operacao")).alias("chave_base_operacao_recompra")
        )
    df_recompras.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.staging_operacoes_recompras")

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

print("Limpeza Silver - Operações finalizada.")
mssparkutils.notebook.exit("Success")
