# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8f85c372-56ad-4f3f-acf9-3be2e9b99513",
# META       "default_lakehouse_name": "LH_Silver",
# META       "default_lakehouse_workspace_id": "41ae19db-f71d-471f-9ac7-ccbc2c75ce11",
# META       "known_lakehouses": [
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

# # Notebook de Análise: Taxa Média Ponderada (2025)
# **Objetivo:** Calcular a taxa média ponderada geral e por gerente para o ano de 2025.
# **Origem:** LH_Gold.fato_operacoes (Dados principais) e LH_Silver.staging_operacoes_limpa (Taxa original).

# CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

from pyspark.sql.functions import col, sum, round, desc
from delta.tables import *

print("Iniciando análise de Taxa Média Ponderada (2025)...")

# 1. Carregar Tabelas
# Carregamos a fato_operacoes da Gold (Curada) e a staging_operacoes da Silver (Fonte da Taxa)
print("Carregando tabelas...")
try:
    df_fato_ops = spark.read.table("LH_Gold.fato_operacoes")
    df_stg_ops = spark.read.table("LH_Silver.staging_operacoes_limpa")
except Exception as e:
    print(f"Erro ao carregar tabelas: {e}")
    raise e

# 2. Join para obter a Taxa
# A tabela fato_operacoes pode não ter a coluna 'taxa' explícita (tem taxa_cadastro/fator),
# então buscamos a 'taxa' original da Silver para garantir.
df_analysis_base = df_fato_ops.join(
    df_stg_ops.select("cod_operacao", "taxa"),
    "cod_operacao",
    "inner"
)

# 3. Filtragem (Ano 2025, Deferidas, Aceitas)
# data_deferimento é a melhor data para 'safra' da operação.
# Filtramos apenas Status Analise = 'D' (Deferido) e Status Aceite = 'A' (Aceito).
# Filtramos Valor de Face > 0 para evitar divisão por zero.
df_filtered = df_analysis_base.filter(
    (col("data_deferimento") >= "2025-01-01") &
    (col("data_deferimento") <= "2025-12-31") &
    (col("status_analise") == "D") &
    (col("status_aceite") == "A") &
    (col("valor_de_face") > 0)
)

count_ops = df_filtered.count()
print(f"Operações qualificadas para o estudo (2025): {count_ops}")

if count_ops > 0:
    # 4. Cálculo da Taxa Média Ponderada GERAL
    # Formula: Sum(Taxa * ValorFace) / Sum(ValorFace)
    # Nota: Taxa geralmente é percentual mensal (ex: 3.5). O resultado será mantido na mesma escala.

    df_general = df_filtered.agg(
        sum(col("taxa") * col("valor_de_face")).alias("weighted_sum"),
        sum("valor_de_face").alias("total_face_value")
    ).withColumn(
        "taxa_media_ponderada_geral",
        col("weighted_sum") / col("total_face_value")
    )

    result_general = df_general.collect()[0]
    taxa_geral = result_general["taxa_media_ponderada_geral"]
    total_volume = result_general["total_face_value"]

    print("-" * 50)
    print(f"RESULTADO GERAL (2025):")
    print(f"Volume Total Analisado: R$ {total_volume:,.2f}")
    print(f"Taxa Média Ponderada Geral: {taxa_geral:.4f}% (ou fator)")
    print("-" * 50)

    # 5. Cálculo por Gerente
    df_manager = df_filtered.groupBy("gestor_da_operacao").agg(
        sum(col("taxa") * col("valor_de_face")).alias("weighted_sum"),
        sum("valor_de_face").alias("total_face_value"),
        sum("valor_de_face").alias("volume_operado") # Alias for clarity
    ).withColumn(
        "taxa_media_ponderada",
        round(col("weighted_sum") / col("total_face_value"), 4)
    ).select(
        "gestor_da_operacao",
        "volume_operado",
        "taxa_media_ponderada"
    ).orderBy(desc("volume_operado"))

    print("\nRESULTADO POR GERENTE (Top 20 por Volume):")
    df_manager.show(20, truncate=False)

    # 6. Salvar Resultado
    output_table = "LH_Gold.analise_taxa_media_2025"
    print(f"Salvando análise detalhada em: {output_table}")
    df_manager.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)
    print("Concluído.")

else:
    print("Nenhuma operação encontrada para os critérios especificados em 2025.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
