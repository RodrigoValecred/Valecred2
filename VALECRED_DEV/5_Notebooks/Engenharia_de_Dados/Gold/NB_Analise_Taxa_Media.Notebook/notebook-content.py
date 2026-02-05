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
# **Objetivo:** Calcular a taxa média ponderada geral, por gerente e distribuição por cliente para o ano de 2025.
# **Fórmula (User Defined):** `(Desagio / (Prazo * Valor)) * 30` para mensalizar.
# **Origem:**
# * `LH_Gold.fato_operacoes` (Deságio, Datas, Gerentes)
# * `LH_Gold.fato_titulos` (Valor * Prazo por título, agregado por operação)
# * `LH_Silver.sup_faixas_taxa` (Tabela de suporte para faixas de taxa)

# CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

from pyspark.sql.functions import col, sum, round, desc, coalesce, lit, floor, count, concat, broadcast
from delta.tables import *

print("Iniciando análise de Taxa Média Ponderada (2025) - Método Prazo Médio...")

# 1. Carregar Tabelas
print("Carregando tabelas...")
try:
    df_fato_ops = spark.read.table("LH_Gold.fato_operacoes")
    df_fato_titulos = spark.read.table("LH_Gold.fato_titulos")

    # Carregando tabela de faixas (Suporte)
    # Assumindo colunas: min_taxa, max_taxa, faixa (ou similar)
    try:
        df_faixas = spark.read.table("LH_Silver.sup_faixas_taxa")
        print("Schema da tabela sup_faixas_taxa:")
        df_faixas.printSchema()
    except Exception as e:
        print(f"AVISO: Tabela LH_Silver.sup_faixas_taxa não encontrada: {e}")
        # Fallback se não existir (apenas para teste/compilação)
        from pyspark.sql.types import StructType, StructField, DoubleType, StringType
        schema = StructType([
            StructField("min_taxa", DoubleType(), True),
            StructField("max_taxa", DoubleType(), True),
            StructField("faixa", StringType(), True)
        ])
        df_faixas = spark.createDataFrame([], schema)

except Exception as e:
    print(f"Erro ao carregar tabelas principais: {e}")
    raise e

# 2. Agregar Fato Títulos (Calculo do Denominador: Prazo * Valor)
# Precisamos do somatório de (Valor * Prazo) por operação.
# A coluna 'valor_vezes_prazo' já existe na fato_titulos (Gold).
print("Agregando dados de títulos...")
df_titulos_agg = df_fato_titulos.groupBy("cod_operacao").agg(
    sum("valor_vezes_prazo").alias("total_valor_vezes_prazo_op")
)

# 3. Join Fato Operações com Agregado de Títulos
print("Realizando Join Operações + Títulos...")
df_analysis_base = df_fato_ops.join(
    df_titulos_agg,
    "cod_operacao",
    "inner" # Inner join pois precisamos dos títulos para calcular o prazo
)

# 4. Filtragem (Ano 2025, Deferidas, Aceitas)
# data_deferimento é a melhor data para 'safra' da operação.
df_filtered = df_analysis_base.filter(
    (col("data_deferimento") >= "2025-01-01") &
    (col("data_deferimento") <= "2025-12-31") &
    (col("status_analise") == "D") &
    (col("status_aceite") == "A") &
    (col("valor_de_face") > 0) &
    (col("total_valor_vezes_prazo_op") > 0)
)

count_ops = df_filtered.count()
print(f"Operações qualificadas para o estudo (2025): {count_ops}")

if count_ops > 0:
    # 4. Cálculo da Taxa Média Ponderada GERAL
    df_general = df_filtered.agg(
        sum("desagio").alias("total_desagio"),
        sum("total_valor_vezes_prazo_op").alias("total_vp_geral"),
        sum("valor_de_face").alias("total_face_value")
    ).withColumn(
        "taxa_media_mensal_ponderada",
        (col("total_desagio") / col("total_vp_geral")) * 30 * 100
    )

    result_general = df_general.collect()[0]
    taxa_geral = result_general["taxa_media_mensal_ponderada"]
    total_volume = result_general["total_face_value"]

    print("-" * 50)
    print(f"RESULTADO GERAL (2025):")
    print(f"Volume Total Analisado: R$ {total_volume:,.2f}")
    print(f"Taxa Média Mensal Ponderada Geral: {taxa_geral:.4f}% a.m.")
    print("-" * 50)

    # 5. Cálculo por Gerente
    df_manager = df_filtered.groupBy("gestor_da_operacao").agg(
        sum("desagio").alias("total_desagio"),
        sum("total_valor_vezes_prazo_op").alias("total_vp_manager"),
        sum("valor_de_face").alias("volume_operado")
    ).withColumn(
        "taxa_media_mensal_ponderada",
        round((col("total_desagio") / col("total_vp_manager")) * 30 * 100, 4)
    ).select(
        "gestor_da_operacao",
        "volume_operado",
        "taxa_media_mensal_ponderada"
    ).orderBy(desc("volume_operado"))

    print("\nRESULTADO POR GERENTE (Top 20 por Volume):")
    df_manager.show(20, truncate=False)

    # 6. Salvar Resultado
    output_table = "LH_Gold.analise_taxa_media_2025"
    df_manager.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)

    # 7. Cálculo por Cliente e Distribuição por Faixa (Tabela Sup)
    print("\nCalculando distribuição de taxas médias por cliente...")

    # 7.1 Taxa Média por Cliente
    df_client_rate = df_filtered.groupBy("cod_cliente").agg(
        sum("desagio").alias("total_desagio"),
        sum("total_valor_vezes_prazo_op").alias("total_vp_client"),
        sum("valor_de_face").alias("volume_operado")
    ).withColumn(
        "taxa_media_cliente",
        (col("total_desagio") / col("total_vp_client")) * 30 * 100
    )

    # 7.2 Classificação usando LH_Silver.sup_faixas_taxa
    # Assumimos que as colunas da tabela de suporte sejam identificáveis.
    # Tentativa de normalização dinâmica ou uso de nomes padrão
    cols = df_faixas.columns
    # Heurística para identificar colunas min/max
    col_min = next((c for c in cols if "min" in c.lower() or "inicio" in c.lower()), "min_taxa")
    col_max = next((c for c in cols if "max" in c.lower() or "fim" in c.lower()), "max_taxa")
    col_nome = next((c for c in cols if "faixa" in c.lower() or "nome" in c.lower() or "desc" in c.lower()), "faixa")

    print(f"Usando colunas para join de faixas: Min='{col_min}', Max='{col_max}', Nome='{col_nome}'")

    # Join Conditional (Non-Equi Join)
    # Broadcast df_faixas pois deve ser pequena
    df_client_bins = df_client_rate.crossJoin(broadcast(df_faixas)) \
        .filter(
            (col("taxa_media_cliente") >= col(col_min)) &
            (col("taxa_media_cliente") < col(col_max))
        ) \
        .select(
            col("cod_cliente"),
            col("volume_operado"),
            col(col_nome).alias("faixa_taxa"),
            col(col_min).alias("ordem_faixa") # Para ordenação
        )

    # 7.3 Contagem por Faixa
    df_distribution = df_client_bins.groupBy("ordem_faixa", "faixa_taxa").agg(
        count("cod_cliente").alias("qtd_clientes"),
        sum("volume_operado").alias("volume_total_faixa")
    ).orderBy("ordem_faixa")

    print("\nDISTRIBUIÇÃO DE CLIENTES POR FAIXA DE TAXA (sup_faixas_taxa):")
    df_distribution.select("faixa_taxa", "qtd_clientes", round("volume_total_faixa", 2).alias("volume_faixa")).show(50, truncate=False)

    # 7.4 Salvar Distribuição
    output_dist_table = "LH_Gold.analise_distribuicao_taxa_clientes_2025"
    print(f"Salvando distribuição em: {output_dist_table}")
    df_distribution.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_dist_table)

    print("Concluído.")

else:
    print("Nenhuma operação encontrada para os critérios especificados em 2025.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
