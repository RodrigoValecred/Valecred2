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
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Relatório de Rentabilidade e Risco de Clientes (Safra 2025)
# **Objetivo:** Analisar a taxa média ponderada, receitas (tarifas, juros de mora) e risco dos clientes que operaram em 2025.
# **Contexto:** A taxa média de 2025 apresentou queda. Este relatório identifica os clientes com menores taxas e cruza com perfil de risco e rentabilidade total.

# CELL ********************

# Configuração e Imports
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

from pyspark.sql.functions import (
    col, sum, avg, count, max, min, lit, when, round, desc, asc, broadcast, coalesce
)
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Iniciando Análise de Rentabilidade 2025...")

# 1. Carregamento de Dados (Gold)
print("Carregando tabelas Fato e Dimensão...")

# Operações (Base da Análise - Safra 2025)
df_ops = spark.read.table("LH_Gold.fato_operacoes") \
    .filter(col("data_deferimento") >= "2025-01-01") \
    .filter(col("status_aceite") == "A") \
    .filter(col("status_analise") == "D")

# Títulos (Para cálculo da Taxa Ponderada: Valor * Prazo)
# Agregado por Operação
df_titulos = spark.read.table("LH_Gold.fato_titulos")
df_titulos_agg = df_titulos.groupBy("cod_operacao").agg(
    sum("valor_vezes_prazo").alias("total_valor_prazo_op"),
    sum("valor").alias("valor_face_titulos_op")
)

# Baixas (Para cálculo de Receita de Juros de Mora Pagos)
# Agregado por Operação
df_baixas = spark.read.table("LH_Gold.fato_baixas")
df_baixas_agg = df_baixas.groupBy("cod_operacao").agg(
    sum("juros").alias("total_juros_mora_pago_op")
)

# Dimensão Clientes (Para Nome e Risco Atual)
df_clientes = spark.read.table("LH_Gold.dim_clientes") \
    .select("cod_cliente", "nome", "risco", "risco_comissaria", "classificacao_risco", "grupo_economico")

# Análise Score Clientes (Para Qualidade/Classificação Detalhada se existir)
try:
    df_score = spark.read.table("LH_Gold.analise_score_clientes") \
        .select("cod_cliente", "qualidade_cliente")
except:
    print("Aviso: Tabela analise_score_clientes não encontrada ou esquema diferente. Usando placeholder.")
    df_score = None

# Dimensão Produtos (Para Nome Amigável e Categorização)
df_produtos = spark.read.table("LH_Gold.dim_produtos") \
    .select("chave_produto", "produto_informacao_de_mercado")

print("Dados carregados.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. Enriquecimento e Cálculos
print("Realizando joins e cálculos de métricas...")

# Join Operações com Agregados
df_base = df_ops.join(df_titulos_agg, "cod_operacao", "left") \
    .join(df_baixas_agg, "cod_operacao", "left") \
    .join(broadcast(df_produtos), "chave_produto", "left")

# Métricas por Cliente e Produto
# Agrupamento: Cliente + Produto (Comissária vs Desconto é relevante)
df_analise = df_base.groupBy("cod_cliente", "produto_informacao_de_mercado").agg(
    # Volumetria
    count("cod_operacao").alias("qtd_operacoes"),
    sum("valor_de_face").alias("volume_operado"),

    # Receita de Taxa (Deságio) e Base para Taxa Ponderada
    sum("desagio").alias("receita_desagio"),
    sum("total_valor_prazo_op").alias("soma_valor_prazo"),

    # Receitas Adicionais
    sum("total_de_tarifas").alias("receita_tarifas"),
    sum("total_juros_mora_pago_op").alias("receita_juros_mora")
)

# 3. Join com Dados do Cliente (Risco e Nome)
df_analise_final = df_analise.join(df_clientes, "cod_cliente", "left")

if df_score:
    df_analise_final = df_analise_final.join(df_score, "cod_cliente", "left")
else:
    df_analise_final = df_analise_final.withColumn("qualidade_cliente", lit(None))

# 4. Cálculo de Indicadores Finais
df_report = df_analise_final \
    .withColumn("taxa_media_ponderada_mensal",
                when(col("soma_valor_prazo") > 0,
                     (col("receita_desagio") / col("soma_valor_prazo")) * 30 * 100
                ).otherwise(0)) \
    .withColumn("receita_total",
                coalesce(col("receita_desagio"), lit(0)) +
                coalesce(col("receita_tarifas"), lit(0)) +
                coalesce(col("receita_juros_mora"), lit(0))) \
    .withColumn("rentabilidade_percentual",
                when(col("volume_operado") > 0,
                     (col("receita_total") / col("volume_operado")) * 100
                ).otherwise(0)) \
    .select(
        col("cod_cliente"),
        col("nome").alias("nome_cliente"),
        col("grupo_economico"),
        col("produto_informacao_de_mercado").alias("produto"),
        col("qualidade_cliente"),
        col("risco").alias("risco_total_atual"),
        col("risco_comissaria").alias("risco_comissaria_atual"),
        col("volume_operado"),
        col("qtd_operacoes"),
        round(col("taxa_media_ponderada_mensal"), 4).alias("taxa_media_pond_2025"),
        round(col("receita_desagio"), 2).alias("receita_desagio"),
        round(col("receita_tarifas"), 2).alias("receita_tarifas"),
        round(col("receita_juros_mora"), 2).alias("receita_juros_mora"),
        round(col("receita_total"), 2).alias("receita_total"),
        round(col("rentabilidade_percentual"), 4).alias("rentabilidade_perc")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5. Output e Análise
print("Gerando output...")

# Ordenar por Taxa Média (Menores Taxas Primeiro) para identificar os clientes solicitados
df_menores_taxas = df_report.filter(col("volume_operado") > 10000) \
    .orderBy(col("taxa_media_pond_2025").asc())

print("Top 20 Clientes com Menores Taxas em 2025 (Volume > 10k):")
df_menores_taxas.show(20, truncate=False)

# Ordenar por Rentabilidade Total (Maiores Receitas)
print("Top 20 Clientes Mais Rentáveis (Receita Total):")
df_report.orderBy(col("receita_total").desc()).show(20, truncate=False)

# Salvar Tabela Gold
output_table = "LH_Gold.relatorio_rentabilidade_clientes_2025"
df_report.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)
print(f"Relatório salvo em: {output_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
