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
# 
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
# Dedup by cod_operacao to be safe
# Incluindo nbordero e cod_operacao na selecao
df_ops = spark.read.table("LH_Gold.fato_operacoes") \
    .filter(col("data_deferimento") >= "2025-01-01") \
    .filter(col("data_deferimento") <= "2025-12-31") \
    .filter(col("status_aceite") == "A") \
    .filter(col("status_analise") == "D") \
    .dropDuplicates(["cod_operacao"])

# Títulos (Para cálculo da Taxa Ponderada: Valor * Prazo)
# Agregado por Operação
df_titulos = spark.read.table("LH_Gold.fato_titulos") \
    .filter(col("aceito") == "S") \
    .filter(col("t_doc") != "BL")

df_titulos_agg = df_titulos.groupBy("cod_operacao").agg(
    sum("valor_vezes_prazo").alias("total_valor_prazo_op"),
    sum("valor").alias("valor_face_titulos_op"),
    sum("custo_financeiro").alias("custo_financeiro_op"),
    sum("spread").alias("spread_op")
)

# Baixas (Para cálculo de Receita de Juros de Mora Pagos)
# Agregado por Operação
df_baixas = spark.read.table("LH_Gold.fato_baixas")
df_baixas_agg = df_baixas.groupBy("cod_operacao").agg(
    sum("juros").alias("total_juros_mora_pago_op")
)

# Dimensão Clientes (Para Nome e Risco Atual)
df_clientes = spark.read.table("LH_Gold.dim_clientes") \
    .select("cod_cliente", "nome", "risco", "risco_comissaria", "status_risco", "grupo_economico") \
    .dropDuplicates(["cod_cliente"])

# Análise Score Clientes (Para Qualidade/Classificação Detalhada se existir)
try:
    df_score = spark.read.table("LH_Gold.analise_score_clientes") \
        .select("cod_cliente", "qualidade_cliente") \
        .dropDuplicates(["cod_cliente"])
except:
    print("Aviso: Tabela analise_score_clientes não encontrada ou esquema diferente. Usando placeholder.")
    df_score = None

# Dimensão Produtos (Para Nome Amigável e Categorização)
# Force Dedup on chave_produto to prevent join explosion
df_produtos = spark.read.table("LH_Gold.dim_produtos") \
    .select("chave_produto", "produto_informacao_de_mercado") \
    .dropDuplicates(["chave_produto"])

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
df_base = df_ops.join(df_titulos_agg, "cod_operacao", "inner") \
    .join(df_baixas_agg, "cod_operacao", "left") \
    .join(broadcast(df_produtos), "chave_produto", "left")

# Join com Dados do Cliente (Risco e Nome)
df_base_cliente = df_base.join(df_clientes, "cod_cliente", "left")

if df_score:
    df_base_cliente = df_base_cliente.join(df_score, "cod_cliente", "left")
else:
    df_base_cliente = df_base_cliente.withColumn("qualidade_cliente", lit(None))

# 4. Cálculo de Indicadores Finais (Granularidade: Operação)

# Janela para Cálculos Médios do Cliente (independente do produto/operação)
w_cliente = Window.partitionBy("cod_cliente")

# Janela para Cálculos Médios do Produto (opcional, mantendo compatibilidade)
w_produto = Window.partitionBy("cod_cliente", "produto_informacao_de_mercado")

df_report = df_base_cliente \
    .withColumn("produto_final", coalesce(col("produto_informacao_de_mercado"), lit("PRODUTO NÃO IDENTIFICADO"))) \
    .withColumn("receita_total_op", 
                coalesce(col("desagio"), lit(0)) + 
                coalesce(col("total_de_tarifas"), lit(0)) + 
                coalesce(col("total_juros_mora_pago_op"), lit(0))) \
    .withColumn("soma_valor_prazo_cliente", sum("total_valor_prazo_op").over(w_cliente)) \
    .withColumn("receita_desagio_cliente", sum("desagio").over(w_cliente)) \
    .withColumn("receita_total_cliente", sum("receita_total_op").over(w_cliente)) \
    .withColumn("custo_financeiro_cliente", sum("custo_financeiro_op").over(w_cliente)) \
    .withColumn("spread_cliente", sum("spread_op").over(w_cliente)) \
    .withColumn("volume_operado_cliente", sum("valor_de_face").over(w_cliente)) \
    .withColumn("qtd_operacoes_cliente", count("cod_operacao").over(w_cliente)) \
    .withColumn("taxa_media_ponderada_mensal_cliente", 
                when(col("soma_valor_prazo_cliente") > 0, 
                     (col("receita_desagio_cliente") / col("soma_valor_prazo_cliente")) * 30 * 100
                ).otherwise(0)) \
    .withColumn("rentabilidade_percentual_cliente", 
                when(col("volume_operado_cliente") > 0, 
                     (col("receita_total_cliente") / col("volume_operado_cliente")) * 100
                ).otherwise(0)) \
    .withColumn("taxa_operacao",
                when(col("total_valor_prazo_op") > 0,
                     (col("desagio") / col("total_valor_prazo_op")) * 30 * 100
                ).otherwise(0)) \
    .withColumn("prazo_medio_operacao",
                when(col("valor_de_face") > 0,
                     col("total_valor_prazo_op") / col("valor_de_face")
                ).otherwise(0)) \
    .withColumn("prazo_medio_ponderado_cliente",
                when(col("volume_operado_cliente") > 0,
                     col("soma_valor_prazo_cliente") / col("volume_operado_cliente")
                ).otherwise(0)) \
    .select(
        # Identificadores da Operação
        col("cod_operacao"),
        col("nbordero"),
        col("data_deferimento"),
        col("cod_cliente"),
        col("nome").alias("nome_cliente"),
        col("grupo_economico"),
        col("produto_final").alias("produto"),
        # Perfil Cliente
        col("qualidade_cliente"),
        col("status_risco"),
        col("risco").alias("risco_total_atual"),
        col("risco_comissaria").alias("risco_comissaria_atual"),
        # Métricas da Operação Individual
        col("valor_de_face").alias("volume_operacao"),
        col("desagio").alias("receita_desagio_op"),
        col("total_de_tarifas").alias("receita_tarifas_op"),
        col("total_juros_mora_pago_op").alias("receita_juros_mora_op"),
        col("receita_total_op"),
        col("custo_financeiro_op").alias("custo_financeiro"),
        col("spread_op").alias("spread"),
        round(col("taxa_operacao"), 4).alias("taxa_operacao"),
        round(col("prazo_medio_operacao"), 2).alias("prazo_medio_operacao"),
        # Métricas Agregadas do Cliente (Repetidas nas linhas)
        col("volume_operado_cliente"),
        col("qtd_operacoes_cliente"),
        round(col("taxa_media_ponderada_mensal_cliente"), 4).alias("taxa_media_pond_2025_cliente"),
        round(col("prazo_medio_ponderado_cliente"), 2).alias("prazo_medio_ponderado_cliente"),
        round(col("rentabilidade_percentual_cliente"), 4).alias("rentabilidade_perc_cliente"),
        round(col("receita_total_cliente"), 2).alias("receita_total_cliente"),
        round(col("custo_financeiro_cliente"), 2).alias("custo_financeiro_cliente"),
        round(col("spread_cliente"), 2).alias("spread_cliente")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5. Output e Análise
print("Gerando output...")

# Para visualização (Top 20 Clientes), agregamos para evitar duplicatas visuais
df_top_clientes = df_report.select(
    "cod_cliente", "nome_cliente", "grupo_economico", 
    "volume_operado_cliente", "qtd_operacoes_cliente", 
    "taxa_media_pond_2025_cliente", "rentabilidade_perc_cliente", "receita_total_cliente",
    "custo_financeiro_cliente", "spread_cliente"
).dropDuplicates(["cod_cliente"])

# Ordenar por Taxa Média Cliente (Menores Taxas Primeiro)
df_menores_taxas = df_top_clientes.filter(col("volume_operado_cliente") > 10000) \
    .orderBy(col("taxa_media_pond_2025_cliente").asc())

print("Top 20 Clientes com Menores Taxas Médias em 2025 (Volume > 10k):")
df_menores_taxas.show(20, truncate=False)

# Ordenar por Rentabilidade Total (Maiores Receitas)
print("Top 20 Clientes Mais Rentáveis (Receita Total):")
df_top_clientes.orderBy(col("receita_total_cliente").desc()).show(20, truncate=False)

# Salvar Tabela Gold (Granularidade: Operação)
output_table = "LH_Gold.relatorio_rentabilidade_clientes_2025"
df_report.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)
print(f"Relatório detalhado salvo em: {output_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 6. Validação de Dados (Conciliação)
# -----------------------------------
print("\n--- Seção de Validação de Dados (Conciliação) ---")

# Validacao para o cliente especifico reportado (cod_cliente 15258059)
print("Validando Cliente 15258059 (Conciliação)...")
df_validacao = df_report.filter(col("cod_cliente") == 15258059) \
    .select(
        "nbordero", "cod_operacao", "data_deferimento", 
        "volume_operacao", "produto", "status_risco"
    ).orderBy("data_deferimento")

count_ops = df_validacao.count()
sum_volume = df_validacao.agg(sum("volume_operacao")).collect()[0][0]

print(f"Total de Operações Encontradas: {count_ops}")
print(f"Volume Total Encontrado: {sum_volume}")
print("\nDetalhe das Operações:")
df_validacao.show(200, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
