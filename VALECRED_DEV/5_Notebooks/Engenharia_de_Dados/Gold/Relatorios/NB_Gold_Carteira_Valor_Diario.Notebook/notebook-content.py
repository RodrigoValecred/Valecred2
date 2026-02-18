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

# # Notebook de Geração da Carteira Diária (Gold)
# **Objetivo:** Calcular o valor diário da carteira (estoque) explodindo o período de vigência de cada título.
# 
# **Lógica:** Baseado na regra DAX: `Start <= Contexto <= End`.
# 
# **Tabelas Origem:** `LH_Gold.fato_operacoes`, `LH_Gold.fato_titulos`.
# 
# **Tabela Destino:** `LH_Gold.gold_carteira_valor_diario`.

# MARKDOWN ********************

# ## Seção 0: Configuração e Leitura

# CELL ********************

# Célula 0.1: Configuração e Imports
# ----------------------------------
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

from pyspark.sql.functions import (
    col, explode, sequence, sum, lit, coalesce, current_date, to_date, 
    when, datediff, count, max, min, round, abs
)
from pyspark.sql.types import DateType
import datetime

print("Configuração concluída.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 0.2: Leitura dos Dados (Gold)
# ------------------------------------
print("Carregando tabelas Gold...")

# Operações: Necessário para obter data_deferimento
df_ops = spark.read.table("LH_Gold.fato_operacoes").select(
    "cod_operacao", 
    "data_deferimento", 
    "cod_empresa", 
    "cod_cliente",
    "status_analise",
    "status_aceite",
    "nome_plataforma",
    "sk_produto",
    "taxa_operacao"
)

# Títulos: Base do valor e liquidação
df_titulos = spark.read.table("LH_Gold.fato_titulos").select(
    "cod_titulo", 
    "cod_operacao", 
    "valor", 
    "liquidacao", 
    "aceito"
)

# Dimensão Produto: Para obter nome do produto
df_prod = spark.read.table("LH_Gold.dim_produtos").select(
    "sk_produto",
    "produto_informacao_de_mercado"
)

print(f"Operações carregadas: {df_ops.count()}")
print(f"Títulos carregados: {df_titulos.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 1: Processamento da Lógica de Estoque Diário

# CELL ********************

# Célula 1.1: Join e Definição de Intervalos
# ------------------------------------------
print("Aplicando lógica de negócio...")

# Filtro 1: Operações Deferidas e Aceitas
df_ops_filtered = df_ops.filter(
    (col("status_analise") == "D") &
    (col("status_aceite") == "A")
)

# Filtro 2: Títulos Aceitos
df_titulos_filtered = df_titulos.filter(col("aceito") == "S")

# Enriquecimento com Produto
df_ops_enriched = df_ops_filtered.join(df_prod, "sk_produto", "left")

# Join para enriquecer Títulos com Data de Deferimento da Operação
# Inner Join: Títulos sem operação correspondente na fato_operacoes são descartados (consistência)
df_joined = df_titulos_filtered.join(df_ops_enriched, "cod_operacao", "inner")

# Definição de Datas (Start/End)
# Start Date: Data de Deferimento da Operação
# End Date: Data de Liquidação do Título OU Data Atual (se ainda não liquidado)
# Regra DAX: titulos[LIQUIDACAO] >= DataContexto || ISBLANK ( titulos[LIQUIDACAO] )
# Isso significa que o título existe até o dia da liquidação (inclusive) ou hoje.
df_dates = (
    df_joined
    .withColumn("start_date", to_date(col("data_deferimento")))
    .withColumn("end_date", coalesce(to_date(col("liquidacao")), current_date()))
    # Filtro de Sanidade: Data Final deve ser maior ou igual a Data Inicial
    .filter(col("start_date") <= col("end_date"))
)

print(f"Títulos ativos elegíveis para explosão: {df_dates.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 1.2: Explosão Diária (Sequence)
# --------------------------------------
print("Gerando snapshot diário (pode demorar)...")

# Gera uma linha para cada dia entre start_date e end_date
df_exploded = df_dates.withColumn(
    "data_referencia", 
    explode(sequence(col("start_date"), col("end_date")))
)

# Agregação Diária por Empresa e Cliente + Plataforma e Produto
# Soma do Valor Nominal dos títulos ativos naquele dia
df_daily_agg = df_exploded.groupBy(
        "data_referencia",
        "cod_empresa",
        "cod_cliente",
        "nome_plataforma",
        "produto_informacao_de_mercado"
    ) \
    .agg(
        sum("valor").alias("valor_carteira_total"),
        sum(col("valor") * col("taxa_operacao")).alias("soma_ponderada_risco"),
        count("cod_titulo").alias("qtd_titulos_ativos")
    ) \
    .withColumn(
        "taxa_media_ponderada",
        col("soma_ponderada_risco") / col("valor_carteira_total")
    )

print("Agregação diária concluída.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 2: Persistência e Dashboard

# CELL ********************

# Célula 2.1: Salvar Tabela Gold
# ------------------------------
target_table = "LH_Gold.gold_carteira_valor_diario"
print(f"Salvando em {target_table}...")

df_daily_agg.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)

print("Salvo com sucesso.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 2.2: Dashboard Rápido de Saída (UX)
# ------------------------------------------
# Exibe um resumo visual do processamento para o operador
try:
    # Coleta métricas básicas
    row_count = df_daily_agg.count()
    max_date = df_daily_agg.agg(max("data_referencia")).collect()[0][0]
    total_value_latest = df_daily_agg.filter(col("data_referencia") == max_date).agg(sum("valor_carteira_total")).collect()[0][0] or 0

    print("\n" + "="*40)
    print("      DASHBOARD DE SAÍDA - CARTEIRA      ")
    print("="*40)
    print(f"Status:       [SUCESSO] ✅")
    print(f"Tabela:       {target_table}")
    print(f"Linhas Ger.:  {row_count:,}".replace(",", "."))
    print(f"Data Ref.:    {max_date}")
    print(f"Valor Total:  R$ {total_value_latest:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
    print("-" * 40)
    
    # Barra de Progresso Visual (Simulada para UX)
    print("Progresso: [████████████████████] 100%")
    print("="*40 + "\n")

except Exception as e:
    print(f"Erro ao gerar dashboard: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
