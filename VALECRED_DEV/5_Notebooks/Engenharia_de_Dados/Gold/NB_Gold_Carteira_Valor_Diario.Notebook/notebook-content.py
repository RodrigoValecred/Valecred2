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
# META       "default_lakehouse_workspace_id": "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Notebook Carteira Valor Diário (Gold)
# **Objetivo:** Calcular o valor diário da carteira (Volume Ativo) explodindo o período de vigência dos títulos.
# **Origem:** `LH_Gold.fato_titulos`, `LH_Gold.fato_operacoes`, `LH_Gold.dim_produtos`.
# **Destino:** `LH_Gold.gold_carteira_valor_diario`.

# CELL ********************

# Configuração e Imports
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

from pyspark.sql.functions import col, lit, sequence, explode, coalesce, current_date, sum, to_date, array
from pyspark.sql import functions as F

print("Iniciando cálculo de Valor Diário da Carteira...")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Leitura de Dados
print("Lendo tabelas Gold...")

# Fato Operações
# Filtro: Apenas operações Aceitas (status_aceite='A') e Deferidas (status_analise='D')
df_operacoes = spark.read.table("LH_Gold.fato_operacoes") \
    .filter(col("status_aceite") == "A") \
    .filter(col("status_analise") == "D") \
    .select(
        col("cod_operacao"),
        col("data_deferimento"),
        col("nome_plataforma"),
        col("chave_produto")
    )

# Fato Títulos
# Filtro: Apenas títulos aceitos (aceito='S')
# Selecionamos 'valor' (Valor de Face) como métrica de volume.
df_titulos = spark.read.table("LH_Gold.fato_titulos") \
    .filter(col("aceito") == "S") \
    .select(
        col("cod_operacao"),
        col("cod_titulo"),
        col("valor"),
        col("liquidacao")
    )

# Dimensão Produtos
# Para obter a descrição comercial do produto
df_produtos = spark.read.table("LH_Gold.dim_produtos") \
    .select(
        col("chave_produto"),
        col("produto_informacao_de_mercado")
    )

print("Leitura concluída.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. Joins e Enriquecimento
print("Realizando joins...")

# Join Operações + Títulos + Produtos
# Left join com produtos para não perder operações com produto não mapeado (embora deva existir)
df_base = df_titulos.join(df_operacoes, "cod_operacao", "inner") \
    .join(df_produtos, "chave_produto", "left")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3. Explosão de Datas (Lógica de Carteira Diária)
print("Explodindo datas para cálculo diário...")

# Definir Data Final: Se liquidacao is null, considera hoje.
df_dates = df_base.withColumn("data_inicio", to_date(col("data_deferimento"))) \
    .withColumn("data_fim", coalesce(to_date(col("liquidacao")), current_date()))

# Filtrar datas inválidas (onde inicio > fim ou nulas) para evitar erro no sequence
df_valid_dates = df_dates.filter(
    col("data_inicio").isNotNull() &
    col("data_fim").isNotNull() &
    (col("data_inicio") <= col("data_fim"))
)

count_invalid = df_dates.count() - df_valid_dates.count()
if count_invalid > 0:
    print(f"Aviso: {count_invalid} registros ignorados por data de deferimento > data final ou datas nulas.")

# Explode sequence
# Gera uma linha para cada dia entre data_inicio e data_fim
# INTERVAL 1 DAY é implícito se default, mas sequence(start, end) funciona para datas.
df_exploded = df_valid_dates.withColumn("data_referencia", explode(sequence(col("data_inicio"), col("data_fim"))))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 4. Agregação
print("Agregando valores por Data, Plataforma e Produto...")

# Agrupamento
# Se produto for nulo, substitui por "NÃO IDENTIFICADO"
df_agg = df_exploded.groupBy(
    col("data_referencia"),
    col("nome_plataforma"),
    coalesce(col("produto_informacao_de_mercado"), lit("NÃO IDENTIFICADO")).alias("produto")
).agg(
    sum("valor").alias("valor_carteira")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5. Validação e Output
print("Validando resultado (Top 20)...")
df_agg.orderBy(col("data_referencia").desc()).show(20, truncate=False)

output_table = "LH_Gold.gold_carteira_valor_diario"
print(f"Salvando tabela em: {output_table}")

df_agg.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)
print("Processo concluído com sucesso.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
