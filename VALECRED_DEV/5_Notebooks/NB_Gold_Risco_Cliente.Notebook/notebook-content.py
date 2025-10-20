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
# META         },
# META         {
# META           "id": "ee40705b-0100-49bc-8f35-81d71839f042"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## 1. Carregamento e Preparação dos Dados
# Carregue os dataframes a partir da sua Lakehouse.

# CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
df_titulos = spark.read.table("LH_Silver.staging_titulos_limpa")
df_operacoes = spark.read.table("LH_Silver.staging_operacoes_limpa")
df_cedentes = spark.read.table("LH_Silver.dim_cliente")
from pyspark.sql.functions import col, when, sum, count, lit
from functools import reduce

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Renomeando colunas duplicadas para evitar conflitos nos joins.

# CELL ********************

df_operacoes = df_operacoes.withColumnRenamed("TTO", "TTO_OPERACAO")

# Join das tabelas de títulos e operações
df_mestra_spark = df_titulos.join(df_operacoes, on="CODOPERACAO", how="left")

# Join com a dimensão de cliente para obter o CODCLIENTE
df_mestra_spark = df_mestra_spark.join(df_cedentes.select("CODCLIENTE", "CPFCNPJ"), on="CODCLIENTE", how="left")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Aplicando as Regras de Risco
# Filtrando o universo de análise para títulos em aberto e com status específico.

# CELL ********************

df_risco_aberto = df_mestra_spark.filter(
    (col('ACEITO') == 'S') &
    (col('STATUSANALISE') == 'D') &
    (col('STATUSACEITE') == 'A') &
    (col('LIQUIDACAO').isNull())
)

# Adicionando filtro para manter apenas os produtos de cliente especificados
produtos_cliente = ['NO', 'FC', 'CM', 'RN', 'GR']
df_risco_aberto = df_risco_aberto.filter(col('TTO_OPERACAO').isin(produtos_cliente))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Agregação de Risco por Cliente e Produto
# Agregando os dados para calcular o risco por cliente, com colunas para cada tipo de produto (TTO).

# CELL ********************

df_risco_produto = df_risco_aberto.groupBy("CODCLIENTE").pivot("TTO_OPERACAO").agg(sum("VALORDEVIDO")).na.fill(0)

# Adicionando uma coluna de RiscoTotal que soma os valores de todas as colunas de TTO
tto_cols = [col(c) for c in df_risco_produto.columns if c not in ['CODCLIENTE']]
if tto_cols:
    df_risco_final = df_risco_produto.withColumn("RiscoTotal", reduce(lambda a, b: a + b, tto_cols))
else:
    df_risco_final = df_risco_produto.withColumn("RiscoTotal", lit(0))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Salvando a Tabela Agregada no Gold
# Salvando o resultado na Lakehouse Gold para consumo.

# CELL ********************

# O nome da lakehouse é 'LH_Gold'
# O nome da tabela será 'risco_cliente_produto'
table_name = "LH_Gold.risco_cliente_produto"

df_risco_final.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(table_name)

print(f"Tabela '{table_name}' salva com sucesso na Lakehouse Gold.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Verificação
# Lendo os dados da tabela recém-criada para garantir que a operação foi bem-sucedida.

# CELL ********************

print("Lendo os 10 primeiros registros da tabela criada:")
df_verificacao = spark.read.table(table_name)
df_verificacao.show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
