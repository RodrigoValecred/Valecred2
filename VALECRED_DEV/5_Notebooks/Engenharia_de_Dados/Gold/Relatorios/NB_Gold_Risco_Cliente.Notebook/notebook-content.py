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

# # Notebook NB_Gold_Risco_Cliente
# **Objetivo:** Gerar o relatório consolidado de Risco por Cliente, agregando métricas de inadimplência, exposição e limites de crédito na camada Gold.

# MARKDOWN ********************

# ## 1. Carregamento e Preparação dos Dados
# Carregue os dataframes a partir da sua Lakehouse.

# CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
df_titulos = spark.read.table("LH_Silver.staging_titulos")
df_operacoes = spark.read.table("LH_Silver.staging_operacoes")
df_cedentes = spark.read.table("LH_Silver.staging_clientes")
from pyspark.sql.functions import col, sum, lit
from functools import reduce

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Renomeando colunas duplicadas para evitar conflitos nos joins.

# CELL ********************

df_operacoes = df_operacoes.withColumnRenamed("tto", "tto_operacao")

# Join das tabelas de títulos e operações
df_mestra_spark = df_titulos.join(df_operacoes, on="cod_operacao", how="left")

# Join com a dimensão de cliente para obter o CODCLIENTE
df_cedentes_deduplicado = df_cedentes.dropDuplicates(["cod_cliente"])
df_mestra_spark = df_mestra_spark.join(df_cedentes_deduplicado.select("cod_cliente", "cpf_cnpj"), on="cod_cliente", how="left")

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
    (col('aceito') == 'S') &
    (col('status_analise') == 'D') &
    (col('status_aceite') == 'A') &
    (col('liquidacao').isNull())
)

# Adicionando filtro para manter apenas os produtos de cliente especificados
produtos_cliente = ['NO', 'FC', 'CM', 'RN', 'GR']
df_risco_aberto = df_risco_aberto.filter(col('tto_operacao').isin(produtos_cliente))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Agregação de Risco por Cliente e Produto
# Agregando os dados para calcular o risco por cliente, com colunas para cada tipo de produto (TTO).

# CELL ********************

# 🧠 Tensor: Fornecer valores explícitos para o pivot para evitar re-scans do DataFrame
# 💡 O que: Injetado `produtos_cliente` explicitamente no método `.pivot()`.
# 🎯 Por que: Chamar `.pivot()` sem fornecer a lista dos valores únicos força o PySpark a instigar uma etapa `collect()` escondida. Isso é severamente ineficiente já que acabamos de usar `produtos_cliente` na filtragem, o que significa que os valores já eram conhecidos.
# 📊 Impacto: Contorna compilações de query custosas e saltos I/O globais do cluster no backend.
df_risco_produto = df_risco_aberto.groupBy("cod_cliente").pivot("tto_operacao", produtos_cliente).agg(sum("valor_devido")).na.fill(0)

# Adicionando uma coluna de RiscoTotal que soma os valores de todas as colunas de TTO
tto_cols = [col(c) for c in df_risco_produto.columns if c not in ['cod_cliente']]
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
