# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

# Fabric notebook source

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Notebook de Preparação de Dados de Contato (Silver)
# **Objetivo:** Este notebook é responsável por ler os dados de contato (endereços, emails, telefones) da camada **Bronze**, aplicar lógicas de "desdobramento" (unfolding), limpeza e desduplicação, e salvar os dados resultantes na camada **Silver** como tabelas de staging.

# MARKDOWN ********************

# ## Seção 0: Configuração do ambiente Python

# CELL ********************

# Célula 0: Configuração da Sessão Spark
# ------------------------------------

# Corrige o problema de LEITURA de datas antigas (formato LEGACY do parquet)
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")

# Corrige o problema de ESCRITA de datas antigas (formato LEGACY do parquet)
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

# Importando as funções necessárias do PySpark
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, when, lit, split, explode, trim

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 1: Limpeza e Desdobramento de E-mails

# MARKDOWN ********************

# **Descrição:** Esta seção processa os dados de **e-mail** da tabela de cadastro geral. O principal desafio é o "desdobramento", onde um único campo de texto pode conter múltiplos emails separados por um delimitador (ex: ';'). O processo abaixo lê a tabela, desdobra os emails em linhas individuais, limpa os dados e remove duplicatas, mantendo apenas o registro mais recente para cada cliente.

# CELL ********************

# Célula 1: Carregamento e Preparação dos Dados
# ---------------------------------------------
# Para evitar leituras repetidas, carregamos a tabela de origem (`cad_geral_pf_pj`) uma única vez.
# As seções seguintes (Email, Telefone, Endereço) irão consumir o DataFrame `df_bronze_source`.

source_lakehouse = "LH_Bronze"
target_lakehouse = "LH_Silver"
source_table = "cad_geral_pf_pj"

print(f"Carregando e preparando os dados de: {source_lakehouse}.{source_table}")

df_bronze_source = spark.read.table(f"{source_lakehouse}.{source_table}")

# Parâmetros para a seção de E-mail
target_table_email = "staging_email_limpa"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 2: Reutilização dos Dados Brutos para E-mail
# ---------------------------------------------------
df_email_bronze = df_bronze_source

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 3: Lógica de Desdobramento e Limpeza
# ----------------------------------------------------

# Assumindo que a coluna com múltiplos emails se chama 'EMAIL' e o delimitador é ';'.
# E que a chave do cliente é 'CPFCNPJ'.
key_columns_email = ["CPFCNPJ"]
email_column = "EMAIL"
delimiter = ";"

# 1. Desdobrar (unfold) a coluna de emails.
#    A função `split` divide a string em um array, e `explode` cria uma nova linha para cada item no array.
df_unfolded_email = df_email_bronze.withColumn(
    "EMAIL_INDIVIDUAL",
    explode(split(col(email_column), delimiter))
)

# 2. Limpar os dados do email desdobrado.
#    A função `trim` remove espaços em branco no início e no fim da string.
df_cleaned_email = df_unfolded_email.withColumn(
    "EMAIL_INDIVIDUAL",
    trim(col("EMAIL_INDIVIDUAL"))
)

print("Desdobramento e limpeza de emails concluídos.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 4: Lógica de Desduplicação
# ----------------------------------------------------

# Após o desdobramento, podemos ter emails duplicados para o mesmo cliente.
# Vamos usar a mesma lógica do outro notebook para manter o mais recente.
# A chave de desduplicação agora é o cliente e o email individual.
key_columns_dedup_email = ["CPFCNPJ", "EMAIL_INDIVIDUAL"]
order_by_column_email = "DATAALTERACAO"

windowSpec_email = Window.partitionBy([col(c) for c in key_columns_dedup_email]).orderBy(col(order_by_column_email).desc())

df_ranked_email = df_cleaned_email.withColumn("row_num", row_number().over(windowSpec_email))
df_deduplicated_email = df_ranked_email.filter(col("row_num") == 1).drop("row_num")

print("Desduplicação de emails concluída.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 5: Salvar o Resultado Limpo na Camada Silver
# ------------------------------------------------------
output_path_email = f"{target_lakehouse}.{target_table_email}"

# Selecionar colunas finais e renomear se necessário
df_final_email = df_deduplicated_email.select(
    col("CPFCNPJ"),
    col("EMAIL_INDIVIDUAL").alias("EMAIL"),
    col("DATAALTERACAO")
    # Adicione outras colunas que queira manter da tabela original
)

df_final_email.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_email)

print(f"Tabela de emails limpa salva com sucesso em: {output_path_email}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 2: Limpeza e Desdobramento de Telefones

# MARKDOWN ********************

# **Descrição:** Esta seção processa os dados de **telefone** da tabela de cadastro geral. Similarmente à tabela de emails, um único campo pode conter múltiplos números de telefone. O processo desdobra esses números, limpa e desduplica os dados.

# CELL ********************

# Célula 1: Identificação e Parâmetros
# ------------------------------------
target_table_tel = "staging_telefones_limpa"

print(f"\nIniciando a limpeza de Telefones da tabela: {source_lakehouse}.{source_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 2: Reutilização dos Dados Brutos para Telefone
# -----------------------------------------------------
df_tel_bronze = df_bronze_source

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 3: Lógica de Desdobramento e Limpeza
# ----------------------------------------------------

# Assumindo que a coluna com múltiplos telefones se chama 'TELEFONE'
key_columns_tel = ["CPFCNPJ"]
tel_column = "TELEFONE"
delimiter = ";"

df_unfolded_tel = df_tel_bronze.withColumn(
    "TELEFONE_INDIVIDUAL",
    explode(split(col(tel_column), delimiter))
)

df_cleaned_tel = df_unfolded_tel.withColumn(
    "TELEFONE_INDIVIDUAL",
    trim(col("TELEFONE_INDIVIDUAL"))
)

print("Desdobramento e limpeza de telefones concluídos.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 4: Lógica de Desduplicação
# ----------------------------------------------------
key_columns_dedup_tel = ["CPFCNPJ", "TELEFONE_INDIVIDUAL"]
order_by_column_tel = "DATAALTERACAO"

windowSpec_tel = Window.partitionBy([col(c) for c in key_columns_dedup_tel]).orderBy(col(order_by_column_tel).desc())

df_ranked_tel = df_cleaned_tel.withColumn("row_num", row_number().over(windowSpec_tel))
df_deduplicated_tel = df_ranked_tel.filter(col("row_num") == 1).drop("row_num")

print("Desduplicação de telefones concluída.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 5: Salvar o Resultado Limpo na Camada Silver
# ------------------------------------------------------
output_path_tel = f"{target_lakehouse}.{target_table_tel}"

df_final_tel = df_deduplicated_tel.select(
    col("CPFCNPJ"),
    col("TELEFONE_INDIVIDUAL").alias("TELEFONE"),
    col("DATAALTERACAO")
)

df_final_tel.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_tel)

print(f"Tabela de telefones limpa salva com sucesso em: {output_path_tel}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 3: Limpeza e Desduplicação de Endereços

# MARKDOWN ********************

# **Descrição:** Esta seção processa os dados de **endereço** da tabela de cadastro geral. Diferente de emails e telefones, é mais comum que um cliente tenha um endereço principal por registro. Portanto, o foco aqui é a desduplicação para garantir que estamos usando o endereço mais atualizado para cada cliente, baseado na data de alteração.

# CELL ********************

# Célula 1: Identificação e Parâmetros
# ------------------------------------
target_table_end = "staging_enderecos_limpa"

print(f"\nIniciando a limpeza de Endereços da tabela: {source_lakehouse}.{source_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 2: Reutilização dos Dados Brutos para Endereço
# -----------------------------------------------------
df_end_bronze = df_bronze_source

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 3: Lógica de Desduplicação
# ----------------------------------------------------
# A chave de negócio é o cliente. Queremos o endereço mais recente.
key_columns_end = ["CPFCNPJ"]
order_by_column_end = "DATAALTERACAO"

windowSpec_end = Window.partitionBy([col(c) for c in key_columns_end]).orderBy(col(order_by_column_end).desc())

df_ranked_end = df_end_bronze.withColumn("row_num", row_number().over(windowSpec_end))

# Filtra para manter apenas a linha mais recente de cada cliente.
df_deduplicated_end = df_ranked_end.filter(col("row_num") == 1).drop("row_num")

print("Desduplicação de endereços concluída.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula 4: Salvar o Resultado Limpo na Camada Silver
# ------------------------------------------------------
output_path_end = f"{target_lakehouse}.{target_table_end}"

# Não há necessidade de renomear colunas se os nomes já estão bons.
df_deduplicated_end.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_end)

print(f"Tabela de endereços limpa salva com sucesso em: {output_path_end}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
