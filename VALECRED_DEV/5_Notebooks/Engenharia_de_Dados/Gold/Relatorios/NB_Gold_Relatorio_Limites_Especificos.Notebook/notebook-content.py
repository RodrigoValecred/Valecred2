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
# META         },
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

# # Relatório de Limites Específicos
# **Objetivo:** Gerar a tabela `LH_Gold.relatorio_limites_especificos` contendo as informações:
# - Nome do Grupo (`nome_grupo`)
# - Nome do Cliente (`nome_cliente`)
# - Nome do Sacado (`nome_sacado`)
# - Valor do Limite Específico do Sacado (`valor_limite_especifico`)
# - Valor do Risco em Aberto do Sacado (`valor_risco_em_aberto`)


# CELL ********************

# Configuração e Imports
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

from pyspark.sql.functions import col, coalesce, lit, sum, avg, collect_set, array
from notebookutils import mssparkutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Leitura das Tabelas
print("Lendo tabelas da camada Silver e Gold...")
df_limites = spark.read.table("LH_Silver.staging_rlc_clientes_sacados_limites")
df_grupos = spark.read.table("LH_Silver.sup_grupos_economicos")
df_clientes = spark.read.table("LH_Silver.staging_clientes_limpa")
df_cad_geral = spark.read.table("LH_Silver.staging_cad_geral_pf_pj_limpa")
df_sacados = spark.read.table("LH_Gold.dim_sacados")
df_titulos = spark.read.table("LH_Gold.fato_titulos")
df_ops = spark.read.table("LH_Gold.fato_operacoes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. Cálculo do Risco em Aberto
print("Calculando o valor do risco em aberto...")

# Filtro de Operações: status_aceite = A e status_analise = D
df_ops_filtered = df_ops.filter(
    (col("status_aceite") == "A") &
    (col("status_analise") == "D")
).select("cod_operacao", "taxa_operacao").dropDuplicates(["cod_operacao"])

# Filtro de Títulos e Join com Operações: liquidação nula, aceito='S' e t_doc não é 'BL'
df_titulos_ativos = df_titulos.dropDuplicates(["cod_titulo"]).filter(
    col("liquidacao").isNull() &
    (col("t_doc") != "BL") &
    (col("aceito") == "S")
).join(df_ops_filtered, "cod_operacao", "inner")

# O risco em aberto utiliza o "valor_devido" em vez de apenas "valor"
df_risco = df_titulos_ativos.groupBy("cpf_cnpj_sacado").agg(
    sum("valor_devido").alias("valor_risco_em_aberto"),
    avg("taxa_operacao").alias("taxa_media"),
    collect_set("cod_titulo").alias("titulos_risco")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3. Preparação das Dimensões para os Nomes
print("Preparando dimensões para cruzamento...")

# Nomes dos Clientes
df_clientes_nome = df_clientes.join(
    df_cad_geral, "cpf_cnpj", "left"
).select(
    col("cod_cliente"), col("nome").alias("nome_cliente")
)

# Nomes dos Grupos Econômicos
df_grupos_nome = df_grupos.select(
    col("codcliente").alias("cod_cliente"),
    col("nomegrupo").alias("nome_grupo")
)

# Base de Limites (Selecionando o valor específico do sacado)
df_limites_base = df_limites.select(
    col("cod_cliente"),
    col("cpf_cnpj").alias("cpf_cnpj_sacado"),
    col("tipo"),
    col("valor").alias("valor_limite_especifico")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 4. Cruzamento e Consolidação (Join)
print("Realizando joins para montar a tabela final...")

df_relatorio = df_limites_base.join(
    df_clientes_nome, "cod_cliente", "left"
).join(
    df_grupos_nome, "cod_cliente", "left"
).join(
    df_sacados, col("cpf_cnpj_sacado") == df_sacados.cpf_cnpj, "left"
).join(
    df_risco, "cpf_cnpj_sacado", "left"
).select(
    coalesce(col("nome_grupo"), lit("SEM GRUPO")).alias("nome_grupo"),
    col("nome_cliente"),
    col("nome_sacado"),
    col("tipo"),
    col("valor_limite_especifico"),
    coalesce(col("valor_risco_em_aberto"), lit(0.0)).alias("valor_risco_em_aberto"),
    coalesce(col("taxa_media"), lit(0.0)).alias("taxa_media"),
    coalesce(col("titulos_risco"), array()).alias("titulos_risco")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5. Escrita na Camada Gold
output_table = "LH_Gold.relatorio_limites_especificos"
print(f"Salvando relatório em {output_table}...")

df_relatorio.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)

print("Tabela do relatório de limites específicos criada com sucesso.")
mssparkutils.notebook.exit("Success")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
