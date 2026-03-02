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
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Notebook de Análise de Cliente Específico
# 
# **Objetivo:** Este notebook tem como finalidade analisar o comportamento histórico de um cliente específico, utilizando as mesmas features e regras de negócio do modelo de previsão de inadimplência.
# 
# **Passos:**
# 1. Carregar os dados das tabelas do Lakehouse Silver.
# 2. Construir a tabela mestra com os mesmos joins do modelo.
# 3. Aplicar os filtros de negócio e criar a variável `TARGET`.
# 4. Isolar e analisar o cliente de interesse.

# MARKDOWN ********************

# ## 1. Configuração e Carregamento de Dados
# 
# Carregando as bibliotecas e tabelas necessárias do Lakehouse.

# CELL ********************

from pyspark.sql.functions import col
import pandas as pd
import numpy as np

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# ATENÇÃO: Substitua o valor abaixo pelo CPF/CNPJ do cliente a ser analisado
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
CLIENTE_CPFCNPJ = "14630809000101"

# Tipos de operação a serem excluídos da análise (mesmo padrão do modelo)
tipos_excluir = ['RN', 'RE', 'RC', 'PR', 'AB', 'AM', 'LB', 'PB']

# Configurações do Spark
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")

# Carregamento das tabelas
print("Carregando tabelas do Lakehouse Silver...")
df_titulos = spark.read.table("LH_Silver.staging_titulos_limpa")
df_operacoes = spark.read.table("LH_Silver.staging_operacoes_limpa")
df_cedentes = spark.read.table("LH_Silver.dim_cliente")
df_cad_geral = spark.read.table("LH_Silver.staging_cad_geral_limpa")
print("Tabelas carregadas com sucesso.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Criação da Tabela Mestra
# 
# Replicando os joins para construir a tabela mestra de análise.

# CELL ********************

# Renomeando colunas duplicadas para evitar conflitos
df_operacoes = df_operacoes.withColumnRenamed("TTO", "TTO_OPERACAO")
df_operacoes = df_operacoes.withColumnRenamed("CODRATING", "CODRATING_OPERACAO")
df_cedentes = df_cedentes.withColumnRenamed("CODRATING", "CODRATING_CEDENTE")

# 🧠 TENSOR OPTIMIZATION: Predicate Pushdown and Inner Joins
# Applying filters directly to source tables BEFORE joining to drastically reduce the data footprint.
# Using INNER JOINs means filtering df_cedentes by CPFCNPJ automatically filters operations and titles.
print(f"Filtrando dados para o cliente {CLIENTE_CPFCNPJ} e aplicando regras de negócio em Spark antes dos joins...")

df_cedentes_filtered = df_cedentes.filter(col("CPFCNPJ") == CLIENTE_CPFCNPJ)

df_operacoes_filtered = df_operacoes.filter(
    (col("STATUSANALISE") == 'D') &
    (col("STATUSACEITE") == 'A') &
    (col("ACEITO") == 'S') &
    (~col("TTO_OPERACAO").isin(tipos_excluir))
)

df_titulos_filtered = df_titulos.filter(col("LIQUIDACAO").isNotNull())

df_cad_geral_filtered = df_cad_geral.select("CPFCNPJ", "CIDADE", "UF").dropDuplicates(["CPFCNPJ"]).filter(col("CPFCNPJ") == CLIENTE_CPFCNPJ)

# Realizando os joins otimizados
print("Criando tabela mestra com joins INNER otimizados...")

# First, join operations with the specific client to filter operations
df_op_ced = df_operacoes_filtered.join(df_cedentes_filtered, on="CODCLIENTE", how="inner")

# Then, join filtered titles with the specific client's operations
df_mestra_spark = df_titulos_filtered.join(df_op_ced, on="CODOPERACAO", how="inner")

# Finally, add the general registry data
df_mestra_spark = df_mestra_spark.join(
    df_cad_geral_filtered,
    on="CPFCNPJ",
    how="left"
)

# Convertendo para Pandas para facilitar a manipulação (agora muito mais leve e rápido)
print("Convertendo para DataFrame Pandas...")
df_filtrado = df_mestra_spark.cache()
print("Tabela mestra filtrada e carregada com sucesso.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Filtros de Negócio e Criação da Variável Target
# 
# Aplicando as mesmas regras de negócio do modelo para garantir consistência na análise.

# CELL ********************

# count() on Spark DataFrame is expensive, so omitting exact count in regular runs to save time
print(f"Universo de análise carregado para o cliente {CLIENTE_CPFCNPJ}.")

# Criando a variável Target
from pyspark.sql.functions import col, when

def create_target_variable():
    """
    Cria a expressão para a variável TARGET usando lógica do Spark.

    Returns:
        Column: Expressão Spark retornando 0 (Adimplente) ou 1 (Inadimplente).
    """
    # Classifica um título como adimplente (0) ou inadimplente (1) com base no motivo da baixa e no tipo de operação.
    # Regra: Se MOTIVO for 'PG' -> 0 (Adimplente)
    #        Se MOTIVO for 'RC' e TTO_OPERACAO for 'FC' ou 'CM' -> 0 (Adimplente)
    #        Caso contrário -> 1 (Inadimplente)
    return when(
        (col('MOTIVO') == 'PG') |
        ((col('MOTIVO') == 'RC') & (col('TTO_OPERACAO').isin(['FC', 'CM']))),
        0
    ).otherwise(1)

df_filtrado = df_filtrado.withColumn('TARGET', create_target_variable())
print("Variável TARGET criada.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Análise do Cliente Específico
# 
# Nesta seção, vamos isolar o cliente de interesse e analisar suas características e histórico de pagamentos.

# CELL ********************

print(f"Analisando o cliente com CPF/CNPJ: {CLIENTE_CPFCNPJ}")

# O DataFrame df_filtrado já contém apenas os dados do cliente específico
df_cliente = df_filtrado

if df_cliente.limit(1).count() == 0:
    print("ALERTA: Nenhum título encontrado para este cliente com os filtros aplicados.")
else:
    print(f"Dados encontrados para este cliente.")

    # Features utilizadas pelo modelo
    features_modelo = [
        'VALOR', 'PRAZO', 'DESAGIO', 'TTO_OPERACAO', 'STTO',
        'CODSTATUSCLIENTE', 'CODRATING_CEDENTE', 'FATOR', 'TARIFA',
        'CIDADE', 'UF', 'TARGET'
    ]

    # Filtrando colunas existentes no DataFrame
    features_existentes = [f for f in features_modelo if f in df_cliente.columns]

    # Exibindo o resumo dos títulos do cliente
    print("\nResumo dos Títulos do Cliente (usando features do modelo):")
    df_cliente.select(*features_existentes).show(10)

    # Análise de inadimplência do cliente
    print("\nTaxa de Inadimplência do Cliente:")
    total_count = df_cliente.count()
    if total_count > 0:
        df_cliente.groupBy('TARGET').count().withColumn('pct', col('count') / total_count).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
