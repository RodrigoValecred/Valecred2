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

# Realizando os joins
print("Criando tabela mestra com os joins...")
df_mestra_spark = df_titulos.join(df_operacoes, on="CODOPERACAO", how="left")
df_mestra_spark = df_mestra_spark.join(df_cedentes, on="CODCLIENTE", how="left")
df_mestra_spark = df_mestra_spark.join(
    df_cad_geral.select("CPFCNPJ", "CIDADE", "UF").dropDuplicates(["CPFCNPJ"]),
    on="CPFCNPJ",
    how="left"
)

# Convertendo para Pandas para facilitar a manipulação
print("Convertendo para DataFrame Pandas...")
df_mestra_bruta = df_mestra_spark.toPandas()
print("Tabela mestra criada com sucesso.")

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

# Regra 1: Considerar apenas títulos aceitos e deferidos
df_filtrado = df_mestra_bruta[
    (df_mestra_bruta['STATUSANALISE'] == 'D') &
    (df_mestra_bruta['STATUSACEITE'] == 'A') &
    (df_mestra_bruta['ACEITO'] == 'S')
].copy()

# Regra 2: Desconsiderar renegociações
tipos_excluir = ['RN','RE','RC','PR','AB','AM','LB','PB']
df_filtrado = df_filtrado[~df_filtrado['TTO_OPERACAO'].isin(tipos_excluir)].copy()

# Regra 3: Considerar apenas títulos liquidados para análise histórica
df_filtrado = df_filtrado[df_filtrado['LIQUIDACAO'].notna()].copy()
print(f"Universo de análise contém {len(df_filtrado)} títulos liquidados.")

# Criando a variável Target
def classificar_inadimplencia(row):
    """
    Classifica um título como adimplente (0) ou inadimplente (1) com base no motivo da baixa e no tipo de operação.

    Args:
        row (pd.Series): Uma linha de um DataFrame do Pandas, contendo as colunas 'MOTIVO' e 'TTO_OPERACAO'.

    Returns:
        int: Retorna 0 se o título for considerado adimplente e 1 se for inadimplente.
    """
    motivo = row['MOTIVO']
    tto_operacao = row['TTO_OPERACAO']
    if motivo == 'PG': return 0
    if motivo == 'RC' and tto_operacao in ['FC', 'CM']: return 0
    return 1

df_filtrado['TARGET'] = df_filtrado.apply(classificar_inadimplencia, axis=1)
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

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# ATENÇÃO: Substitua o valor abaixo pelo CPF/CNPJ do cliente a ser analisado
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
CLIENTE_CPFCNPJ = "14630809000101"

print(f"Analisando o cliente com CPF/CNPJ: {CLIENTE_CPFCNPJ}")

# Filtrando o DataFrame para o cliente específico
df_cliente = df_filtrado[df_filtrado['CPFCNPJ'] == CLIENTE_CPFCNPJ].copy()

if df_cliente.empty:
    print("ALERTA: Nenhum título encontrado para este cliente com os filtros aplicados.")
else:
    print(f"Encontrados {len(df_cliente)} títulos para este cliente.")

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
    display(df_cliente[features_existentes])

    # Análise de inadimplência do cliente
    inadimplencia_cliente = df_cliente['TARGET'].value_counts(normalize=True)
    print("\nTaxa de Inadimplência do Cliente:")
    print(inadimplencia_cliente)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
