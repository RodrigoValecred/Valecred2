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
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Notebook de Carga de Dados de CEP e Coordenadas
# **Objetivo:** Baixar a base de CEPs do Brasil contendo Latitude e Longitude e carregar na camada Bronze (LH_Bronze).
# **Fonte:** GitHub (Maahzuka/database-CEPS)
# **Processo:**
# 1. Download do arquivo ceps.xlsx direto do repositório no GitHub.
# 2. Leitura com Pandas, tratamento de nomes de colunas e dados.
# 3. Escrita no Lakehouse Bronze como uma tabela Delta para cruzamento posterior.

# CELL ********************

import pandas as pd
from pyspark.sql.types import *
import pyspark.sql.functions as F

# Habilitar o PyArrow para otimizar a conversão Pandas -> Spark
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# Configurar rebase de datas (padrao para tabelas fora da RFB)
spark.conf.set('spark.sql.parquet.datetimeRebaseModeInWrite', 'LEGACY')

# URL do arquivo no GitHub (raw)
url = "https://github.com/Maahzuka/database-CEPS/raw/main/ceps.xlsx"

print(f"Baixando dados de {url}...")
# Lendo direto com Pandas. Pode demorar alguns segundos dependendo do tamanho.
df_pandas = pd.read_excel(url)

print(f"Total de registros lidos: {len(df_pandas)}")

# Limpeza e padronização dos nomes das colunas para snake_case
df_pandas.columns = [c.lower().replace(" ", "_").strip() for c in df_pandas.columns]

# As colunas latitude e longitude vêm como string com vírgula. Precisamos converter para float.
def converte_para_float(valor):
    if pd.isna(valor):
        return None
    try:
        return float(str(valor).replace(",", "."))
    except:
        return None

# Utilizando list comprehensions nativo Python para performance, conforme as boas práticas do projeto
if "latitude" in df_pandas.columns:
    df_pandas["latitude"] = [converte_para_float(x) for x in df_pandas["latitude"]]
if "longitude" in df_pandas.columns:
    df_pandas["longitude"] = [converte_para_float(x) for x in df_pandas["longitude"]]

# Converter algumas colunas de CEP para string, garantindo os zeros à esquerda
# As faixas de CEP variam de tamanho, mas no Brasil são 8 dígitos
def formata_cep(valor):
    if pd.isna(valor):
        return None
    try:
        return str(int(valor)).zfill(8)
    except:
        return str(valor)

# Utilizando list comprehensions para performance
if "cep_inicial" in df_pandas.columns:
    df_pandas["cep_inicial"] = [formata_cep(x) for x in df_pandas["cep_inicial"]]
if "cep_final" in df_pandas.columns:
    df_pandas["cep_final"] = [formata_cep(x) for x in df_pandas["cep_final"]]

# Para prevenir falhas na inferência do PySpark, garantimos que colunas textuais são str
for col in df_pandas.columns:
    if df_pandas[col].dtype == 'object':
        df_pandas[col] = df_pandas[col].astype(str)

print("Conversão para Spark DataFrame...")
df_spark = spark.createDataFrame(df_pandas)

# Salvando a tabela na camada Bronze
table_name = "LH_Bronze.cep_coordenadas"
print(f"Salvando dados na tabela {table_name}...")

df_spark.write.format("delta").mode("overwrite").saveAsTable(table_name)

print("Carga finalizada com sucesso!")
