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
from pyspark.sql import *
import pyspark.sql.functions as F

# Habilitar o PyArrow para otimizar a conversão pandas -> Spark 
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# Configurar rebase de datas
spark.conf.set('spark.sql.parquet.datetimeRebaseModeInWrite', 'LEGACY')

# URL do arquivo no GitHub (raw)
url = "https://github.com/Maahzuka/database-CEPS/raw/main/ceps.xlsx"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Baixando dados de {url}...")
# Lendo direto com Padas. Pode demorar alguns segundos dependedno do tamanho.
df_pandas = pd.read_excel(url)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Total de registros lidos: {len(df_pandas)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Limpeza e padronização dos nome das colunas para snake_case
df_pandas.columns = [c.lower().replace(" ", "_").strip() for c in df_pandas.columns]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# As strings latitude e longitude vêm como sting com vírgula.
# Precisamos converter para float.
# 🧠 Tensor: Implement Vectorized String Operations for NLP pipelines
# 💡 O que: Substituiu a iteração em Python (list comprehensions e a função `converte_para_float`) por operações vetorizadas nativas do Pandas.
# 🎯 Por que: Iterações linha-a-linha no Python (como `.apply()` ou list comprehensions) são lentas pois não utilizam as otimizações em C do Pandas. Métodos vetorizados processam o bloco de dados de uma só vez, evitando o overhead do loop Python.
# 📊 Impacto: O tempo de conversão de dados é reduzido drasticamente para conjuntos grandes.
# 🔬 Medição: Elimina overhead de laços de iteração Python para processamento de strings e numéricos.

if "latitude" in df_pandas.columns:
    df_pandas["latitude"] = pd.to_numeric(df_pandas["latitude"].astype(str).str.replace(",", ".", regex=False), errors="coerce")
if "longitude" in df_pandas.columns:
    df_pandas["longitude"] = pd.to_numeric(df_pandas["longitude"].astype(str).str.replace(",", ".", regex=False), errors="coerce")
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Converter algumas colunas de CEP para string, garantindo os zeros à esquerda
# As faixas de CEP variam de tamanho, mas no Brasil são 8 dígitos
# 🧠 Tensor: Implement Vectorized String Operations for NLP pipelines
# 💡 O que: Substituiu a iteração em Python (list comprehensions e a função `formata_cep`) por operações vetorizadas do Pandas.
# 🎯 Por que: Similar à conversão de floats, a formatação de CEP com laços for Python é um gargalo; operações `.astype` e `.str.zfill` funcionam diretamente na matriz subjacente em C.
# 📊 Impacto: Processamento mais rápido na limpeza de strings.
# 🔬 Medição: Acelera em múltiplas ordens de grandeza frente ao list comprehension com try/except.

import numpy as np

if "cep_inicial" in df_pandas.columns:
    # 🧠 Tensor: Handle string conversion carefully to avoid padding "<NA>" into "0000<NA>"
    s = pd.to_numeric(df_pandas["cep_inicial"], errors="coerce").astype("Int64").astype(str)
    df_pandas["cep_inicial"] = s.where(s != "<NA>", np.nan).str.zfill(8)
if "cep_final" in df_pandas.columns:
    s = pd.to_numeric(df_pandas["cep_final"], errors="coerce").astype("Int64").astype(str)
    df_pandas["cep_final"] = s.where(s != "<NA>", np.nan).str.zfill(8)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Para previnir falhas na inferência do PySpark, garantimos que colunas textuais são str
for col in df_pandas.columns:
    if df_pandas[col].dtype == 'object':
        df_pandas[col] = df_pandas[col].astype(str)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Conversão para Spark DataFrame...")
df_spark = spark.createDataFrame(df_pandas)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Salvando a tabela na camada Bronze
table_name = "LH_Bronze.cep_coordenadas"
print(f"Savando dados na tabela {table_name}...")

df_spark.write.format("delta").mode("overwrite").saveAsTable(table_name)

print("Carga finalizada com sucesso!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
