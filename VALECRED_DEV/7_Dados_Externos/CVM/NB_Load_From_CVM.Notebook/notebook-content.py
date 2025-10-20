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

# # Carregar dados da CVM
# 
# Este notebook baixa os dados mensais de FIDC (Fundos de Investimento em Direitos Creditórios) do portal de dados abertos da CVM e os carrega na camada Bronze.
# 
# **Fonte:** https://dados.cvm.gov.br/dataset/fidc-doc-inf_mensal
# 
# ### Como parametrizar este notebook no Microsoft Fabric:
# 1. Adicione uma nova célula no topo.
# 2. Adicione as variáveis que você quer parametrizar, por exemplo:
#    ```python
#    ano = "2025"
#    mes = "09"
#    ```
# 3. Na barra de ferramentas da célula, clique em reticências (...) e selecione "Ativar/desativar célula de parâmetro".
# 4. Ao executar este notebook a partir de um pipeline do Fabric, você poderá passar valores para `ano` e `mes`.

# CELL ********************

import requests
import zipfile
import os
import shutil
import pandas as pd
from pyspark.sql import SparkSession

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Parâmetros do Notebook
# Estes valores podem ser substituídos por parâmetros de pipeline do Fabric.
# Veja a célula de markdown acima para instruções.
ano = "2024"
mes = "07"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Baixar e Descompactar o arquivo ZIP
# 
# Os arquivos serão baixados para o diretório `Files/temp` do Lakehouse para garantir que sejam acessíveis pelo driver.

# CELL ********************

# Lógica para download do arquivo
url = f"https://dados.cvm.gov.br/dados/FIDC/DOC/INF_MENSAL/DADOS/inf_mensal_fidc_{ano}{mes}.zip"
# Usando o diretório 'Files' do Lakehouse para armazenamento temporário.
temp_base_path = f"Files/temp/cvm_fidc_{ano}{mes}"
download_path = f"{temp_base_path}/inf_mensal_fidc.zip"
unzip_path = f"{temp_base_path}/unzipped/"

# Limpa o diretório temporário de execuções anteriores para garantir a idempotência
if os.path.exists(temp_base_path):
    shutil.rmtree(temp_base_path)
os.makedirs(unzip_path, exist_ok=True)

print(f"Baixando arquivo de {url}...")
response = requests.get(url)

if response.status_code == 200:
    with open(download_path, "wb") as f:
        f.write(response.content)
    print(f"Arquivo salvo em {download_path}")

    print(f"Descompactando arquivo em {unzip_path}...")
    with zipfile.ZipFile(download_path, "r") as zip_ref:
        zip_ref.extractall(unzip_path)
    print("Arquivo descompactado com sucesso.")
else:
    raise Exception(f"Falha ao baixar o arquivo. Status code: {response.status_code}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Ler o CSV com Pandas e criar DataFrame Spark
# 
# Para contornar problemas de acesso a arquivos entre o driver e os workers do Spark no Fabric,
# o arquivo CSV é lido na memória do driver usando Pandas e, em seguida, convertido para um DataFrame Spark.

# CELL ********************

import os

# Lógica para ler os arquivos CSV
csv_files = [f for f in os.listdir(unzip_path) if f.endswith('.csv')]
print(f"Arquivos CSV encontrados: {csv_files}")

target_file_prefix = "inf_mensal_fidc_tab_I_"
target_file = next((f for f in csv_files if f.startswith(target_file_prefix)), None)

if not target_file:
    raise Exception(f"Nenhum arquivo com o prefixo '{target_file_prefix}' foi encontrado no ZIP.")

local_csv_path = os.path.join(unzip_path, target_file)
print(f"Lendo o arquivo com Pandas: {local_csv_path}")

# Ler com Pandas
pandas_df = pd.read_csv(local_csv_path, sep=';', encoding='ISO-8859-1', dtype=str)

# Criar DataFrame Spark a partir do DataFrame Pandas
df = spark.createDataFrame(pandas_df)

# Forçar todas as colunas para o tipo String para evitar erros de tipo VOID
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType

for column in df.columns:
    df = df.withColumn(column, col(column).cast(StringType()))
    
# Adicionar colunas de partição (ANO_REF, MES_REF)
from pyspark.sql.functions import lit
df = df.withColumn("ANO_REF", lit(ano)).withColumn("MES_REF", lit(mes))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Salvar na camada Bronze

# CELL ********************

# Lógica para salvar a tabela no Lakehouse Bronze
bronze_table_name = "LH_Bronze.cvm_fidc_informe_mensal"

print(f"Salvando dados na tabela {bronze_table_name}...")

# Habilitar o modo de substituição de partição dinâmica
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

(
    df.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("ANO_REF", "MES_REF")
    .option("mergeSchema", "true")
    .saveAsTable(bronze_table_name)
)

print("Dados salvos com sucesso!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Limpeza

# CELL ********************

# Limpar arquivos temporários
print("Limpando arquivos temporários...")
if os.path.exists(temp_base_path):
    shutil.rmtree(temp_base_path)
print("Limpeza concluída.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
