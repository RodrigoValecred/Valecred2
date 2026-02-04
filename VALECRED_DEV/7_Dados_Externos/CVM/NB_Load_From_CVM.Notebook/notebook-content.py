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
#    lista_periodos = ["202501", "202502"] # Opcional: Para processar múltiplos meses
#    ```
# 3. Na barra de ferramentas da célula, clique em reticências (...) e selecione "Ativar/desativar célula de parâmetro".
# 4. Ao executar este notebook a partir de um pipeline do Fabric, você poderá passar valores para `ano`, `mes` ou `lista_periodos`.

# CELL ********************

import requests
import zipfile
import os
import shutil
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType

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
lista_periodos = [] # Ex: ["202401", "202402"] - Se preenchido, ignora ano/mes individuais

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Processamento dos Dados (Download, Leitura e Salvamento)
# 
# O bloco abaixo itera sobre a lista de períodos (ou o único período definido por `ano` e `mes`), baixa o arquivo da CVM, converte para DataFrame Spark e salva na camada Bronze particionado por Ano e Mês.

# CELL ********************

# Habilitar o modo de substituição de partição dinâmica
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Determinar lista de períodos a processar
if lista_periodos:
    periodos_para_processar = lista_periodos
else:
    periodos_para_processar = [f"{ano}{mes}"]

print(f"Períodos a processar: {periodos_para_processar}")

bronze_table_name = "LH_Bronze.cvm_fidc_informe_mensal"

for periodo in periodos_para_processar:
    current_ano = periodo[:4]
    current_mes = periodo[4:]
    print(f"\n=== Processando: Ano {current_ano}, Mês {current_mes} ===")

    temp_base_path = f"Files/temp/cvm_fidc_{current_ano}{current_mes}"
    
    try:
        # --- 1. Download ---
        url = f"https://dados.cvm.gov.br/dados/FIDC/DOC/INF_MENSAL/DADOS/inf_mensal_fidc_{current_ano}{current_mes}.zip"
        download_path = f"{temp_base_path}/inf_mensal_fidc.zip"
        unzip_path = f"{temp_base_path}/unzipped/"

        # Limpa o diretório temporário de execuções anteriores
        if os.path.exists(temp_base_path):
            shutil.rmtree(temp_base_path)
        os.makedirs(unzip_path, exist_ok=True)

        print(f"Baixando arquivo de {url}...")
        response = requests.get(url)

        if response.status_code != 200:
            print(f"AVISO: Falha ao baixar o arquivo para {periodo}. Status code: {response.status_code}. Pulando...")
            continue

        with open(download_path, "wb") as f:
            f.write(response.content)
        print(f"Arquivo salvo em {download_path}")

        # --- 2. Descompactação ---
        print(f"Descompactando arquivo em {unzip_path}...")
        with zipfile.ZipFile(download_path, "r") as zip_ref:
            zip_ref.extractall(unzip_path)

        # --- 3. Leitura ---
        csv_files = [f for f in os.listdir(unzip_path) if f.endswith('.csv')]
        target_file_prefix = "inf_mensal_fidc_tab_I_"
        target_file = next((f for f in csv_files if f.startswith(target_file_prefix)), None)

        if not target_file:
            print(f"AVISO: Nenhum arquivo com o prefixo '{target_file_prefix}' foi encontrado no ZIP para {periodo}. Pulando...")
            continue

        local_csv_path = os.path.join(unzip_path, target_file)
        print(f"Lendo o arquivo com Pandas: {local_csv_path}")

        # Ler com Pandas
        pandas_df = pd.read_csv(local_csv_path, sep=';', encoding='ISO-8859-1', dtype=str)

        # Criar DataFrame Spark
        df = spark.createDataFrame(pandas_df)

        # Forçar tipo String
        for column in df.columns:
            df = df.withColumn(column, col(column).cast(StringType()))

        # Adicionar colunas de partição
        df = df.withColumn("ANO_REF", lit(current_ano)).withColumn("MES_REF", lit(current_mes))

        # --- 4. Salvamento ---
        print(f"Salvando dados de {periodo} na tabela {bronze_table_name}...")
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("ANO_REF", "MES_REF")
            .option("mergeSchema", "true")
            .saveAsTable(bronze_table_name)
        )
        print(f"SUCESSO: Dados de {periodo} salvos!")

    except Exception as e:
        print(f"ERRO CRÍTICO ao processar {periodo}: {e}")
        # Opcional: raise e se quiser parar tudo, mas num loop geralmente queremos tentar o próximo

    finally:
        # --- 5. Limpeza ---
        print(f"Limpando arquivos temporários para {periodo}...")
        if os.path.exists(temp_base_path):
            shutil.rmtree(temp_base_path)

print("\nProcessamento de todos os períodos concluído.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
