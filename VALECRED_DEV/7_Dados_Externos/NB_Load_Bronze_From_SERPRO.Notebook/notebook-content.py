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

# # Notebook de Carga de Dados de Licitações e Contratos do SERPRO
# **Objetivo:** Este notebook é responsável por baixar os dados de licitações e contratos do Portal da Transparência, filtrar os dados para obter apenas os dados do SERPRO e carregá-los na camada Bronze do Lakehouse.
# **Fonte dos Dados:** [Portal da Transparência - Licitações](https://portaldatransparencia.gov.br/download-de-dados/licitacoes)
# **Processos realizados:**
# 1.  **Download dos Dados:** Baixa o arquivo `.zip` de licitações para um mês específico.
# 2.  **Descompressão e Leitura:** Lê os arquivos CSV de dentro do ZIP para DataFrames Spark.
# 3.  **Filtragem dos Dados:** Filtra os DataFrames para manter apenas os dados relacionados ao SERPRO.
# 4.  **Gravação na Camada Bronze:** Salva os DataFrames filtrados como tabelas Delta no Lakehouse `LH_Bronze`.

# MARKDOWN ********************

# ## Seção 1: Configuração e Download

# MARKDOWN ********************

# **Nota sobre Parametrização:**
# Para um ambiente de produção, a URL e os nomes de arquivo (especialmente o ano e mês `202401`) devem ser parametrizados. Em um ambiente Fabric, isso pode ser feito usando [widgets de notebook](https://docs.microsoft.com/en-us/fabric/data-engineering/notebook-widgets) para permitir a execução para diferentes períodos sem alterar o código.

# CELL ********************

import os
import requests
from notebookutils import mssparkutils
import zipfile
from pyspark.sql.functions import col
from unidecode import unidecode

def safe_extract(zip_ref, path):
    """
    Extracts a zip file to the specified path, preventing Zip Slip vulnerability.
    """
    # Normalize the target path to an absolute path
    target_path = os.path.abspath(path)

    for member in zip_ref.namelist():
        # Resolve the full path of the member
        # Note: os.path.join will discard 'target_path' if 'member' is absolute
        member_path = os.path.join(target_path, member)
        # Normalize the member path to resolve '..' and '.'
        abs_member_path = os.path.abspath(member_path)

        # Check if the member path starts with the target path
        # We append os.sep to ensure we match directory boundaries (e.g. /tmp/foo vs /tmp/foobar)
        if not abs_member_path.startswith(os.path.join(target_path, '')) and not abs_member_path == target_path:
             raise Exception(f"Zip Slip vulnerability detected: {member}")

    zip_ref.extractall(path)

# --- ETAPA 1: DEFINIR OS CAMINHOS E URLS ---
# Parâmetro para o ano e mês dos dados
ANO_MES = "202401"

# URL for the data download
url = f'https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/licitacoes/{ANO_MES}_Licitacoes.zip'
zip_filename = f'{ANO_MES}_Licitacoes.zip'

# Caminho de download local temporário no cluster
local_tmp_path = f"/tmp/serpro_data/{ANO_MES}"
local_download_path = f"{local_tmp_path}/{zip_filename}"
local_unzip_path = f"{local_tmp_path}/unzipped/"

# Caminho de destino no Lakehouse (Files)
lakehouse_unzip_path = f"Files/serpro_data/{ANO_MES}/unzipped/"

# Limpa os diretórios locais de execuções anteriores
if os.path.exists(local_tmp_path):
    import shutil
    shutil.rmtree(local_tmp_path)
os.makedirs(local_unzip_path, exist_ok=True)


# --- ETAPA 2: DOWNLOAD DO ARQUIVO ---
print(f"Baixando {url} para {local_download_path}...")
response = requests.get(url, stream=True, timeout=60) # Added timeout for security
response.raise_for_status()

with open(local_download_path, "wb") as f:
    for chunk in response.iter_content(chunk_size=8192):
        f.write(chunk)
print(f"Download de {zip_filename} concluído.")


# --- ETAPA 3: DESCOMPACTAR LOCALMENTE E MOVER PARA O LAKEHOUSE ---
print(f"Descompactando '{local_download_path}' em '{local_unzip_path}'...")
with zipfile.ZipFile(local_download_path, 'r') as zip_ref:
    safe_extract(zip_ref, local_unzip_path) # Safe extraction
print("Descompressão local concluída.")

print(f"Movendo arquivos descompactados de '{local_unzip_path}' para '{lakehouse_unzip_path}'...")
mssparkutils.fs.mv(f"file:{local_unzip_path}", lakehouse_unzip_path, recurse=True)
print("Arquivos movidos para o Lakehouse com sucesso.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 2: Download dos Dados de Contratos

# CELL ********************

# --- ETAPA 1: DEFINIR OS CAMINHOS E URLS PARA CONTRATOS ---
# URL for the data download
contratos_url = f'https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/contratos/{ANO_MES}_Contratos.zip'
contratos_zip_filename = f'{ANO_MES}_Contratos.zip'

# Caminho de download local temporário no cluster
contratos_local_tmp_path = f"/tmp/serpro_data_contratos/{ANO_MES}"
contratos_local_download_path = f"{contratos_local_tmp_path}/{contratos_zip_filename}"
contratos_local_unzip_path = f"{contratos_local_tmp_path}/unzipped/"

# Caminho de destino no Lakehouse (Files)
contratos_lakehouse_unzip_path = f"Files/serpro_data_contratos/{ANO_MES}/unzipped/"

# Limpa os diretórios locais de execuções anteriores
if os.path.exists(contratos_local_tmp_path):
    import shutil
    shutil.rmtree(contratos_local_tmp_path)
os.makedirs(contratos_local_unzip_path, exist_ok=True)


# --- ETAPA 2: DOWNLOAD DO ARQUIVO DE CONTRATOS ---
print(f"Baixando {contratos_url} para {contratos_local_download_path}...")
response = requests.get(contratos_url, stream=True, timeout=60) # Added timeout for security
response.raise_for_status()

with open(contratos_local_download_path, "wb") as f:
    for chunk in response.iter_content(chunk_size=8192):
        f.write(chunk)
print(f"Download de {contratos_zip_filename} concluído.")


# --- ETAPA 3: DESCOMPACTAR LOCALMENTE E MOVER PARA O LAKEHOUSE (CONTRATOS) ---
print(f"Descompactando '{contratos_local_download_path}' em '{contratos_local_unzip_path}'...")
with zipfile.ZipFile(contratos_local_download_path, 'r') as zip_ref:
    safe_extract(zip_ref, contratos_local_unzip_path) # Safe extraction
print("Descompressão local concluída.")

print(f"Movendo arquivos descompactados de '{contratos_local_unzip_path}' para '{contratos_lakehouse_unzip_path}'...")
mssparkutils.fs.mv(f"file:{contratos_local_unzip_path}", contratos_lakehouse_unzip_path, recurse=True)
print("Arquivos de contratos movidos para o Lakehouse com sucesso.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 2: Processamento e Carga dos Dados de Licitações

# CELL ********************

# --- ETAPA 4: LER E PROCESSAR OS DADOS DE LICITAÇÕES COM SPARK ---

print("\n--- Lendo os dados a partir do Lakehouse ---")
# Leitura do arquivo de licitações
licitacao_df = spark.read.csv(f"{lakehouse_unzip_path}{ANO_MES}_Licitação.csv", header=True, inferSchema=True, sep=';', encoding='latin-1')

# Filtrar para obter apenas licitações do SERPRO
serpro_biddings_df = licitacao_df.filter(col("Nome Órgão") == "SERVICO FEDERAL DE PROCESSAMENTO DE DADOS")
print("Dados de licitações do SERPRO filtrados.")
serpro_biddings_df.show(5)

# Coletar os números de licitação do SERPRO
serpro_bidding_numbers = [row['Número Licitação'] for row in serpro_biddings_df.select('Número Licitação').distinct().collect()]

# Salvar a tabela de licitações do SERPRO
target_table_name = "bronze_serpro_licitacoes"
print(f"Salvando DataFrame como tabela Delta '{target_table_name}'...")
serpro_biddings_df.write.mode("overwrite").format("delta").saveAsTable(target_table_name)
print("Tabela de licitações salva com sucesso.")

# --- ETAPA 5: PROCESSAR E FILTRAR ARQUIVOS RELACIONADOS ---
related_files = {
    "ItemLicitacao": f"{ANO_MES}_ItemLicitação.csv",
    "ParticipantesLicitacao": f"{ANO_MES}_ParticipantesLicitação.csv",
    "EmpenhosRelacionados": f"{ANO_MES}_EmpenhosRelacionados.csv"
}

# MARKDOWN ********************

# ## Seção 3: Processamento e Carga dos Dados de Contratos

# CELL ********************

# --- ETAPA 1: LER E PROCESSAR OS DADOS DE CONTRATOS COM SPARK ---

print("\n--- Lendo os dados de Contratos a partir do Lakehouse ---")
# Leitura do arquivo de contratos
contratos_df = spark.read.csv(f"{contratos_lakehouse_unzip_path}{ANO_MES}_Contratos.csv", header=True, inferSchema=True, sep=';', encoding='latin-1')

# Filtrar para obter apenas contratos do SERPRO
# O nome da coluna pode variar, usamos "Órgão/Entidade Contratante" como uma suposição.
# Se isso falhar, precisaremos inspecionar o schema do dataframe.
serpro_contracts_df = contratos_df.filter(col("Órgão / Entidade Contratante") == "SERVICO FEDERAL DE PROCESSAMENTO DE DADOS")
print("Dados de contratos do SERPRO filtrados.")
serpro_contracts_df.show(5)

# Salvar a tabela de contratos do SERPRO
target_table_name = "bronze_serpro_contratos"
print(f"Salvando DataFrame como tabela Delta '{target_table_name}'...")
serpro_contracts_df.write.mode("overwrite").format("delta").saveAsTable(target_table_name)
print("Tabela de contratos salva com sucesso.")

# Coletar os números de contrato do SERPRO
serpro_contract_numbers = [row['Número Contrato'] for row in serpro_contracts_df.select('Número Contrato').distinct().collect()]

# --- ETAPA 2: PROCESSAR E FILTRAR ARQUIVOS DE CONTRATOS RELACIONADOS ---
related_contract_files = {
    "ContratosAditivos": f"{ANO_MES}_Aditivos.csv",
    "ContratosResponsaveis": f"{ANO_MES}_Responsaveis.csv",
    "ContratosDocumentos": f"{ANO_MES}_DocumentosRelacionados.csv"
}

for name, filename in related_contract_files.items():
    # Normalizar o nome do arquivo para o caminho do lakehouse
    normalized_filename = unidecode(filename)

    print(f"Processando arquivo de contrato relacionado: {filename}")
    df = spark.read.csv(f"{contratos_lakehouse_unzip_path}{filename}", header=True, inferSchema=True, sep=';', encoding='latin-1')

    # Filtrar o DataFrame
    if 'Número Contrato' in df.columns:
        filtered_df = df.filter(col("Número Contrato").isin(serpro_contract_numbers))
    else:
        filtered_df = df # Manter o DataFrame como está se a coluna de contrato não existir

    # Salvar a tabela filtrada
    if filtered_df.count() > 0:
        target_table_name = f"bronze_serpro_{name}"
        print(f"Salvando dados filtrados como tabela Delta '{target_table_name}'...")
        filtered_df.write.mode("overwrite").format("delta").saveAsTable(target_table_name)
        print(f"Tabela '{target_table_name}' salva com sucesso.")
        filtered_df.show(5)
    else:
        print(f"Nenhum dado do SERPRO encontrado em '{filename}'.")

for name, filename in related_files.items():
    # Normalizar o nome do arquivo para o caminho do lakehouse
    normalized_filename = unidecode(filename)

    print(f"Processando arquivo relacionado: {filename}")
    df = spark.read.csv(f"{lakehouse_unzip_path}{filename}", header=True, inferSchema=True, sep=';', encoding='latin-1')

    # Filtrar o DataFrame
    if 'Número Licitação' in df.columns:
        filtered_df = df.filter(col("Número Licitação").isin(serpro_bidding_numbers))
    else:
        filtered_df = df # Manter o DataFrame como está se a coluna de licitação não existir

    # Salvar a tabela filtrada
    if filtered_df.count() > 0:
        target_table_name = f"bronze_serpro_{name}"
        print(f"Salvando dados filtrados como tabela Delta '{target_table_name}'...")
        filtered_df.write.mode("overwrite").format("delta").saveAsTable(target_table_name)
        print(f"Tabela '{target_table_name}' salva com sucesso.")
        filtered_df.show(5)
    else:
        print(f"Nenhum dado do SERPRO encontrado em '{filename}'.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
