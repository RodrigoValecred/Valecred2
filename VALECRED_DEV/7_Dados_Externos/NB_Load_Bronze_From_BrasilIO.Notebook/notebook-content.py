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

# # Notebook de Carga de Dados do Brasil.IO
# **Objetivo:** Este notebook é responsável por baixar os datasets públicos do [Brasil.IO](https://brasil.io/dataset/socios-brasil/files/) e carregá-los na camada Bronze do Lakehouse.
# **Datasets:**
# * Empresas
# * Holdings
# * Sócios
# **Processos realizados:**
# 1.  **Download dos Dados:** Baixa os arquivos `.csv.gz` a partir das URLs do Brasil.IO.
# 2.  **Descompressão e Leitura:** Lê os arquivos compactados diretamente para DataFrames Spark.
# 3.  **Gravação na Camada Bronze:** Salva os DataFrames como tabelas no Lakehouse `LH_Bronze`.

# MARKDOWN ********************

# ## Seção 1: Configuração do Ambiente e Download

# CELL ********************

import os
import requests
from notebookutils import mssparkutils

# URLs dos datasets
urls = {
    "empresas": "https://data.brasil.io/dataset/socios-brasil/empresas.csv.gz",
    "holdings": "https://data.brasil.io/dataset/socios-brasil/holdings.csv.gz",
    "socios": "https://data.brasil.io/dataset/socios-brasil/socios.csv.gz"
}

# --- ETAPA 1: BAIXAR OS ARQUIVOS PARA O DIRETÓRIO LOCAL /tmp ---

local_download_path = "/tmp/brasil_io_data"
os.makedirs(local_download_path, exist_ok=True)

# Dicionário para armazenar os caminhos finais dos arquivos no Lakehouse
lakehouse_files = {}

# Loop para baixar cada arquivo
for name, url in urls.items():
    file_name = f"{name}.csv.gz"
    local_file_path = os.path.join(local_download_path, file_name)

    print(f"Baixando {url} para {local_file_path}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()  # Lança um erro se o download falhar

    with open(local_file_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Download de {name} concluído.")

# --- ETAPA 2: MOVER O ARQUIVO LOCAL PARA O LAKEHOUSE ---
    # Define o diretório de destino no seu Lakehouse (dentro da pasta 'Files')
    # Este caminho é relativo ao Lakehouse padrão associado ao seu notebook.
    lakehouse_dir = "Files/brasil_io_data"
    
    # Cria o diretório no Lakehouse se ele ainda não existir
    mssparkutils.fs.mkdirs(lakehouse_dir)

    # Monta o caminho de destino final no Lakehouse
    lakehouse_file_path = os.path.join(lakehouse_dir, file_name)
    
    print(f"Movendo '{local_file_path}' para o Lakehouse em '{lakehouse_file_path}'...")
    
    # O prefixo 'file:' é crucial para indicar que a origem é o sistema de arquivos local.
    # O destino é o caminho relativo no Lakehouse.
    mssparkutils.fs.mv(f"file:{local_file_path}", lakehouse_file_path)
    
    print(f"Arquivo movido para o Lakehouse com sucesso.")
    lakehouse_files[name] = lakehouse_file_path


# --- ETAPA 3: LER OS DADOS DO LAKEHOUSE COM O SPARK ---

print("\n--- Lendo os dados a partir do Lakehouse ---")
for name, path in lakehouse_files.items():
    print(f"Lendo a tabela '{name}' do arquivo '{path}'...")
    
    # Agora o Spark lê o arquivo do caminho correto no Lakehouse, onde ele tem acesso
    df = spark.read.csv(path, header=True, inferSchema=True, sep=',')
    
    print(f"Leitura concluída. Amostra dos dados de '{name}':")
    df.show(5)

    # Opcional: Se desejar, você pode salvar o DataFrame como uma tabela Delta
    # para otimizar futuras consultas e gerenciamento.
    target_table_name = f"brasil_io_{name}"
    print(f"Salvando DataFrame como tabela Delta '{target_table_name}'...")
    df.write.mode("overwrite").format("delta").saveAsTable(target_table_name)
    print("Tabela salva com sucesso.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
