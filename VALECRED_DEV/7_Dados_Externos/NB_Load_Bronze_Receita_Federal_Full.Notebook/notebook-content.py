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

# # Notebook de Carga de Dados Completos da Receita Federal
# **Objetivo:** Este notebook é responsável por baixar os dados brutos de CNPJ do site da Receita Federal, processá-los e carregá-los na camada Bronze do Lakehouse.
# **Fonte dos Dados:** Dados públicos de CNPJ da [Receita Federal do Brasil](https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj).
# **Fonte dos Scripts de Processamento:** [turicas/socios-brasil](https://github.com/turicas/socios-brasil)
# **Processos realizados:**
# 1.  **Download dos Dados Brutos:** Baixa os arquivos `.zip` a partir de um mirror do Brasil.IO para o Lakehouse.
# 2.  **Execução do Script de Extração:** Utiliza os scripts Python do repositório `socios-brasil` para descompactar e converter os dados brutos em formato CSV.
# 3.  **Gravação na Camada Bronze:** Salva o DataFrame do arquivo de empresas principal como uma tabela Delta no Lakehouse `LH_Bronze`.


# MARKDOWN ********************

# ## Seção 1: Preparação do Ambiente de Processamento
# Esta célula baixa os scripts Python e os arquivos de configuração necessários do repositório `turicas/socios-brasil` e os armazena no Lakehouse para que possam ser usados nas etapas posteriores.

# CELL ********************

import os
import requests
import hashlib
from notebookutils import mssparkutils

# --- CONFIGURAÇÃO DOS ARQUIVOS DE AJUDA ---

# Dicionário com os arquivos necessários e seus destinos no Lakehouse
helper_files = {
    # Scripts Python
    "extract_dump.py": "Files/RFB_Processor/extract_dump.py",
    # Arquivo de dados para o script
    "data/natureza-juridica.csv": "Files/RFB_Processor/data/natureza-juridica.csv",
    # Arquivos de cabeçalho para o script
    "headers/cnae_secundaria.csv": "Files/RFB_Processor/headers/cnae_secundaria.csv",
    "headers/empresa.csv": "Files/RFB_Processor/headers/empresa.csv",
    "headers/header.csv": "Files/RFB_Processor/headers/header.csv",
    "headers/socio.csv": "Files/RFB_Processor/headers/socio.csv",
    "headers/trailler.csv": "Files/RFB_Processor/headers/trailler.csv",
    "requirements.txt": "Files/RFB_Processor/requirements.txt"
}

# SECURITY: SHA256 hashes of critical files to prevent tampering
expected_hashes = {
    "extract_dump.py": "c53801ddd2e4c04fd69c5d7179b48e365b50048bd6840d27f0d4be7ab0b8e4f4",
    "requirements.txt": "ef9f18112ebaf55988be6cdc869e3382ed224d0ca9cefe382f001f1659431f3f"
}

# SECURITY: Pinning to specific commit hash (7b56360) to prevent supply chain attacks via mutable 'master' branch.
# This ensures that malicious code pushed to the remote repository cannot be automatically executed here.
base_repo_url = "https://raw.githubusercontent.com/turicas/socios-brasil/7b56360e93f35349fe29588dddf7d3c8b07eb22b/"
local_temp_dir = "/tmp/rfb_helpers"

# --- EXECUÇÃO DO DOWNLOAD DOS ARQUIVOS DE AJUDA ---

print("Iniciando a preparação do ambiente de processamento...")
os.makedirs(local_temp_dir, exist_ok=True)

for source_path, lakehouse_dest_path in helper_files.items():
    file_url = f"{base_repo_url}{source_path}"
    file_name = os.path.basename(source_path)
    local_file = os.path.join(local_temp_dir, file_name)

    # Cria o diretório de destino no Lakehouse, se não existir
    lakehouse_dir = os.path.dirname(lakehouse_dest_path)
    mssparkutils.fs.mkdirs(lakehouse_dir)

    print(f"Baixando '{file_name}' de '{file_url}'...")
    try:
        # SECURITY: Added timeout=60 to prevent indefinite hanging (DoS risk)
        r = requests.get(file_url, allow_redirects=True, timeout=60)
        r.raise_for_status()

        # SECURITY: Verify SHA256 hash if the file is critical
        if file_name in expected_hashes:
            file_hash = hashlib.sha256(r.content).hexdigest()
            if file_hash != expected_hashes[file_name]:
                raise ValueError(f"SECURITY ALERT: Hash mismatch for {file_name}. Expected {expected_hashes[file_name]}, got {file_hash}")
            print(f"Verified SHA256 hash for {file_name}")

        with open(local_file, 'wb') as f:
            f.write(r.content)

        # Move o arquivo baixado para o Lakehouse
        print(f"Movendo '{file_name}' para '{lakehouse_dest_path}'")
        mssparkutils.fs.mv(f"file://{local_file}", lakehouse_dest_path, overwrite=True)

    except requests.exceptions.RequestException as e:
        print(f"ERRO: Falha ao baixar {file_url}: {e}")
        # Considerar parar a execução se um arquivo essencial falhar
        # raise e

print("\nAmbiente de processamento preparado com sucesso!")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 2: Configuração e Download dos Dados Brutos


# CELL ********************

import os
import requests
from bs4 import BeautifulSoup
from notebookutils import mssparkutils
import time

# --- ETAPA 1: DEFINIR OS CAMINHOS E URLS ---

# URL oficial para obter a lista de arquivos (URL antiga e funcional que não exige login)
RFB_URL = "https://receita.economia.gov.br/orientacao/tributaria/cadastros/cadastro-nacional-de-pessoas-juridicas-cnpj/dados-publicos-cnpj"
# URL do mirror para download rápido
MIRROR_URL_BASE = "https://data.brasil.io/mirror/socios-brasil"

# Caminho de download local temporário no cluster
local_download_path = "/tmp/rfb_downloads"
os.makedirs(local_download_path, exist_ok=True)

# Caminho de destino no Lakehouse (Files)
lakehouse_download_dir = "Files/RFB_Downloads"
mssparkutils.fs.mkdirs(lakehouse_download_dir)


# --- ETAPA 2: GERAR A LISTA DE ARQUIVOS .ZIP PARA BAIXAR ---

# Com base na URL fornecida pelo usuário, podemos construir as URLs diretamente.
# Isso é mais robusto do que depender de web scraping.
BASE_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-08"
FILE_TYPES = ["Empresas", "Socios", "Estabelecimentos"] # Adicione outros tipos se necessário
NUM_FILES_PER_TYPE = 10 # Assumindo 10 arquivos por tipo (0-9)

file_urls = []
for file_type in FILE_TYPES:
    for i in range(NUM_FILES_PER_TYPE):
        file_urls.append(f"{BASE_URL}/{file_type}{i}.zip")

print(f"Geradas {len(file_urls)} URLs para tentar o download.")
print("Exemplo de URL:", file_urls[0])


# --- ETAPA 3: FAZER O DOWNLOAD DOS ARQUIVOS ---

for url in file_urls:
    file_name = os.path.basename(url)
    local_file_path = os.path.join(local_download_path, file_name)
    lakehouse_file_path = os.path.join(lakehouse_download_dir, file_name)

    print(f"Iniciando o download de '{file_name}' de: {url}")

    # Verifica se o arquivo já existe no lakehouse para evitar re-download
    if mssparkutils.fs.exists(lakehouse_file_path):
        print(f"O arquivo '{file_name}' já existe no Lakehouse. Pulando o download.")
        continue

    # Baixa o arquivo para o /tmp local
    try:
        with requests.get(url, stream=True) as r:
            # Se o arquivo não existir (404), pula para o próximo
            if r.status_code == 404:
                print(f"Arquivo não encontrado em {url}. Pulando.")
                continue
            r.raise_for_status() # Lança erro para outros status HTTP ruins

            with open(local_file_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"Download de '{file_name}' para o diretório local concluído.")

        # Move o arquivo do /tmp local para o Lakehouse
        print(f"Movendo '{local_file_path}' para o Lakehouse em '{lakehouse_file_path}'...")
        mssparkutils.fs.mv(f"file://{local_file_path}", lakehouse_file_path, overwrite=True)
        print("Arquivo movido para o Lakehouse com sucesso.")

    except requests.exceptions.RequestException as e:
        print(f"Falha ao baixar {url}: {e}")

    # Pequena pausa para não sobrecarregar o servidor
    time.sleep(1)

print("\nProcesso de download concluído.")


# ## Seção 2: Extração e Processamento dos Dados


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import subprocess
import shutil
from notebookutils import mssparkutils

# --- ETAPA 1: CONFIGURAR AMBIENTE DE EXECUÇÃO LOCAL ---

# Diretórios no Lakehouse
LAKEHOUSE_PROCESSOR_DIR = "Files/RFB_Processor"
LAKEHOUSE_DOWNLOAD_DIR = "Files/RFB_Downloads"
LAKEHOUSE_OUTPUT_DIR = "Files/RFB_Output"

# Diretórios locais temporários no nó do Spark
LOCAL_ROOT = "/tmp/rfb_processing"
LOCAL_PROCESSOR_DIR = os.path.join(LOCAL_ROOT, "RFB_Processor")
LOCAL_INPUT_DIR = os.path.join(LOCAL_ROOT, "input")
LOCAL_OUTPUT_DIR = os.path.join(LOCAL_ROOT, "output")

# Limpa o diretório local de execuções anteriores e recria a estrutura
if os.path.exists(LOCAL_ROOT):
    shutil.rmtree(LOCAL_ROOT)
os.makedirs(LOCAL_INPUT_DIR, exist_ok=True)
os.makedirs(LOCAL_OUTPUT_DIR, exist_ok=True)

print(f"Diretórios locais criados em: {LOCAL_ROOT}")


# --- ETAPA 2: COPIAR ARQUIVOS DO LAKEHOUSE PARA O AMBIENTE LOCAL ---

# Copia os scripts e arquivos de dependência (headers, etc.)
print(f"Copiando scripts de '{LAKEHOUSE_PROCESSOR_DIR}' para '{LOCAL_PROCESSOR_DIR}'...")
mssparkutils.fs.cp(LAKEHOUSE_PROCESSOR_DIR, f"file://{LOCAL_PROCESSOR_DIR}", recurse=True)

# Copia os arquivos .zip baixados
print(f"Copiando arquivos .zip de '{LAKEHOUSE_DOWNLOAD_DIR}' para '{LOCAL_INPUT_DIR}'...")
zip_files_source = [f.path for f in mssparkutils.fs.ls(LAKEHOUSE_DOWNLOAD_DIR) if f.path.endswith('.zip')]
for f_path in zip_files_source:
    mssparkutils.fs.cp(f_path, f"file://{LOCAL_INPUT_DIR}/")

print("Cópia de arquivos para o ambiente local concluída.")


# --- ETAPA 3: EXECUTAR O SCRIPT DE PROCESSAMENTO ---

print("\n--- Instalando dependências do script ---")
requirements_path = os.path.join(LOCAL_PROCESSOR_DIR, "requirements.txt")
install_command = ["pip", "install", "-r", requirements_path]

print(f"Executando comando: {install_command}")
install_result = subprocess.run(
    install_command,
    shell=False,
    capture_output=True,
    text=True,
    timeout=300  # Timeout de 5 minutos para a instalação
)

if install_result.returncode == 0:
    print("Dependências instaladas com sucesso!")
    # print("STDOUT:", install_result.stdout) # Opcional: pode ser muito verboso
else:
    print("ERRO ao instalar dependências:")
    print("RETURN CODE:", install_result.returncode)
    print("STDOUT:", install_result.stdout)
    print("STDERR:", install_result.stderr)
    # Lançar uma exceção para parar a execução do notebook se as dependências falharem
    raise Exception("Falha ao instalar as dependências do script. Verifique o log de erro acima.")

print("\n--- Executando o script de extração ---")
script_path = os.path.join(LOCAL_PROCESSOR_DIR, "extract_dump.py")
local_zip_files = [os.path.join(LOCAL_INPUT_DIR, os.path.basename(f)) for f in zip_files_source]
# O script espera ser executado do diretório que contém a pasta 'data', então mudamos o CWD
command = ["python", os.path.basename(script_path), LOCAL_OUTPUT_DIR] + local_zip_files

print(f"Comando a ser executado em '{LOCAL_PROCESSOR_DIR}':")
print(command)

try:
    # Executa o script. O timeout é longo para permitir o processamento.
    result = subprocess.run(
        command,
        shell=False,
        capture_output=True,
        text=True,
        timeout=3600,  # Timeout de 1 hora
        cwd=LOCAL_PROCESSOR_DIR  # Define o diretório de trabalho
    )

    if result.returncode == 0:
        print("Script executado com sucesso!")
        print("STDOUT:", result.stdout)
    else:
        print("Erro na execução do script:")
        print("RETURN CODE:", result.returncode)
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)

except subprocess.TimeoutExpired:
    print("Erro: O script demorou mais de 1 hora para executar e sofreu timeout.")
except Exception as e:
    print(f"Ocorreu um erro inesperado ao executar o subprocesso: {e}")


# --- ETAPA 4: MOVER OS RESULTADOS DE VOLTA PARA O LAKEHOUSE ---

print(f"Copiando arquivos processados de '{LOCAL_OUTPUT_DIR}' para '{LAKEHOUSE_OUTPUT_DIR}'...")
# O comando 'mv' do mssparkutils pode não suportar 'recurse=True'. Usamos 'cp' que é mais garantido.
# A limpeza do diretório de origem será feita na etapa 5.
mssparkutils.fs.cp(f"file://{LOCAL_OUTPUT_DIR}", LAKEHOUSE_OUTPUT_DIR, recurse=True)
print("Arquivos copiados para o Lakehouse com sucesso.")


# --- ETAPA 5: LIMPEZA DO AMBIENTE LOCAL ---

print(f"Limpando diretório local: {LOCAL_ROOT}")
shutil.rmtree(LOCAL_ROOT)
print("Limpeza concluída.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seção 3: Carga para a Tabela Delta

# CELL ********************

from pyspark.sql.functions import col
from pyspark.sql.types import StringType

# --- ETAPA 1: DEFINIR OS CAMINHOS E NOME DA TABELA ---

# Caminho onde os arquivos processados foram movidos no Lakehouse
LAKEHOUSE_OUTPUT_DIR = "/lakehouse/default/Files/RFB_Output"
# O script de processamento cria uma subpasta 'output' dentro do diretório de saída
processed_empresas_path = f"{LAKEHOUSE_OUTPUT_DIR}/output/empresa.csv.gz"

# Nome da tabela Delta de destino
target_table_name = "bronze_rfb_empresas_full"


# --- ETAPA 2: LER O CSV PROCESSADO E SALVAR COMO TABELA DELTA ---

print(f"Iniciando a leitura do arquivo processado: {processed_empresas_path}")

# A etapa anterior (extração) deve ter movido os arquivos CSV processados para o Lakehouse.
# Agora, vamos ler o arquivo de empresas e salvá-lo como uma tabela Delta.
df = spark.read.csv(processed_empresas_path,
                    header=True,
                    inferSchema=True,
                    sep=',',
                    quote='"',
                    escape='"')

print("Leitura do CSV concluída. Schema inferido:")
df.printSchema()

print(f"\nSalvando o DataFrame na tabela Delta '{target_table_name}'...")
df.write.mode("overwrite").format("delta").saveAsTable(target_table_name)
print("Tabela salva com sucesso!")

# Mostra uma amostra dos dados da nova tabela
print("Amostra da tabela criada:")
spark.table(target_table_name).show(5)

print("\nProcesso de carga para a camada Bronze concluído.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
