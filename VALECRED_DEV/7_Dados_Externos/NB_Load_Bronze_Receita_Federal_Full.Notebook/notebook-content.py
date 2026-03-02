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

# # Notebook de Carga de Dados da Receita Federal (Nativo)
# **Objetivo:** Baixar e carregar dados públicos de CNPJ (Empresas e Estabelecimentos) da Receita Federal para a camada Bronze.
# **Fonte:** [Dados Abertos CNPJ](https://dadosabertos.rfb.gov.br/CNPJ/)
# **Processo:**
# 1.  Download dos arquivos ZIP (Empresas0-9, Estabelecimentos0-9).
# 2.  Extração dos arquivos CSV.
# 3.  Leitura com Spark (Schema manual).
# 4.  Salvamento em Delta (LH_Bronze).

# CELL ********************

import requests
import zipfile
import os
import shutil
import urllib3
from notebookutils import mssparkutils
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, to_date, regexp_replace

# --- Configurações ---
MIRRORS = [
    "https://dadosabertos.rfb.gov.br/CNPJ/",
    "http://200.152.38.155/CNPJ/", # IP direto caso DNS falhe
    "https://github.com/jonathands/dados-abertos-receita-cnpj/releases/download/2024.09/" # Mirror GitHub
]

# Diretórios no Lakehouse (Files API)
# Para operações de arquivo Python (os.path, open), usamos o caminho absoluto do OneLake.
# Para operações Spark e mssparkutils, usamos o caminho relativo "Files/...".

LAKEHOUSE_ROOT = "/lakehouse/default"
DOWNLOAD_DIR_REL = "Files/RFB_Downloads"
EXTRACT_DIR_REL = "Files/RFB_Extracted"

LAKEHOUSE_DOWNLOAD_DIR = f"{LAKEHOUSE_ROOT}/{DOWNLOAD_DIR_REL}"
LAKEHOUSE_EXTRACT_DIR = f"{LAKEHOUSE_ROOT}/{EXTRACT_DIR_REL}"

# Garantir diretórios
mssparkutils.fs.mkdirs(DOWNLOAD_DIR_REL)
mssparkutils.fs.mkdirs(EXTRACT_DIR_REL)

# Listas de arquivos para baixar
FILES_EMPRESAS = [f"Empresas{i}.zip" for i in range(10)]
FILES_ESTABELECIMENTOS = [f"Estabelecimentos{i}.zip" for i in range(10)]

# --- Schemas ---

schema_empresas = StructType([
    StructField("cnpj_basico", StringType(), True),
    StructField("razao_social", StringType(), True),
    StructField("natureza_juridica", StringType(), True),
    StructField("qualificacao_responsavel", StringType(), True),
    StructField("capital_social", StringType(), True), # Vem como string com vírgula (ex: "1000,00")
    StructField("porte_empresa", StringType(), True),
    StructField("ente_federativo_responsavel", StringType(), True)
])

schema_estabelecimentos = StructType([
    StructField("cnpj_basico", StringType(), True),
    StructField("cnpj_ordem", StringType(), True),
    StructField("cnpj_dv", StringType(), True),
    StructField("identificador_matriz_filial", StringType(), True),
    StructField("nome_fantasia", StringType(), True),
    StructField("situacao_cadastral", StringType(), True),
    StructField("data_situacao_cadastral", StringType(), True),
    StructField("motivo_situacao_cadastral", StringType(), True),
    StructField("nome_cidade_exterior", StringType(), True),
    StructField("pais", StringType(), True),
    StructField("data_inicio_atividade", StringType(), True),
    StructField("cnae_fiscal_principal", StringType(), True),
    StructField("cnae_fiscal_secundaria", StringType(), True),
    StructField("tipo_logradouro", StringType(), True),
    StructField("logradouro", StringType(), True),
    StructField("numero", StringType(), True),
    StructField("complemento", StringType(), True),
    StructField("bairro", StringType(), True),
    StructField("cep", StringType(), True),
    StructField("uf", StringType(), True),
    StructField("municipio", StringType(), True),
    StructField("ddd_1", StringType(), True),
    StructField("telefone_1", StringType(), True),
    StructField("ddd_2", StringType(), True),
    StructField("telefone_2", StringType(), True),
    StructField("ddd_fax", StringType(), True),
    StructField("fax", StringType(), True),
    StructField("correio_eletronico", StringType(), True),
    StructField("situacao_especial", StringType(), True),
    StructField("data_situacao_especial", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def safe_extract(zip_ref, path):
    """
    Extrai arquivos de forma segura, prevenindo path traversal (Zip Slip).
    """
    target_path = os.path.abspath(path)
    safe_members = []

    for member in zip_ref.namelist():
        # Resolver o caminho completo do membro
        # Nota: os.path.join descartará 'target_path' se 'member' for absoluto
        member_path = os.path.join(target_path, member)
        # Normalizar o caminho do membro para resolver '..' e '.'
        abs_member_path = os.path.abspath(member_path)

        # Verificar se o caminho do membro começa com o caminho de destino
        # Adicionamos os.sep para garantir que correspondemos aos limites do diretório (ex: /tmp/foo vs /tmp/foobar)
        if not abs_member_path.startswith(os.path.join(target_path, '')) and not abs_member_path == target_path:
             raise Exception(f"Zip Slip vulnerability detected: {member}")

        safe_members.append(member)

    zip_ref.extractall(path, members=safe_members)

def safe_extract(zip_ref, path):
    """
    Extracts a zip file to the specified path, preventing Zip Slip vulnerability.
    """
    # Normalize the target path to an absolute path
    target_path = os.path.abspath(path)
    safe_members = []

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

        safe_members.append(member)

    zip_ref.extractall(path, members=safe_members)

def download_and_extract(filename, base_dir_download, base_dir_extract):
    """
    Baixa um arquivo ZIP e extrai seu conteúdo.
    Tenta baixar de múltiplos mirrors em caso de falha.
    Realiza verificação de integridade do ZIP imediatamente após download.
    """
    local_zip_path = os.path.join(base_dir_download, filename)
    success = False

    # Headers para evitar bloqueio (alguns servidores exigem User-Agent)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    for base_url in MIRRORS:
        url = f"{base_url}{filename}"
        print(f"Tentando baixar de: {url}")

        try:
            # HEAD request para verificar disponibilidade
            try:
                head_response = requests.head(url, headers=headers, verify=True, timeout=30, allow_redirects=True)
                if head_response.status_code != 200:
                    print(f"Arquivo não encontrado ou erro no servidor (HEAD): {url} - Status: {head_response.status_code}")
                    continue
            except Exception as e:
                print(f"Erro no HEAD request para {url}: {e}. Tentando GET direto...")

            # Download
            response = requests.get(url, headers=headers, verify=True, stream=True, timeout=120)

            if response.status_code == 200:
                with open(local_zip_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"Download concluído: {local_zip_path}")

                # Verificação de integridade e Extração imediata
                try:
                    print(f"Verificando integridade e extraindo {filename}...")
                    with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
                        if zip_ref.testzip() is not None:
                            raise zipfile.BadZipFile("Teste de integridade falhou (CRC check)")
                        # Usar extração segura para prevenir Zip Slip
                        safe_extract(zip_ref, base_dir_extract)

                    print(f"Sucesso: {filename} baixado e extraído corretamente.")
                    success = True
                    break # Sair do loop de mirrors pois funcionou

                except zipfile.BadZipFile as e:
                    print(f"ERRO: Arquivo corrompido baixado de {url}. Erro: {e}")
                    # Remove arquivo corrompido e tenta próximo mirror
                    if os.path.exists(local_zip_path):
                        os.remove(local_zip_path)
                    continue
                except Exception as e:
                    print(f"ERRO: Falha na extração de {filename}. Erro: {e}")
                    # Falha de extração (não necessariamente download) - tentar próximo mirror por segurança
                    if os.path.exists(local_zip_path):
                        os.remove(local_zip_path)
                    continue

            else:
                print(f"Falha ao baixar de {url}. Status Code: {response.status_code}")

        except requests.exceptions.RequestException as e:
            print(f"Erro de conexão ao baixar de {url}: {e}")
        except Exception as e:
            print(f"Erro inesperado ao baixar de {url}: {e}")

    if not success:
        print(f"FALHA FATAL: Não foi possível baixar e extrair {filename} de nenhum mirror.")
        return False

    return Tru

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Execução Download e Extração ---

# Empresas
print("Processando EMPRESAS...")
for file in FILES_EMPRESAS:
    download_and_extract(file, LAKEHOUSE_DOWNLOAD_DIR, LAKEHOUSE_EXTRACT_DIR)

# Estabelecimentos
print("Processando ESTABELECIMENTOS...")
for file in FILES_ESTABELECIMENTOS:
    download_and_extract(file, LAKEHOUSE_DOWNLOAD_DIR, LAKEHOUSE_EXTRACT_DIR)

# Verificação de Arquivos Extraídos
print("\n--- Verificando arquivos extraídos ---")
try:
    files_extracted = mssparkutils.fs.ls(EXTRACT_DIR_REL)
    print(f"Total de arquivos em {EXTRACT_DIR_REL}: {len(files_extracted)}")
    # Listar primeiros 5 para debug
    for f in files_extracted[:5]:
        print(f" - {f.name}")
except Exception as e:
    print(f"Erro ao listar arquivos extraídos: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Leitura e Carga para Bronze: EMPRESAS ---

print("Lendo dados de EMPRESAS...")

try:
    # Usar caminho relativo para leitura no Spark para evitar problemas com driver ABFS
    path_empresas = f"{EXTRACT_DIR_REL}/*.EMPRE*"

    # Verificar se existem arquivos antes de tentar ler
    if len([f for f in mssparkutils.fs.ls(EXTRACT_DIR_REL) if ".EMPRE" in f.name]) > 0:
        df_empresas = spark.read.format("csv") \
            .option("delimiter", ";") \
            .option("header", "false") \
            .option("encoding", "ISO-8859-1") \
            .option("quote", '"') \
            .schema(schema_empresas) \
            .load(path_empresas)

        # Tratamento básico: Converter capital social (1000,00 -> 1000.00)
        df_empresas = df_empresas.withColumn("capital_social", regexp_replace(col("capital_social"), ",", ".").cast(DoubleType()))

        print("Salvando EMPRESAS em LH_Bronze...")
        df_empresas.write.format("delta").mode("overwrite").saveAsTable("LH_Bronze.rfb_empresas_full")
        print("EMPRESAS salvas com sucesso.")
    else:
        print("AVISO: Nenhum arquivo de EMPRESAS encontrado para processar. Pulando etapa.")

except Exception as e:
    print(f"Erro ao processar EMPRESAS: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Leitura e Carga para Bronze: ESTABELECIMENTOS ---

print("Lendo dados de ESTABELECIMENTOS...")

try:
    # Usar caminho relativo para leitura no Spark
    path_estabelecimentos = f"{EXTRACT_DIR_REL}/*.ESTABELE*"

    # Verificar se existem arquivos antes de tentar ler
    if len([f for f in mssparkutils.fs.ls(EXTRACT_DIR_REL) if ".ESTABELE" in f.name]) > 0:
        df_estab = spark.read.format("csv") \
            .option("delimiter", ";") \
            .option("header", "false") \
            .option("encoding", "ISO-8859-1") \
            .option("quote", '"') \
            .schema(schema_estabelecimentos) \
            .load(path_estabelecimentos)

        # Converter datas de string YYYYMMDD para DateType
        date_cols = ["data_situacao_cadastral", "data_inicio_atividade", "data_situacao_especial"]
        for c in date_cols:
            df_estab = df_estab.withColumn(c, to_date(col(c), "yyyyMMdd"))

        print("Salvando ESTABELECIMENTOS em LH_Bronze...")
        df_estab.write.format("delta").mode("overwrite").saveAsTable("LH_Bronze.rfb_estabelecimentos_full")
        print("ESTABELECIMENTOS salvos com sucesso.")
    else:
        print("AVISO: Nenhum arquivo de ESTABELECIMENTOS encontrado para processar. Pulando etapa.")

except Exception as e:
    print(f"Erro ao processar ESTABELECIMENTOS: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
