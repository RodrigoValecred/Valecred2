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
from notebookutils import mssparkutils
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, to_date, regexp_replace

# --- Configurações ---
BASE_URL = "https://dadosabertos.rfb.gov.br/CNPJ/"
FALLBACK_URL = "http://200.152.38.155/CNPJ/" # IP direto caso DNS falhe

# Diretórios no Lakehouse (Files API)
LAKEHOUSE_DOWNLOAD_DIR = "/lakehouse/default/Files/RFB_Downloads"
LAKEHOUSE_EXTRACT_DIR = "/lakehouse/default/Files/RFB_Extracted"

# Garantir diretórios
mssparkutils.fs.mkdirs("Files/RFB_Downloads")
mssparkutils.fs.mkdirs("Files/RFB_Extracted")

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

# CELL ********************

def download_and_extract(filename, base_dir_download, base_dir_extract):
    """
    Baixa um arquivo ZIP e extrai seu conteúdo.
    """
    url = f"{BASE_URL}{filename}"
    local_zip_path = os.path.join(base_dir_download, filename)

    # Check if already processed (could add more robust check)
    # For now, simplistic check if zip exists.
    # Em produção, ideal checar se extraído já existe.

    print(f"Iniciando download: {url}")
    try:
        response = requests.get(url, verify=False, stream=True, timeout=120)
        if response.status_code != 200:
            # Tentar fallback
            url = f"{FALLBACK_URL}{filename}"
            print(f"Tentando fallback: {url}")
            response = requests.get(url, verify=False, stream=True, timeout=120)

        if response.status_code == 200:
            with open(local_zip_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Download concluído: {local_zip_path}")

            # Extrair
            print(f"Extraindo {filename}...")
            with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
                zip_ref.extractall(base_dir_extract)
            print(f"Extração concluída em {base_dir_extract}")

            # Remove zip para economizar espaço? (Opcional)
            # os.remove(local_zip_path)

            return True
        else:
            print(f"Erro ao baixar {filename}: Status {response.status_code}")
            return False
    except Exception as e:
        print(f"Exceção no download de {filename}: {e}")
        return False

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

# CELL ********************

# --- Leitura e Carga para Bronze: EMPRESAS ---

# Listar arquivos CSV extraídos que correspondem a Empresas
# Os arquivos extraídos geralmente têm nomes como "K3241.K03200Y0.D50211.EMPRECSV" ou similar, mas terminam ou contêm indicação.
# Na verdade, o padrão do RFB ao extrair é manter o nome interno.
# Vamos assumir que todos os extraídos estão na pasta EXTRACT.
# Precisamos diferenciar quais são Empresas e quais são Estabelecimentos se eles não tiverem nomes claros.
# O padrão dos arquivos dentro do ZIP:
# Empresas -> *.EMPRECSV
# Estabelecimentos -> *.ESTABELE

# Vamos usar wildcard do Spark para ler todos que correspondem ao padrão.
# path_empresas = f"{LAKEHOUSE_EXTRACT_DIR}/*.EMPRECSV" # Ajustar conforme o nome real extraído
# Se o nome for aleatório, talvez tenhamos que inspecionar os arquivos.
# Pelo padrão atual (2024/2025), a extensão costuma ajudar.

# Vamos verificar o diretório para ver o padrão de nomes (se já houver arquivos)
# Como é a primeira execução, assumimos o padrão documentado ou comum "*.EMPRECSV".

print("Lendo dados de EMPRESAS...")

try:
    df_empresas = spark.read.format("csv") \
        .option("delimiter", ";") \
        .option("header", "false") \
        .option("encoding", "ISO-8859-1") \
        .option("quote", '"') \
        .schema(schema_empresas) \
        .load(f"{LAKEHOUSE_EXTRACT_DIR}/*.EMPRE*") # Tentativa de match genérico

    # Tratamento básico: Converter capital social (1000,00 -> 1000.00)
    df_empresas = df_empresas.withColumn("capital_social", regexp_replace(col("capital_social"), ",", ".").cast(DoubleType()))

    print("Salvando EMPRESAS em LH_Bronze...")
    df_empresas.write.format("delta").mode("overwrite").saveAsTable("LH_Bronze.rfb_empresas_full")
    print("EMPRESAS salvas com sucesso.")

except Exception as e:
    print(f"Erro ao processar EMPRESAS: {e}")

# CELL ********************

# --- Leitura e Carga para Bronze: ESTABELECIMENTOS ---

print("Lendo dados de ESTABELECIMENTOS...")

try:
    df_estab = spark.read.format("csv") \
        .option("delimiter", ";") \
        .option("header", "false") \
        .option("encoding", "ISO-8859-1") \
        .option("quote", '"') \
        .schema(schema_estabelecimentos) \
        .load(f"{LAKEHOUSE_EXTRACT_DIR}/*.ESTABELE*") # Tentativa de match genérico

    # Converter datas de string YYYYMMDD para DateType
    date_cols = ["data_situacao_cadastral", "data_inicio_atividade", "data_situacao_especial"]
    for c in date_cols:
        df_estab = df_estab.withColumn(c, to_date(col(c), "yyyyMMdd"))

    print("Salvando ESTABELECIMENTOS em LH_Bronze...")
    df_estab.write.format("delta").mode("overwrite").saveAsTable("LH_Bronze.rfb_estabelecimentos_full")
    print("ESTABELECIMENTOS salvos com sucesso.")

except Exception as e:
    print(f"Erro ao processar ESTABELECIMENTOS: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
