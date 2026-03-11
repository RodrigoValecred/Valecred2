# Fabric notebook source


# CELL ********************

# Fabric notebook source

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
    "https://dadosabertos.rfb.gov.br/CNPJ/"
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
        member_path = os.path.join(target_path, member)
        abs_member_path = os.path.abspath(member_path)

        if not abs_member_path.startswith(os.path.join(target_path, '')) and not abs_member_path == target_path:
             raise Exception(f"Zip Slip vulnerability detected: {member}")

        safe_members.append(member)

    try:
        zip_ref.extractall(path, members=safe_members)
    except Exception as e:
        print(f"Erro ao extrair {zip_ref.filename}: {e}")

def extract_file(filename, base_dir_download, base_dir_extract):
    local_zip_path = os.path.join(base_dir_download, filename)
    
    if not os.path.exists(local_zip_path):
        print(f"Arquivo não encontrado para extração: {local_zip_path}")
        return False
        
    try:
        print(f"Extraindo {filename}...")
        with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
            safe_extract(zip_ref, base_dir_extract)
        print(f"Sucesso: {filename} extraído corretamente.")
        return True
    except Exception as e:
        print(f"ERRO: Falha na extração de {filename}. Erro: {e}")
        return False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Execução Extração ---

# Empresas
print("Extraindo EMPRESAS...")
for file in FILES_EMPRESAS:
    extract_file(file, LAKEHOUSE_DOWNLOAD_DIR, LAKEHOUSE_EXTRACT_DIR)

# Estabelecimentos
print("Extraindo ESTABELECIMENTOS...")
for file in FILES_ESTABELECIMENTOS:
    extract_file(file, LAKEHOUSE_DOWNLOAD_DIR, LAKEHOUSE_EXTRACT_DIR)

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
        spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
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
