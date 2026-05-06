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

# # import as funções que serão utilizadas

# CELL ********************

import requests
import json
import io
import zipfile
from datetime import datetime, timedelta
from pyspark.sql.functions import current_timestamp, lit, col, get_json_object, when, replace

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # --- CONFIGURAÇÕES ---

# CELL ********************

# coloque suas credenciais e o endereço que vai acessar
api_key = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJWYWR1IiwidXNyIjoyNTY3NCwiZW1sIjoiaW50ZWdyYWNhby52YWR1QGRpbWVuc2EuY29tLmJyIiwiZW1wIjo1MzIzNTg0NX0.reuyRRQXsIA2UtGRpt7j1BHiRYiEvWfAibv3w2tkvr4"
url_auth = "https://www.vadu.com.br/vadurc.dll/Autenticacao/JSONPegarToken"
# Data dinâmica para pegar sempre os dados de ontem
ontem = (datetime.now() - timedelta(days=12)).strftime('%d/%m/%Y')
url_download = f"https://www.vadu.com.br/vaduintegracao.dll/ServicoGrupoMonitoramento/DownloadZipConsultaSerasaJson?Desde={ontem}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # STEP 1: Autenticação

# CELL ********************

headers_auth = {"Authorization": f"Bearer {api_key}"}
auth_res = requests.get(url_auth, headers=headers_auth)
auth_res.raise_for_status()
temp_token = auth_res.json().get("token")
print("✓ Token temporário obtido.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # STEP 2: Download do ZIP

# CELL ********************

headers_download = {"Authorization": f"Bearer {temp_token}"}
response = requests.get(url_download, headers=headers_download)
response.raise_for_status()
print(f"✓ Download realizado: {len(response.content)} bytes.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # STEP 3: Extração do ZIP e Leitura Binária

# CELL ********************

with zipfile.ZipFile(io.BytesIO(response.content)) as z:
    nome_arquivo = z.namelist()[0]
    with z.open(nome_arquivo) as f:
        conteudo_csv = f.read().decode('iso-8859-1') # Decodificação para sistemas BR

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # STEP 4: Ingestão no Spark

# CELL ********************

# 1. Transformação em DataFrame Spark (Voltando ao splitlines)
linhas = conteudo_csv.splitlines()
rdd_linhas = spark.sparkContext.parallelize(linhas)

# 2. Lendo o CSV de forma simples
df_raw = spark.read.option("header", "true") \
                   .option("sep", ";") \
                   .option("inferSchema", "true") \
                   .csv(rdd_linhas)

# 3. Limpando os nomes das colunas (Essencial para o Spark achar a 'Retorno')
for col_name in df_raw.columns:
    nome_limpo = col_name.replace('\r', '').strip()
    df_raw = df_raw.withColumnRenamed(col_name, nome_limpo)

# --- Agora vamos para a Extração que você viu dar certo ---

# A rota que trouxe o seu "Sim" (o corredor que leva ao Cedente)
rota_cedente = "$.reports[0].advancedCommercialPaymentHistory.segmentData.assignor"
rota_uf = "$.reports[0].identificationReport.address.state"

df_vitoria = df_raw.withColumn(
    "Visao_Cedente", 
    when(get_json_object(col("Retorno"), rota_cedente).isNotNull(), lit("Sim")).otherwise(lit("Não"))
).withColumn(
    "UF", get_json_object(col("Retorno"), rota_uf)
)

# 4. Mostrando o resultado
print(f"✓ Base recuperada com {df_vitoria.count()} linhas.")
display(df_vitoria.select("CNPJ", "Visao_Cedente", "UF").filter(col("Visao_Cedente")=="Sim").limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # STEP 5: Limpeza e Bronze (Dedup e Higienização de Nomes)

# CELL ********************

# Aqui removemos os espaços extras de todos os nomes de colunas automaticamente
for col_name in df_raw.columns:
    df_raw = df_raw.withColumnRenamed(col_name, col_name.strip())

df_bronze = df_raw.dropDuplicates(["CNPJ"]) \
                    .withColumn("data_carga", current_timestamp()) \
                    .withColumn("arquivo_origem", lit(nome_arquivo))

# Salva na Bronze
df_bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Bronze.vadu_serasa")
print(f"✓ Camada Bronze atualizada e colunas limpas!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(df_bronze.columns)
display(df_bronze.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
