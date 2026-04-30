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

# CELL ********************

# import as funções que serão utilizadas
import requests
import json
from datetime import datetime
import io
import zipfile
from pyspark.sql.functions import current_timestamp, lit

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# coloque suas credenciais e o endereço que vai acessar
api_key = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJWYWR1IiwidXNyIjoyNTY3NCwiZW1sIjoiaW50ZWdyYWNhby52YWR1QGRpbWVuc2EuY29tLmJyIiwiZW1wIjo1MzIzNTg0NX0.reuyRRQXsIA2UtGRpt7j1BHiRYiEvWfAibv3w2tkvr4"
url_auth = "https://www.vadu.com.br/vadurc.dll/Autenticacao/JSONPegarToken"

#ajuste a data no final do comando
url_download = "https://www.vadu.com.br/vaduintegracao.dll/ServicoGrupoMonitoramento/DownloadZipConsultaSerasaJson?Desde=23/04/2026"

headers_auth = {"Authorization": f"Bearer {api_key}"}

#requisita um token temporario
auth_res = requests.get(url_auth, headers=headers_auth)
auth_res.raise_for_status()
temp_token = auth_res.json().get("token")
print("✓ Token temporário obtido.")

# Início do bloco de download
headers_download = {"Authorization": f"Bearer {temp_token}"}
response = requests.get(url_download, headers=headers_download)
response.raise_for_status()
print(f"✓ Download realizado: {len(response.content)} bytes recebidos.")

try:
    # Extração do CSV de dentro do ZIP
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        nome_arquivo = z.namelist()[0]
        with z.open(nome_arquivo) as f:
            # Lemos os bytes e decodificamos para texto
            # Usamos 'iso-8859-1' ou 'utf-8' (comum em sistemas brasileiros)
            conteudo_csv = f.read().decode('iso-8859-1') 

    # Transformação em DataFrame Spark
    # Quebramos o texto em linhas
    linhas = conteudo_csv.splitlines()
    
    # Criamos um RDD (o motor do Spark) para ler essas linhas
    rdd_linhas = spark.sparkContext.parallelize(linhas)
    
    # Lendo o CSV com o separador ';' que identificamos no log
    df = spark.read.option("header", "true") \
                   .option("sep", ";") \
                   .option("inferSchema", "true") \
                   .csv(rdd_linhas)

    # 6. Adição de Metadados e Carga na Bronze
    df_final = df.withColumn("data_carga", current_timestamp()) \
                 .withColumn("arquivo_origem", lit(nome_arquivo))
    
    # Salvando no seu Lakehouse
    df_final.write.format("delta").mode("append").saveAsTable("LH_Bronze.vadu_serasa")
    
    print(f"✓ Sucesso! {df_final.count()} linhas carregadas na LH_Bronze.vadu_serasa.")
    display(df_final.limit(5))

except Exception as e:
    print(f"Erro na fase de carga: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# 1. Definindo o "Mapa" (Schema) do JSON que está dentro da coluna Retorno
# Pelo que vimos, ele começa com {"reports": [...]}
json_schema = StructType([
    StructField("reports", ArrayType(
        StructType([
            StructField("reportName", StringType(), True),
            # Adicione aqui outros campos que você viu no JSON
        ])
    ), True)
])

# 2. Transformando a coluna Retorno (que é texto) em uma coluna de Dados Estruturados
df_silver = df_final.withColumn("Retorno_Estruturado", from_json(col("Retorno"), json_schema))

# 3. "Abrindo" os dados para colunas individuais
# O select permite que a gente pegue o que está lá dentro do mapa
df_silver_flat = df_silver.select(
    "CNPJ",
    "EmitidoEm",
    col("Retorno_Estruturado.reports")[0].alias("Dados_Serasa") # Pega o primeiro relatório da lista
)

display(df_silver_flat.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
