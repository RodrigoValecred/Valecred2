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
ontem = (datetime.now() - timedelta(days=1)).strftime('%d/%m/%Y')
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

# # STEP 4: Ingestão no Spark (Modo MultiLine para não quebrar o JSON)

# CELL ********************

rdd_arquivo = spark.sparkContext.parallelize([conteudo_csv])
df_raw = spark.read.option("header", "true") \
                    .option("sep", ";") \
                    .option("multiLine", "true") \
                    .option("quote", '"') \
                    .option("escape", '"') \
                    .csv(rdd_arquivo)
for col_name in df_raw.columns:
    # strip() tira espaços, replace tira o 'Enter' (\r)
    nome_limpo = col_name.replace('\r', '').strip()
    df_raw = df_raw.withColumnRenamed(col_name, nome_limpo)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(df_raw.columns)
display(df_raw.limit(5))

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

# MARKDOWN ********************

# # STEP 6: Transformação Silver (Agora sem erro de nome de coluna!)

# CELL ********************

path_cedente = "$.reports[0].advancedCommercialPaymentHistory.segmentData.assignor"
path_sacado = "$.reports[0].drawee"

# Agora 'Retorno' (sem espaço) vai funcionar perfeitamente
df_silver = df_bronze.withColumn(
    "Possui_Visao_Cedente", 
    when(get_json_object(col("Retorno"), path_cedente).isNotNull(), lit("Sim")).otherwise(lit("Não"))
).withColumn(
    "Pontualidade_Sacado", 
    get_json_object(col("Retorno"), f"{path_sacado}.paymentHistory.summary.total.percentageValueFrom").cast("float")
)

display(df_silver.select("CNPJ", "Possui_Visao_Cedente", "Pontualidade_Sacado").limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_filtrado = df_final.filter(col("Retorno").isNotNull())
display(df_filtrado.limit(5))

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

# CELL ********************

from pyspark.sql.functions import col, get_json_object, lit, current_timestamp

# 1. Seleciona o conteúdo da coluna de Retorno e extrai o JSON
# Substitua "Retorno" pelo nome exato da coluna que apareceu na sua tabela (pode ser "Retorno" ou "Dados_Serasa")
coluna_json = "Retorno" # Altere se o nome da coluna no seu DataFrame for diferente

df_analise = df_final.withColumn(
    "conteudo_json",
    col(coluna_json)
)

# 2. Vamos verificar as chaves que existem dentro do JSON
# Se a sua coluna de retorno é uma string JSON, podemos usar o display para inspecionar
display(df_analise.select("CNPJ", "conteudo_json").limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import from_json, col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# 1. Lendo os dados que você já salvou na camada Bronze
df_bronze = spark.read.table("LH_Bronze.vadu_serasa")

# 2. Schema para ler o conteúdo da coluna Retorno
schema_retorno = StructType([
    StructField("reports", StringType(), True) # Estrutura genérica para inspecionar e extrair
])

# 3. Extraindo o conteúdo da Visão Cedente
# O caminho exato no JSON é facts, mas a visão cedente fica em segmentData.assignor
df_silver = df_bronze.withColumn(
    "Visao_Cedente_Dados",
    col("Retorno ") # Aqui está o JSON que vimos na imagem
)

# 4. Criando colunas de interesse da visão cedente
df_silver_analise = df_silver.select(
    "CNPJ",
    "EmitidoEm",
    "arquivo_origem",
    "data_carga"
)

display(df_silver_analise.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, get_json_object, when, lit

# 1. Ajustando o caminho: $.reports[0] entra no primeiro relatório da lista
# Procuramos pela chave 'assignor' dentro de 'segmentData'
df_verificacao = df_bronze.withColumn(
    "Dados_Cedente", 
    get_json_object(col("Retorno"), "$.reports[0].segmentData.assignor")
)

# 2. Criando a coluna de Check (Tem ou Não Tem)
df_final_cedente = df_verificacao.withColumn(
    "Possui_Visao_Cedente", 
    when(col("Dados_Cedente").isNotNull(), lit("Sim")).otherwise(lit("Não"))
)

# 3. Exibindo o resultado
display(df_final_cedente.select("CNPJ", "Possui_Visao_Cedente", "Dados_Cedente").limit(20))

df_apenas_sim = df_final_cedente.filter(col("CNPJ") == "1201041000109")
display(df_apenas_sim)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import get_json_object, col

df_debug = df_bronze.select(
    "CNPJ",
    # Camada 1: Existe a lista reports?
    get_json_object(col("Retorno"), "$.reports").alias("check_reports"),
    
    # Camada 2: O que tem no primeiro item da lista?
    get_json_object(col("Retorno"), "$.reports[0]").alias("check_item_zero"),
    
    # Camada 3: Existe a pasta segmentData lá dentro?
    get_json_object(col("Retorno"), "$.reports[0].segmentData").alias("check_segment")
)

display(df_debug.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Trocamos assignor por segmentDescription para testar a rota
df_verificacao = df_bronze.withColumn(
    "Descricao_Segmento", 
    get_json_object(col("Retorno"), "$.reports[0].segmentData.segmentDescription")
)

# Agora filtramos quem NÃO é nulo na descrição
display(df_verificacao.filter(col("Descricao_Segmento").isNotNull()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import get_json_object, col

# Vamos olhar apenas os primeiros 200 caracteres do item zero 
# para ver quais são os nomes das pastas (chaves) reais.
df_inspecao = df_bronze.select(
    "CNPJ",
    get_json_object(col("Retorno"), "$.reports[0]").alias("conteudo_bruto")
)

# Mostra o texto puro. Procure por "segment" no texto que vai aparecer.
display(df_inspecao.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, get_json_object, when, lit

# A NOVA ROTA: Adicionamos o 'advancedCommercialPaymentHistory' no meio
rota_cedente = "$.reports[0].advancedCommercialPaymentHistory.segmentData.assignor"

df_verificacao = df_bronze.withColumn(
    "Dados_Cedente", 
    get_json_object(col("Retorno"), rota_cedente)
)

# Criando o Check (Tem ou Não Tem)
df_final_cedente = df_verificacao.withColumn(
    "Possui_Visao_Cedente", 
    when(col("Dados_Cedente").isNotNull(), lit("Sim")).otherwise(lit("Não"))
)

# Filtramos para ver se agora aparecem os "Sim"
display(df_final_cedente.select("CNPJ", "Possui_Visao_Cedente").filter(col("Possui_Visao_Cedente") == "Sim"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import get_json_object, col

# Remove duplicados baseados no CNPJ, mantendo apenas o registro mais recente
df_limpo = df_final_cedente.dropDuplicates(["CNPJ"])

print(f"Total original: {df_final_cedente.count()}")
print(f"Total após limpeza: {df_limpo.count()}")

# Definindo os caminhos para o "Cofre do Sacado" (Drawee)
path_sacado = "$.reports[0].drawee"

df_potencial = df_limpo.withColumn(
    "Pontualidade_Sacado", 
    get_json_object(col("Retorno"), f"{path_sacado}.paymentHistory.summary.punctual.percentageValueFrom")
).withColumn(
    "Comprometimento_Total", 
    get_json_object(col("Retorno"), f"{path_sacado}.evolutionCommitmentsSuppliers.summary.total.upcomingValueFrom")
).withColumn(
    "Maior_Fatura_Historica", 
    get_json_object(col("Retorno"), f"{path_sacado}.businessReferences.businessReferencesList[1].potentialValueFrom")
)

# Selecionando o que importa para a tomada de decisão
df_silver_decisao = df_potencial.select(
    "CNPJ",
    "Possui_Visao_Cedente",
    "Pontualidade_Sacado",
    "Comprometimento_Total",
    "Maior_Fatura_Historica"
)

display(df_silver_decisao.filter(col("Pontualidade_Sacado").isNotNull()))

from pyspark.sql.functions import get_json_object, col

# Definindo os caminhos para o "Cofre do Sacado" (Drawee)
path_sacado = "$.reports[0].drawee"

df_potencial = df_limpo.withColumn(
    "Pontualidade_Sacado", 
    get_json_object(col("Retorno"), f"{path_sacado}.paymentHistory.summary.punctual.percentageValueFrom")
).withColumn(
    "Comprometimento_Total", 
    get_json_object(col("Retorno"), f"{path_sacado}.evolutionCommitmentsSuppliers.summary.total.upcomingValueFrom")
).withColumn(
    "Maior_Fatura_Historica", 
    get_json_object(col("Retorno"), f"{path_sacado}.businessReferences.businessReferencesList[1].potentialValueFrom")
)

# Selecionando o que importa para a tomada de decisão
df_silver_decisao = df_potencial.select(
    "CNPJ",
    "Possui_Visao_Cedente",
    "Pontualidade_Sacado",
    "Comprometimento_Total",
    "Maior_Fatura_Historica"
)

display(df_silver_decisao.filter(col("Pontualidade_Sacado").isNotNull()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import get_json_object, col

# Vamos testar 3 rotas possíveis baseadas no seu JSON
df_debug_sacado = df_limpo.select(
    "CNPJ",
    # Rota A: Direto no relatório
    get_json_object(col("Retorno"), "$.reports[0].drawee").alias("Rota_A"),
    
    # Rota B: Dentro do histórico avançado (algumas versões do Vadu fazem isso)
    get_json_object(col("Retorno"), "$.reports[0].advancedCommercialPaymentHistory.drawee").alias("Rota_B"),
    
    # Rota C: Verificando se a pasta de histórico de pagamentos do sacado existe
    get_json_object(col("Retorno"), "$.reports[0].drawee.paymentHistory").alias("Rota_C")
)

display(df_debug_sacado.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, get_json_object, when, lit

# Rota base para o Sacado
path_drawee = "$.reports[0].drawee"

df_silver_potencial = df_limpo.withColumn(
    "Pontualidade_Sacado", 
    # No seu JSON, o caminho parece ser: drawee -> paymentHistory -> summary -> punctual -> percentageValueFrom
    get_json_object(col("Retorno"), f"{path_drawee}.paymentHistory.summary.punctual.percentageValueFrom")
).withColumn(
    "Faturamento_Estimado", 
    # Buscando o valor total de compras (businessReferences)
    get_json_object(col("Retorno"), f"{path_drawee}.businessReferences.summary.total.totalValueFrom")
)

# Criando um indicador visual
df_decisao = df_silver_potencial.withColumn(
    "Analise_Rapida",
    when(col("Pontualidade_Sacado").cast("float") > 90, lit("✅ BOM PAGADOR"))
    .otherwise(lit("⚠️ ATENÇÃO / SEM DADOS"))
)

# EXIBIÇÃO SEM FILTRO (Para você ver os NULLs e entender o que está acontecendo)
display(df_decisao.select("CNPJ", "Possui_Visao_Cedente", "Pontualidade_Sacado", "Analise_Rapida").limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json
import io
import zipfile
from pyspark.sql.functions import current_timestamp, lit, col

# 1. Configurações
api_key = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJWYWR1IiwidXNyIjoyNTY3NCwiZW1sIjoiaW50ZWdyYWNhby52YWR1QGRpbWVuc2EuY29tLmJyIiwiZW1wIjo1MzIzNTg0NX0.reuyRRQXsIA2UtGRpt7j1BHiRYiEvWfAibv3w2tkvr4"
url_auth = "https://www.vadu.com.br/vadurc.dll/Autenticacao/JSONPegarToken"
url_download = "https://www.vadu.com.br/vaduintegracao.dll/ServicoGrupoMonitoramento/DownloadZipConsultaSerasaJson?Desde=23/04/2026"

try:
    # --- PASSO A: BUSCAR O DADO NOVO ---
    print("Iniciando resgate de dados...")
    headers_auth = {"Authorization": f"Bearer {api_key}"}
    auth_res = requests.get(url_auth, headers=headers_auth)
    auth_res.raise_for_status()
    temp_token = auth_res.json().get("token")

    headers_download = {"Authorization": f"Bearer {temp_token}"}
    response = requests.get(url_download, headers=headers_download)
    response.raise_for_status()
    print(f"✓ Download concluído: {len(response.content)} bytes.")

    # --- PASSO B: EXTRAIR E LIMPAR O TEXTO ---
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        nome_arquivo = z.namelist()[0]
        with z.open(nome_arquivo) as f:
            conteudo_csv = f.read().decode('iso-8859-1')

    # --- PASSO C: INGESTÃO NO SPARK (O TANQUE DE GUERRA) ---
    rdd_arquivo = spark.sparkContext.parallelize([conteudo_csv])
    df_raw = spark.read.option("header", "true") \
                       .option("sep", ";") \
                       .option("multiLine", "true") \
                       .option("quote", '"') \
                       .option("escape", '"') \
                       .csv(rdd_arquivo)

    # Limpeza de cabeçalho imediata (tirando o \r e espaços)
    for col_name in df_raw.columns:
        nome_limpo = col_name.replace('\r', '').replace('\n', '').strip()
        df_raw = df_raw.withColumnRenamed(col_name, nome_limpo)

    # --- PASSO D: SALVAR NA BRONZE ---
    df_bronze_final = df_raw.dropDuplicates(["CNPJ"]) \
                            .withColumn("data_carga", current_timestamp()) \
                            .withColumn("arquivo_origem", lit(nome_arquivo))
    
    # IMPORTANTE: overwriteSchema=True garante que a tabela aceite as colunas limpas
    df_bronze_final.write.format("delta") \
                         .mode("overwrite") \
                         .option("overwriteSchema", "true") \
                         .saveAsTable("LH_Bronze.vadu_serasa")
    
    print(f"✓ SUCESSO! Tabela Bronze restaurada com {df_bronze_final.count()} linhas.")
    display(df_bronze_final.limit(5))

except Exception as e:
    print(f"❌ Ocorreu um erro no processo: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, get_json_object, when, lit, current_timestamp

# 1. Carregando a base Bronze original
df_original = spark.read.table("LH_Bronze.vadu_serasa")
print(f"Total de registros na Bronze: {df_original.count()}")

# 2. LIMPEZA DE CABEÇALHO (Eliminando o \r e espaços de vez)
for col_name in df_original.columns:
    nome_limpo = col_name.replace('\r', '').replace('\n', '').strip()
    df_original = df_original.withColumnRenamed(col_name, nome_limpo)

# 3. VERIFICAÇÃO DE SAÚDE DO JSON
# Vamos checar se a coluna 'Retorno' tem conteúdo ou se está vindo vazia
check_vazio = df_original.filter(col("Retorno").isNotNull()).count()
print(f"Registros com conteúdo na coluna Retorno: {check_vazio}")

# 4. A ROTA DO TESOURO (Ajustada para o seu JSON exato)
rota_cedente = "$.reports[0].advancedCommercialPaymentHistory.segmentData.assignor"
rota_sacado = "$.reports[0].drawee.paymentHistory.summary.total.percentageValueFrom"

# 5. EXTRAÇÃO SEM FILTROS (Para não esconder nada se der NULL)
df_final = df_original.withColumn(
    "Visao_Cedente", 
    when(get_json_object(col("Retorno"), rota_cedente).isNotNull(), lit("Sim")).otherwise(lit("Não"))
).withColumn(
    "Pontualidade_Sacado", 
    get_json_object(col("Retorno"), rota_pontualidade)
)

# 6. SALVAMENTO GARANTIDO
# Usamos 'append' para somar ou 'overwrite' para resetar a tabela com os nomes limpos
df_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Bronze.vadu_serasa")
print("✓ Tabela LH_Bronze.vadu_serasa regravada com sucesso!")

# 7. EXIBIÇÃO DE RESULTADOS
print("--- PRIMEIRAS LINHAS DA TABELA SALVA ---")
display(spark.read.table("LH_Bronze.vadu_serasa").select("CNPJ", "Visao_Cedente", "Pontualidade_Sacado").limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import io
import zipfile

# 1. Busca o dado (mesmo processo)
headers_download = {"Authorization": f"Bearer {temp_token}"}
response = requests.get(url_download, headers=headers_download)

with zipfile.ZipFile(io.BytesIO(response.content)) as z:
    nome_arquivo = z.namelist()[0]
    with z.open(nome_arquivo) as f:
        conteudo_bruto = f.read().decode('iso-8859-1')

# --- O DIAGNÓSTICO ---
print(f"--- NOME DO ARQUIVO DENTRO DO ZIP: {nome_arquivo} ---")
print("--- PRIMEIROS 500 CARACTERES DO ARQUIVO ---")
print(conteudo_bruto[:500]) 

# Vamos testar a leitura manual das linhas no Spark
linhas = conteudo_bruto.splitlines()
print(f"\nNúmero de linhas detectadas pelo Python: {len(linhas)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, get_json_object, when, lit

# 1. Criando o DataFrame a partir das linhas que o Python já validou
rdd_linhas = spark.sparkContext.parallelize(linhas)

# 2. Lendo o CSV (Note que não usamos multiLine aqui, pois o Python já quebrou as linhas)
df_resgate = spark.read.option("header", "true") \
                       .option("sep", ";") \
                       .csv(rdd_linhas)

# 3. Limpeza TOTAL dos nomes das colunas (Para matar o \r de vez)
for col_name in df_resgate.columns:
    nome_limpo = col_name.replace('\r', '').replace('\n', '').strip()
    df_resgate = df_resgate.withColumnRenamed(col_name, nome_limpo)

print(f"Colunas identificadas: {df_resgate.columns}")

# 4. Extração da Visão Cedente (Usando a rota que você já viu funcionar)
path_cedente = "$.reports[0].advancedCommercialPaymentHistory.segmentData.assignor"

df_vitoria = df_resgate.withColumn(
    "Visao_Cedente", 
    when(get_json_object(col("Retorno"), path_cedente).isNotNull(), lit("Sim")).otherwise(lit("Não"))
)

# 5. Salvando e Mostrando
df_vitoria.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Bronze.vadu_serasa")

print(f"✓ SUCESSO! {df_vitoria.count()} linhas processadas.")
display(df_vitoria.select("CNPJ", "Visao_Cedente").limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from pyspark.sql.functions import col, get_json_object, when, lit

# 1. Criando o DataFrame a partir das linhas que o Python já validou
rdd_linhas = spark.sparkContext.parallelize(linhas)

# 2. Lendo o CSV (Note que não usamos multiLine aqui, pois o Python já quebrou as linhas)
df_resgate = spark.read.option("header", "true") \
                       .option("sep", ";") \
                       .csv(rdd_linhas)

# 3. Limpeza TOTAL dos nomes das colunas (Para matar o \r de vez)
for col_name in df_resgate.columns:
    nome_limpo = col_name.replace('\r', '').replace('\n', '').strip()
    df_resgate = df_resgate.withColumnRenamed(col_name, nome_limpo)

print(f"Colunas identificadas: {df_resgate.columns}")

# 4. Extração da Visão Cedente (Usando a rota que você já viu funcionar)
path_cedente = "$.reports[0].advancedCommercialPaymentHistory.segmentData.assignor"

df_vitoria = df_resgate.withColumn(
    "Visao_Cedente", 
    when(get_json_object(col("Retorno"), path_cedente).isNotNull(), lit("Sim")).otherwise(lit("Não"))
)

# 5. Salvando e Mostrando
df_vitoria.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("LH_Bronze.vadu_serasa")

print(f"✓ SUCESSO! {df_vitoria.count()} linhas processadas.")
display(df_vitoria.select("CNPJ", "Visao_Cedente").limit(20))

# 1. Pegamos o JSON bruto de apenas uma linha que tenha dados
json_texto = df_vitoria.filter(col("Retorno").isNotNull()).limit(1).select("Retorno").collect()[0][0]
data = json.loads(json_texto)

relatorio = data['reports'][0]

pasta_para_olhar = 'advancedComeercialPaymentHistory'

if pasta_para_olhar in relatorio:
    conteudo_interno = relatorio[pasta_para_olhar]
    if isinstance(conteudo_interno, dict):
        print(f"--- PASTAS DENTRO DE '{pasta_para_olhar}' ---")
        for sub_chave in conteudo_interno.keys():
            print(f"-> {sub_chave}")
    else:
        print(f"'{pasta_para_olhar}' não é uma pasta, é um valor: {conteudo_interno}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
