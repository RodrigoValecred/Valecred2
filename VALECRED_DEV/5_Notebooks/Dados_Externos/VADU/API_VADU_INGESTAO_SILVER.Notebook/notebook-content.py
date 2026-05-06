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
# META         },
# META         {
# META           "id": "8f85c372-56ad-4f3f-acf9-3be2e9b99513"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Config

# CELL ********************

from pyspark.sql.functions import col, get_json_object, when, lit

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # STEP 1: Carga Bronze

# CELL ********************

df_bronze = spark.table("LH_Bronze.vadu_serasa")
display(df_bronze.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Step 2: Filtrar Retornos vazios

# CELL ********************

df_silver = df_bronze.filter(col("Retorno").isNotNull())
display(df_silver.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # STEP 3: Seleção de colunas

# CELL ********************

# Agora 'Retorno' (sem espaço) vai funcionar perfeitamente
df_silver = df_silver.withColumn(
    "Possui_Visao_Cedente", 
    when(get_json_object(col("Retorno"), "$.reports[0].advancedCommercialPaymentHistory.segmentData.assignor").isNotNull(), lit("Sim")).otherwise(lit("Não"))
)

display(df_silver.select("CNPJ", "Possui_Visao_Cedente").limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



# 1. Criando a coluna UF
# Rota: entra em reports[0], depois identificationReport, depois address, pega o state
rota_uf = "$.reports[0].identificationReport.address.state"

df_com_uf = df_vitoria.withColumn("UF", get_json_object(col("Retorno"), rota_uf))

# 2. Filtrando apenas SP e MG
df_filtro_brasil = df_com_uf.filter(col("UF").isin(["SP", "MG"]))

# 3. Verificando o resultado
print(f"Total encontrado em SP/MG: {df_filtro_brasil.count()}")
display(df_filtro_brasil.select("CNPJ", "UF", "Visao_Cedente").limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

# Pegamos o conteúdo de um CNPJ que você sabe que deveria ter dados
exemplo_json = df_vitoria.filter(col("Retorno").isNotNull()).limit(1).select("Retorno").collect()[0][0]

# O indent=4 organiza o texto com espaços, facilitando a leitura
print(json.dumps(json.loads(exemplo_json), indent=4))

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

df_analise = df_silver.withColumn(
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
