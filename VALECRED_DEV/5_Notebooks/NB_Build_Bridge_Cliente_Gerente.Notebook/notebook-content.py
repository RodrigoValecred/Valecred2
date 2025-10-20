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

# ## Célula de configuração e Imports
# ###### =================================================================
# ###### Notebook: Notebook_Build_Bridge_Cliente_Gerente
# ######
# ###### Objetivo: Criar a tabela ponte (bridge) definitiva que mapeia o 
# ######           relacionamento histórico entre Clientes e Gerentes (Brokers),
# ######           com datas de início e fim de vigência.
# ######
# ###### Resultado: Uma tabela chamada 'bridge_cliente_gerente' no LH_Silver.
# ###### =================================================================

# CELL ********************

from pyspark.sql.functions import col, when, lit, lag, date_sub, coalesce
from pyspark.sql.window import Window

# Configuração para lidar com datas antigas (boa prática)
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

print("Configurações e imports concluídos.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Célula 2: Leitura e União dos Dados Brutos

# CELL ********************

# Ler a tabela de histórico e a tabela de relacionamentos atuais da camada Bronze
df_historico = spark.read.table("LH_Bronze.rlc_brokers_clientes_historico")
df_atual = spark.read.table("LH_Bronze.rlc_brokers_clientes")

# Unir as duas tabelas em um único DataFrame. 
# O Spark lida com colunas que podem não existir em um dos DFs se os schemas forem diferentes.
# Se os schemas forem idênticos, a união é direta.
df_unificado = df_historico.unionByName(df_atual, allowMissingColumns=True)

print("Dados históricos e atuais unidos.")
df_unificado.show() # Opcional: para visualizar os dados unidos

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Célula 3: Limpeza e Preparação dos Dados


# CELL ********************

# Criar a coluna de data unificada, exatamente como na lógica M
# Usamos a função 'when' (equivalente ao IF) e 'coalesce' (retorna o primeiro não nulo)
# 'coalesce' é uma forma mais limpa de fazer o if/else para nulos.
# Usamos coalesce se houver nulos: coalesce(col("DATAINICIO"),col("DATAINCLUSAO")).cast("date")
df_preparado = df_unificado.withColumn(
    "DataInicioVigencia",
    coalesce(col("DATAINICIO"), col("DATAINCLUSAO")).cast("date") 
).select(
    col("CODCLIENTE").alias("ClienteID"),
    col("CODBROKER").alias("GerenteID"),
    "DataInicioVigencia"
).filter(
    col("ClienteID").isNotNull() & col("GerenteID").isNotNull() &
col("DataInicioVigencia").isNotNull()
).distinct() # Remove registros 100% idênticos

print("Dados limpos, colunas selecionadas e duplicatas exatas removidas.")
df_preparado.show() # Opcional: para visualizar os dados preparados

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Célula 4: A Mágica - Cálculo da Data Fim de Vigência

# CELL ********************

# Define a "janela" de operação: particione os dados por cliente e ordene pela data de início
windowSpec = Window.partitionBy("ClienteID").orderBy(col("DataInicioVigencia").asc())

# Usa a função 'lag' com deslocamento negativo para "olhar para a frente"
# Ela pega a DataInicioVigencia da PRÓXIMA linha dentro da janela de cada cliente.
# O valor padrão '9999-12-31' é usado para a última linha de cada cliente (o registro atual).
df_com_data_fim = df_preparado.withColumn(
    "DataFimVigencia_temp",
    lag("DataInicioVigencia",-1,"9999-12-31").over(windowSpec)
)

# Ajusta a data de fim para ser um dia antes da data de início do próximo relacionamento
df_final = df_com_data_fim.withColumn(
    "DataFimVigencia",
    when(
        col("DataFimVigencia_temp") == "9999-12-31",
        lit("9999-12-31").cast("date")
    ).otherwise(
        date_sub(col("DataFimVigencia_temp"), 1)
    )
).select("ClienteID", "GerenteID", "DataInicioVigencia", "DataFimVigencia")

print("Tabela ponte com datas de início e fim calculadas.")
df_final.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Célula 5: Salvar a Tabela Ponte Definitiva na Silver

# MARKDOWN ********************

# ## 

# CELL ********************

# Salvar o resultado final no Lakehouse Silver
output_path = "LH_Silver.bridge_cliente_gerente"

df_final.write.mode("overwrite").option("overwriteSchema",
"true").saveAsTable(output_path)

print(f"Tabela ponte '{output_path}' criada/atualizada com sucesso no Lakehouse Silver.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Célula 6: Criar Tabela com Relacionamento Atual
# ###### =================================================================
# ###### Objetivo: Criar uma tabela apenas com o relacionamento mais recente
# ######           (atual) de cada cliente com seu gerente.
# ###### Lógica:   Filtrar a tabela ponte onde a DataFimVigencia é a
# ######           data máxima ('9999-12-31'), indicando que o
# ######           relacionamento está ativo.
# ###### =================================================================

# CELL ********************

from pyspark.sql.functions import col

# Filtrar a tabela final para obter apenas os registros atuais
df_relacionamento_atual = df_final.filter(col("DataFimVigencia") == "9999-12-31") \
    .select("ClienteID", "GerenteID", "DataInicioVigencia")

print("Tabela com relacionamentos atuais criada.")
df_relacionamento_atual.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Célula 7: Salvar Tabela de Relacionamento Atual na Silver

# CELL ********************

# Salvar o resultado final no Lakehouse Silver
output_path_atual = "LH_Silver.relacionamento_cliente_gerente_atual"

df_relacionamento_atual.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_path_atual)

print(f"Tabela de relacionamento atual '{output_path_atual}' criada/atualizada com sucesso no Lakehouse Silver.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
