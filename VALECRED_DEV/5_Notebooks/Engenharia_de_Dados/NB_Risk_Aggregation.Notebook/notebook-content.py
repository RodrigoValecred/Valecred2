# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8f85c372-56ad-4f3f-acf9-3be2e9b99513",
# META       "default_lakehouse_name": "LH_Silver",
# META       "default_lakehouse_workspace_id": "41ae19db-f71d-471f-9ac7-ccbc2c75ce11",
# META       "known_lakehouses": [
# META         {
# META           "id": "8f85c372-56ad-4f3f-acf9-3be2e9b99513"
# META         },
# META         {
# META           "id": "30368149-1b39-46e6-8575-f8d757833a69"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MARKDOWN ********************

# ## 1. Carregamento e Preparação dos Dados
# Carregue os dataframes a partir da sua Lakehouse. O Fabric facilita isso com o próprio explorador de Lakehouse.

# CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
df_titulos = spark.read.table("LH_Silver.staging_titulos_limpa")
df_operacoes = spark.read.table("LH_Silver.staging_operacoes_limpa")
df_cedentes = spark.read.table("LH_Silver.dim_cliente")
df_cad_geral = spark.read.table("LH_Silver.staging_cad_geral_limpa")
from pyspark.sql.functions import col, when, sum, count, lit

# CELL ********************

# MARKDOWN ********************

# Renomeando colunas duplicadas para evitar conflitos nos joins.

# CELL ********************

df_operacoes = df_operacoes.withColumnRenamed("EXIGECANHOTO", "EXIGECANHOTO_OPERACAO")
df_operacoes = df_operacoes.withColumnRenamed("EXIGECONFIRMACAO", "EXIGECONFIRMACAO_OPERACAO")
df_operacoes = df_operacoes.withColumnRenamed("TTO", "TTO_OPERACAO")
df_operacoes = df_operacoes.withColumnRenamed("DATAINCLUSAO", "DATAINCLUSAO_OPERACAO")
df_operacoes = df_operacoes.withColumnRenamed("USUAINCLUSAO", "USUAINCLUSAO_OPERACAO")
df_operacoes = df_operacoes.withColumnRenamed("DATAALTERACAO", "DATAALTERACAO_OPERACAO")
df_operacoes = df_operacoes.withColumnRenamed("USUAALTERACAO", "USUAALTERACAO_OPERACAO")
df_operacoes = df_operacoes.withColumnRenamed("CODRATING", "CODRATING_OPERACAO")
df_operacoes = df_operacoes.withColumnRenamed("PREIMPRESSO", "PREIMPRESSO_OPERACAO")
df_operacoes = df_operacoes.withColumnRenamed("BOLETOESPECIAL", "BOLETOESPECIAL_OPERACAO")
df_operacoes = df_operacoes.withColumnRenamed("TARIFARECOMPRA", "TARIFARECOMPRA_OPERACAO")
df_operacoes = df_operacoes.withColumnRenamed("RECEBEBOLETO", "RECEBEBOLETO_OPERACAO")

df_cedentes = df_cedentes.withColumnRenamed("DATAINCLUSAO", "DATAINCLUSAO_CEDENTE")
df_cedentes = df_cedentes.withColumnRenamed("USUAINCLUSAO", "USUAINCLUSAO_CEDENTE")
df_cedentes = df_cedentes.withColumnRenamed("DATAALTERACAO", "DATAALTERACAO_CEDENTE")
df_cedentes = df_cedentes.withColumnRenamed("USUAALTERACAO", "USUAALTERACAO_CEDENTE")
df_cedentes = df_cedentes.withColumnRenamed("CODRATING", "CODRATING_CEDENTE")
df_cedentes = df_cedentes.withColumnRenamed("PEFIN", "PEFIN_CEDENTE")
df_cedentes = df_cedentes.withColumnRenamed("BAIXADOPEFIN", "BAIXADOPEFIN_CEDENTE")
df_cedentes = df_cedentes.withColumnRenamed("PREIMPRESSO", "PREIMPRESSO_CEDENTE")
df_cedentes = df_cedentes.withColumnRenamed("BOLETOESPECIAL", "BOLETOESPECIAL_CEDENTE")
df_cedentes = df_cedentes.withColumnRenamed("TARIFARECOMPRA", "TARIFARECOMPRA_CEDENTE")
df_cedentes = df_cedentes.withColumnRenamed("RECEBEBOLETO", "RECEBEBOLETO_CEDENTE")

df_mestra_spark = df_titulos.join(df_operacoes, on="CODOPERACAO", how="left")
df_mestra_spark = df_mestra_spark.join(df_cedentes, on="CODCLIENTE", how="left")
df_mestra_spark = df_mestra_spark.join(
    df_cad_geral.select("CPFCNPJ", "CIDADE", "UF").dropDuplicates(["CPFCNPJ"]),
    on="CPFCNPJ",
    how="left"
)

# CELL ********************

# MARKDOWN ********************

# Aplicando as regras de negócio para filtrar o universo de análise.

# CELL ********************

df_filtrado = df_mestra_spark.filter(
    (col('STATUSANALISE') == 'D') &
    (col('STATUSACEITE') == 'A') &
    (col('ACEITO') == 'S')
)

tipos_excluir = ['RN','RE','RC','PR','AB','AM','LB','PB']
df_filtrado = df_filtrado.filter(~col('TTO_OPERACAO').isin(tipos_excluir))

tdoc_excluir = ['BL']
df_filtrado = df_filtrado.filter(~col('TDOC').isin(tdoc_excluir))

df_filtrado = df_filtrado.filter(col('LIQUIDACAO').isNotNull())

# CELL ********************

# MARKDOWN ********************

# ### Criação da Variável Target
# A variável `TARGET` é criada com base na coluna `MOTIVO`. Se o motivo for 'PG' (pago), o título é considerado adimplente (0). Qualquer outro motivo indica inadimplência (1).

# CELL ********************

motivos_adimplentes = ['PG']
df_com_target = df_filtrado.withColumn(
    'TARGET',
    when(col('MOTIVO').isin(motivos_adimplentes), 0).otherwise(1)
)

# CELL ********************

# MARKDOWN ********************

# ## 2. Agregação de Risco por Cliente
# Agregando os dados para calcular as métricas de risco para cada cliente (CPFCNPJ).

# CELL ********************

df_risco_cliente = df_com_target.groupBy("CPFCNPJ").agg(
    count("*").alias("total_titulos"),
    sum("TARGET").alias("total_inadimplencia"),
    sum(col("VALOR")).alias("valor_total_transacionado"),
    sum(when(col("TARGET") == 1, col("VALOR")).otherwise(0)).alias("valor_total_inadimplencia")
)

df_risco_cliente = df_risco_cliente.withColumn(
    "taxa_inadimplencia",
    (col("total_inadimplencia") / col("total_titulos"))
)

# CELL ********************

# MARKDOWN ********************

# ## 3. Salvando a Tabela Agregada no Gold
# Salvando o resultado no Data Warehouse Gold para consumo por ferramentas de BI e outros sistemas.

# CELL ********************

# O nome do warehouse é 'WH_Gold'
# O nome da tabela será 'risco_por_cliente'
table_name = "WH_Gold.risco_por_cliente"

df_risco_cliente.write.mode("overwrite").format("delta").saveAsTable(table_name)

print(f"Tabela '{table_name}' salva com sucesso no Warehouse Gold.")

# CELL ********************

# MARKDOWN ********************

# ## 4. Verificação
# Lendo os dados da tabela recém-criada para garantir que a operação foi bem-sucedida.

# CELL ********************

print("Lendo os 10 primeiros registros da tabela criada:")
df_verificacao = spark.read.table(table_name)
df_verificacao.show(10)
