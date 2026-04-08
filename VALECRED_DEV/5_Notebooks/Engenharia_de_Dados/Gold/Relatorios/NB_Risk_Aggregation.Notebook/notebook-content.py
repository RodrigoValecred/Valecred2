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

from pyspark.sql.functions import col, when, sum, count, lit

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")

# --- OTIMIZACAO (BOLT): Predicate Pushdown ---
# Filtramos as tabelas ANTES dos joins para reduzir o volume de dados trafegado (Shuffle).

# 1. Leitura e Filtro de Titulos
df_titulos = spark.read.table("LH_Silver.staging_titulos_limpa")

# Aplicando filtros de titulos imediatamente
tdoc_excluir = ['BL']
df_titulos = df_titulos.filter(~col('TDOC').isin(tdoc_excluir))
df_titulos = df_titulos.filter(col('LIQUIDACAO').isNotNull())

# 2. Leitura e Filtro de Operacoes
df_operacoes = spark.read.table("LH_Silver.staging_operacoes_limpa")

# Renomeando colunas (necessário antes do filtro se usar nome novo, mas aqui usamos colunas originais se possivel ou novas)
# O codigo original renomeava TTO -> TTO_OPERACAO e depois filtrava TTO_OPERACAO. Vamos manter o padrao.
# 🧠 Tensor: Optimize bulk column renaming
# 💡 What: Replaced iterative .withColumnRenamed() with a single vectorized df.toDF() projection.
# 🎯 Why: Iteratively calling .withColumnRenamed() creates deeply nested Project nodes in the Catalyst logical plan, leading to high compilation overhead and potential StackOverflowError.
# 📊 Impact: Reduce drastically the depth of the Catalyst query plan and compilation time.
# 🔬 Measurement: Plan compilation overhead for this segment drops from linear O(N) to O(1) in Catalyst.
cols_operacoes = {
    "EXIGECANHOTO": "EXIGECANHOTO_OPERACAO",
    "EXIGECONFIRMACAO": "EXIGECONFIRMACAO_OPERACAO",
    "TTO": "TTO_OPERACAO",
    "DATAINCLUSAO": "DATAINCLUSAO_OPERACAO",
    "USUAINCLUSAO": "USUAINCLUSAO_OPERACAO",
    "DATAALTERACAO": "DATAALTERACAO_OPERACAO",
    "USUAALTERACAO": "USUAALTERACAO_OPERACAO",
    "CODRATING": "CODRATING_OPERACAO",
    "PREIMPRESSO": "PREIMPRESSO_OPERACAO",
    "BOLETOESPECIAL": "BOLETOESPECIAL_OPERACAO",
    "TARIFARECOMPRA": "TARIFARECOMPRA_OPERACAO",
    "RECEBEBOLETO": "RECEBEBOLETO_OPERACAO"
}
new_cols_operacoes = [cols_operacoes.get(c, c) for c in df_operacoes.columns]
df_operacoes = df_operacoes.toDF(*new_cols_operacoes)

# Aplicando filtros de operacoes imediatamente
df_operacoes = df_operacoes.filter(
    (col('STATUSANALISE') == 'D') &
    (col('STATUSACEITE') == 'A') &
    (col('ACEITO') == 'S')
)
tipos_excluir = ['RN','RE','RC','PR','AB','AM','LB','PB']
df_operacoes = df_operacoes.filter(~col('TTO_OPERACAO').isin(tipos_excluir))


df_cedentes = spark.read.table("LH_Silver.dim_cliente")
df_cad_geral = spark.read.table("LH_Silver.staging_cad_geral_limpa")

# CELL ********************

# MARKDOWN ********************

# Renomeando colunas duplicadas para evitar conflitos nos joins.

# CELL ********************

# (Operacoes ja foi renomeada acima para permitir o filtro antecipado)

# 🧠 Tensor: Optimize bulk column renaming
# 💡 What: Replaced iterative .withColumnRenamed() with a single vectorized df.toDF() projection.
# 🎯 Why: Iteratively calling .withColumnRenamed() creates deeply nested Project nodes in the Catalyst logical plan, leading to high compilation overhead and potential StackOverflowError.
# 📊 Impact: Reduce drastically the depth of the Catalyst query plan and compilation time.
# 🔬 Measurement: Plan compilation overhead for this segment drops from linear O(N) to O(1) in Catalyst.
cols_cedentes = {
    "DATAINCLUSAO": "DATAINCLUSAO_CEDENTE",
    "USUAINCLUSAO": "USUAINCLUSAO_CEDENTE",
    "DATAALTERACAO": "DATAALTERACAO_CEDENTE",
    "USUAALTERACAO": "USUAALTERACAO_CEDENTE",
    "CODRATING": "CODRATING_CEDENTE",
    "PEFIN": "PEFIN_CEDENTE",
    "BAIXADOPEFIN": "BAIXADOPEFIN_CEDENTE",
    "PREIMPRESSO": "PREIMPRESSO_CEDENTE",
    "BOLETOESPECIAL": "BOLETOESPECIAL_CEDENTE",
    "TARIFARECOMPRA": "TARIFARECOMPRA_CEDENTE",
    "RECEBEBOLETO": "RECEBEBOLETO_CEDENTE"
}
new_cols_cedentes = [cols_cedentes.get(c, c) for c in df_cedentes.columns]
df_cedentes = df_cedentes.toDF(*new_cols_cedentes)

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
