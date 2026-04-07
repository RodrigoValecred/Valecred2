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
# META           "id": "ee40705b-0100-49bc-8f35-81d71839f042"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col, lit, explode, sequence, to_date, last_day, when, sum as _sum, months_between, expr, broadcast

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Definir o Período de Análise (ex: Últimos 12 meses até hoje)
# Gera uma linha para cada fim de mês: 2024-01-31, 2024-02-29...
df_calendario = spark.sql("""
    SELECT explode(
        sequence(to_date('2024-01-01'), current_date(), interval 1 month)
    ) as inicio_mes
""").select(last_day("inicio_mes").alias("DATA_CORTE"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. Ler seus Títulos e Operações (ajuste os nomes das tabelas)
df_titulos = spark.read.table("LH_Silver.staging_titulos_limpa").filter(
    col("aceito")=="S"
) # Tabela de parcelas
df_operacoes = spark.read.table("LH_Silver.staging_operacoes_limpa").filter(
    (col("status_analise") == "D") &
    (col("status_aceite") == "A")
).select("cod_operacao", "data_analise") # Para saber data de aceite
df_pareceres = spark.read.table("LH_Silver.staging_pareceres_operacoes").select("cod_operacao","IS_LIMITE_PLUS") # Para saber se é PLUS

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3. Preparação dos Dados (O JOIN acontece AGORA)
# Trazemos a DATAACEITE e a Flag PLUS para o nível do título
df_titulos_enrich = df_titulos.join(
    df_operacoes, 
    on="cod_operacao", 
    how="inner" # Só queremos títulos que tenham operação válida
).join(
    df_pareceres,
    on="cod_operacao",
    how="left"
).fillna({"IS_LIMITE_PLUS": "NAO"})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 4. Join com Calendário (Otimizado com Broadcast e Condição)
# Substituir Cross join + Filter por join Condicional
# ⚡ Bolt Optimization: Usar Broadcast Join com condições em vez de CrossJoin + Filter
df_calculo_status = df_titulos_enrich.join(
    broadcast(df_calendario),
    (col("data_analise") <= col("DATA_CORTE")) &
    ((col("liquidacao").isNull()) | (col("liquidacao") > col("DATA_CORTE"))),
    "inner"
).withColumn("IS_ABERTO_NA_DATA", lit(1))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 6. Classificar Inadimplência na Data (Lógica Mantida)
# Comparando DATA_CORTE (Foto) com DT_VENCIMENTO (Título)
df_status_final = df_calculo_status.withColumn(
    "DIAS_ATRASO_NA_DATA", 
    expr("datediff(DATA_CORTE, venc_prorrogado)")
).withColumn(
    "IS_INADIMPLENTE_NA_DATA",
    when(col("DIAS_ATRASO_NA_DATA") > 5, lit(1)).otherwise(lit(0))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 7. Agregação Final (Por Mês e Tipo de Limite)
df_agregado_mensal = df_status_final.groupBy("DATA_CORTE", "IS_LIMITE_PLUS").agg(
    _sum("valor_devido").alias("CARTEIRA_ATIVA"), # Ajuste se usar valor presente
    _sum(when(col("IS_INADIMPLENTE_NA_DATA") == 1, col("valor_devido")).otherwise(0)).alias("SALDO_INADIMPLENTE")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Gravar na Gold
df_agregado_mensal.write.mode("overwrite").saveAsTable("LH_Gold.dim_historico_inadimplencia")
print("tabela criada com sucesso!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
