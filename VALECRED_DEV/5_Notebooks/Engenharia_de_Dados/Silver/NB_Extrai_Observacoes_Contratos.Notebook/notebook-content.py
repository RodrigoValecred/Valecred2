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

# CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

# 1. Imports (Tudo junto aqui em cima)
from pyspark.sql.functions import col, regexp_extract, regexp_replace, decode, current_timestamp, trim

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. Leitura da Camada Bronze
# df_bronze = spark.read.format("delta").load("Tables/cad_contratos_clientes") 
# ou via SQL como você fez:
df_bronze = spark.sql("SELECT * FROM LH_Bronze.cad_contratos_clientes WHERE STATUS = 'A' and CODCLIENTE =  15283149")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3. Definição das Regex
pat_geral       = r"Limite Geral:.*?R\$\s*([\d\.,]+)"
pat_comissaria  = r"Comiss.ria Simples:.*?R\$\s*([\d\.,]+)"
pat_inter       = r"Intercompany:.*?R\$\s*([\d\.,]+)"
pat_fomento     = r"Fomento:.*?R\$\s*([\d\.,]+)"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3. Transformação "TRADUZINDO" seu SQL para PySpark
df_silver = df_bronze.withColumn(
    # Passo A: Decodificar (Igual ao CONVERT ... USING latin1)
    # Se a coluna no Lakehouse já for String mas estiver bagunçada, o decode converte binário corretamente
    "obs_decodificada", 
    decode(col("OBSERVACOES"), "ISO-8859-1") 
).withColumn(
    # Passo B: Limpar (Igual ao REPLACE \r e \n por espaço)
    "texto_limpo", 
    trim(regexp_replace(col("obs_decodificada"), "[\r\n]+", " "))
).withColumn(
    # Passo C: Extrair os valores (Regex)
    "raw_limite_geral", regexp_extract(col("texto_limpo"), pat_geral, 1)
).withColumn(
    "raw_limite_comissaria", regexp_extract(col("texto_limpo"), pat_comissaria, 1)
).withColumn(
    "raw_limite_intercompany", regexp_extract(col("texto_limpo"), pat_inter, 1)
).withColumn(
    "raw_limite_fomento", regexp_extract(col("texto_limpo"), pat_fomento, 1)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5. Função auxiliar de conversão
def converter_moeda_br(col_name):
    return (
        regexp_replace(
            regexp_replace(col(col_name), "\.", ""), # Tira ponto de milhar
            ",", "." # Troca vírgula por ponto
        ).cast("double")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 6. Seleção Final e Conversão
df_final = df_silver.select(
    col("CODCLIENTE"),
    col("texto_limpo").alias("OBS_TRATADA"),
    converter_moeda_br("raw_limite_geral").alias("limite_geral"),
    converter_moeda_br("raw_limite_comissaria").alias("limite_comissaria"),
    converter_moeda_br("raw_limite_intercompany").alias("limite_intercompany"),
    converter_moeda_br("raw_limite_fomento").alias("limite_fomento")
).withColumn("dt_processamento_silver", current_timestamp()) # Adiciona data aqui no final

# Exibir
display(df_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 7. Salvar em uma tabela da camada Silver
df_final.write.mode("overwrite").option("overwriteSchema","true").saveAsTable("LH_Silver.stg_limites_contratos_silver")
print("Tabela Silver gravada com sucesso!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
