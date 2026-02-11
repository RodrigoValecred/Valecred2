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
from pyspark.sql.functions import col, regexp_extract, regexp_replace, decode, current_timestamp, trim, coalesce, lit

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. Leitura da Camada Bronze
# df_bronze = spark.read.format("delta").load("Tables/cad_contratos_clientes") 
# ou via SQL como você fez:
df_bronze = spark.sql("SELECT * FROM LH_Bronze.cad_contratos_clientes WHERE STATUS = 'A'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3. Definição das Regex "Case Insensitive" ( Ignora Maiúscula/Minúscula)
# O código (?i) no começo torna a busca insensível a caixa alta/baixa
# ReDoS Mitigation: Use bounded quantifier .{0,200}? instead of unbounded .*? to prevent excessive backtracking
pat_geral       = r"(?i)Limite\s+(?:Geral|Total).{0,200}?R\$\s*([\d\.,]+)"
pat_comissaria  = r"(?i)Comiss.ria Simples.{0,200}?R\$\s*([\d\.,]+)"
pat_inter       = r"(?i)Intercompany.{0,200}?R\$\s*([\d\.,]+)"
pat_fomento     = r"(?i)Fomento.{0,200}?R\$\s*([\d\.,]+)"
pat_plus        = r"(?i)Limite\s+EXTRA\s+PLUS.{0,200}?R\$\s*([\d\.,]+)"
pat_extra_formal   = r"(?i)Limite\s+extra\s+desconto.{0,200}?\bformal.{0,200}?R\$\s*([\d\.,]+)"
pat_extra_informal = r"(?i)Limite\s+extra\s+desconto.{0,200}?\binformal.{0,200}?R\$\s*([\d\.,]+)"

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
    # Adicionando replace para NBSP (\u00A0) e tabulações para garantir o match
    "texto_limpo", 
    trim(regexp_replace(col("obs_decodificada"), "[\r\n\t\u00A0]+", " "))
).withColumn(
    # Passo C: Extrair os valores (Regex)
    "raw_limite_geral", regexp_extract(col("texto_limpo"), pat_geral, 1)
).withColumn(
    "raw_limite_comissaria", regexp_extract(col("texto_limpo"), pat_comissaria, 1)
).withColumn(
    "raw_limite_intercompany", regexp_extract(col("texto_limpo"), pat_inter, 1)
).withColumn(
    "raw_limite_fomento", regexp_extract(col("texto_limpo"), pat_fomento, 1)
).withColumn(
    "raw_limite_plus", regexp_extract(col("texto_limpo"), pat_plus, 1)
).withColumn(
    "raw_limite_extra_formal", regexp_extract(col("texto_limpo"), pat_extra_formal, 1)
).withColumn(
    "raw_limite_extra_informal", regexp_extract(col("texto_limpo"), pat_extra_informal, 1)
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
    # Se for nulo, coloca 0.0
    coalesce(converter_moeda_br("raw_limite_geral"), lit(0.0)).alias("limite_geral"),
    coalesce(converter_moeda_br("raw_limite_comissaria"), lit(0.0)).alias("limite_comissaria"),
    coalesce(converter_moeda_br("raw_limite_intercompany"), lit(0.0)).alias("limite_intercompany"),
    coalesce(converter_moeda_br("raw_limite_fomento"), lit(0.0)).alias("limite_fomento"),
    coalesce(converter_moeda_br("raw_limite_plus"), lit(0.0)).alias("limite_extra_plus"),
    coalesce(converter_moeda_br("raw_limite_extra_formal"), lit(0.0)).alias("limite_extra_desconto_formal"),
    coalesce(converter_moeda_br("raw_limite_extra_informal"), lit(0.0)).alias("limite_extra_desconto_informal")
).withColumn("dt_processamento_silver", current_timestamp())

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
