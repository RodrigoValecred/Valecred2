import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, when, length, substring, concat, lit

spark = SparkSession.builder.appName("Benchmark").getOrCreate()

# Create dummy data
data = [("I", "12345678901", "123", "2024-01-01", "EXTRA_COL")] * 1000
df_bronze_limites = spark.createDataFrame(data, ["TIPO", "CPFCNPJ", "CODCLIENTE", "DATAINCLUSAO", "EXTRA"])

# Scenario 1: Chained .withColumn
start_time_chained = time.time()
df_chained = df_bronze_limites \
    .withColumn("tipo", regexp_replace(col("TIPO"), "^I$", "INTERCIA")) \
    .withColumn("tipo_documento_sacado", when(length(col("CPFCNPJ")) == 11, "CPF").when(length(col("CPFCNPJ")) == 14, "CNPJ").otherwise("Inválido")) \
    .withColumn("raiz_cnpj", when(col("tipo_documento_sacado") == "CNPJ", substring(col("CPFCNPJ"), 1, 8)).otherwise(col("CPFCNPJ"))) \
    .withColumn("chave_cliente_sacado", concat(col("CODCLIENTE").cast("string"), lit("-"), col("raiz_cnpj"))) \
    .withColumnRenamed("CODCLIENTE", "cod_cliente") \
    .withColumnRenamed("CPFCNPJ", "cpf_cnpj") \
    .withColumnRenamed("DATAINCLUSAO", "data_inclusao")

df_chained = df_chained.select([col(c).alias(c.lower()) for c in df_chained.columns])
df_chained.explain()
df_chained.show(5)
end_time_chained = time.time()

# Scenario 2: Single .select (Optimized)
start_time_select = time.time()

# Construindo as expressões base para reutilização
expr_tipo = regexp_replace(col("TIPO"), "^I$", "INTERCIA")
expr_tipo_doc = when(length(col("CPFCNPJ")) == 11, "CPF").when(length(col("CPFCNPJ")) == 14, "CNPJ").otherwise("Inválido")
expr_raiz = when(expr_tipo_doc == "CNPJ", substring(col("CPFCNPJ"), 1, 8)).otherwise(col("CPFCNPJ"))
expr_chave = concat(col("CODCLIENTE").cast("string"), lit("-"), expr_raiz)

# Para evitar ambiguidade (AnalysisException) quando fizermos .lower() nas colunas do DataFrame,
# precisamos remover TIPO, CPFCNPJ, CODCLIENTE e DATAINCLUSAO originais,
# mas também precisamos manter colunas extras não tocadas (ex: EXTRA) que `df_bronze_limites` possa ter.
# Isso equivale a fazer um select com todas as colunas não-renomeadas + as novas, MAS com os aliases já em lowercase.

original_cols = [c for c in df_bronze_limites.columns if c not in ["TIPO", "CPFCNPJ", "CODCLIENTE", "DATAINCLUSAO"]]
# Mantemos essas originais, já em lowercase
select_exprs = [col(c).alias(c.lower()) for c in original_cols]

# Adicionamos as transformadas e renomeadas, já no formato final minúsculo
select_exprs.extend([
    expr_tipo.alias("tipo"),
    expr_tipo_doc.alias("tipo_documento_sacado"),
    expr_raiz.alias("raiz_cnpj"),
    expr_chave.alias("chave_cliente_sacado"),
    col("CODCLIENTE").alias("cod_cliente"),
    col("CPFCNPJ").alias("cpf_cnpj"),
    col("DATAINCLUSAO").alias("data_inclusao")
])

df_select = df_bronze_limites.select(*select_exprs)

df_select.explain()
df_select.show(5)
end_time_select = time.time()

print(f"Time with chained .withColumn: {end_time_chained - start_time_chained:.4f}s")
print(f"Time with single .select: {end_time_select - start_time_select:.4f}s")
