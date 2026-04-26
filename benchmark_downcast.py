import time
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("BenchmarkDowncast") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Create a wide dataframe with many doubles
num_rows = 1000000
num_cols = 20

print("Generating data...")
exprs = ["id"] + [f"rand() * 1000 as f{i}" for i in range(num_cols)]
df = spark.range(num_rows).selectExpr(*exprs)
df.cache()
df.count()

print("Running .toPandas() with DoubleType...")
start = time.time()
pdf_double = df.toPandas()
print(f"Time with DoubleType: {time.time() - start:.2f}s")
print(f"Pandas RAM: {pdf_double.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
del pdf_double

print("Running .toPandas() with FloatType (Downcast in Spark)...")
start = time.time()
cast_exprs = [F.col("id")] + [F.col(f"f{i}").cast("float").alias(f"f{i}") for i in range(num_cols)]
df_float = df.select(*cast_exprs)
pdf_float = df_float.toPandas()
print(f"Time with FloatType: {time.time() - start:.2f}s")
print(f"Pandas RAM: {pdf_float.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
del pdf_float

spark.stop()
