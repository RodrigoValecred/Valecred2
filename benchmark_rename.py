import timeit
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Benchmark").getOrCreate()

def old_way():
    df = spark.range(100)
    for i in range(50):
        df = df.withColumn(f"col_{i}", df["id"])

    # Do the renaming
    for i in range(50):
        df = df.withColumnRenamed(f"col_{i}", f"new_col_{i}")

    # Force catalyst evaluation
    return df.explain(extended=False)

def new_way():
    df = spark.range(100)
    for i in range(50):
        df = df.withColumn(f"col_{i}", df["id"])

    # Do the renaming
    cols_map = {f"col_{i}": f"new_col_{i}" for i in range(50)}
    new_cols = [cols_map.get(c, c) for c in df.columns]
    df = df.toDF(*new_cols)

    # Force catalyst evaluation
    return df.explain(extended=False)

print("Old way:")
old_time = timeit.timeit(old_way, number=5)
print(f"Old time: {old_time}")

print("New way:")
new_time = timeit.timeit(new_way, number=5)
print(f"New time: {new_time}")
