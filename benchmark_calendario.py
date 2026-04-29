import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max

spark = SparkSession.builder.appName("Benchmark").getOrCreate()

# Create dummy data
data = [(f"2023-01-{i:02d}",) for i in range(1, 31)] * 10000
df = spark.createDataFrame(data, ["data_inclusao"])

# Approach 1: collect()
start = time.time()
for _ in range(10):
    val_collect = df.agg(min("data_inclusao")).collect()[0][0]
end1 = time.time() - start

# Approach 2: first()[0]
start = time.time()
for _ in range(10):
    val_first = df.agg(min("data_inclusao")).first()[0]
end2 = time.time() - start

print(f"Collect time: {end1:.4f}s")
print(f"First time: {end2:.4f}s")
