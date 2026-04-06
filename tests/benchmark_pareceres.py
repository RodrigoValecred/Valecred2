import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

def benchmark():
    spark = SparkSession.builder.appName("bench").master("local[4]").getOrCreate()

    # Cria um grande conjunto de dados mock
    num_rows = 100000
    df = spark.range(num_rows).withColumn("obs_normalized", col("id").cast("string"))

    keywords = {
        "K1": "1", "K2": "2", "K3": "3", "K4": "4", "K5": "5",
        "K6": "6", "K7": "7", "K8": "8", "K9": "9", "K10": "0"
    }

    # Método 1: Iterativo com withColumn
    t0 = time.time()
    df_iter = df
    for col_name, search_term in keywords.items():
        df_iter = df_iter.withColumn(
            col_name.lower(),
            when(col("obs_normalized").contains(search_term), 1).otherwise(0)
        )
    df_iter.write.mode("overwrite").parquet("/tmp/bench_iter")
    t1 = time.time()

    # Método 2: Seleção (select) consolidada
    t2 = time.time()
    expr_list = [
        when(col("obs_normalized").contains(search_term), 1).otherwise(0).alias(col_name.lower())
        for col_name, search_term in keywords.items()
    ]
    df_vector = df.select("*", *expr_list)
    df_vector.write.mode("overwrite").parquet("/tmp/bench_vector")
    t3 = time.time()

    print(f"Iterative: {t1-t0:.2f}s")
    print(f"Consolidated: {t3-t2:.2f}s")

if __name__ == "__main__":
    benchmark()
