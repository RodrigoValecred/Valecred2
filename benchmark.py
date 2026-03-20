import time
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

def main():
    spark = SparkSession.builder.appName("Benchmark").master("local[1]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Create a DataFrame with 10000 columns to exaggerate O(N) cost
    cols = [f"col_{i}" for i in range(10000)]
    df = spark.createDataFrame([(1,)*10000], cols)

    # 1000 candidates to check
    candidates = [f"cand_{i}" for i in range(1000)]
    # Target candidate is at the end to force worst case
    candidates.append("col_9999")

    def rename_first_match_list(df, candidates, target_name):
        existing_cols = df.columns
        if target_name in existing_cols:
             return df
        for cand in candidates:
            if cand in existing_cols:
                return df.withColumnRenamed(cand, target_name)
        return df.withColumn(target_name, lit(0))

    def rename_first_match_set(df, candidates, target_name):
        existing_cols = set(df.columns)
        if target_name in existing_cols:
             return df
        for cand in candidates:
            if cand in existing_cols:
                return df.withColumnRenamed(cand, target_name)
        return df.withColumn(target_name, lit(0))

    # Warmup
    _ = rename_first_match_list(df, candidates, "target_1")
    _ = rename_first_match_set(df, candidates, "target_2")

    list_times = []
    set_times = []

    for _ in range(50):
        start = time.perf_counter()
        rename_first_match_list(df, candidates, "target_list")
        list_times.append(time.perf_counter() - start)

        start = time.perf_counter()
        rename_first_match_set(df, candidates, "target_set")
        set_times.append(time.perf_counter() - start)

    avg_list = sum(list_times) / len(list_times)
    avg_set = sum(set_times) / len(set_times)

    print(f"List lookup average time: {avg_list:.6f} s")
    print(f"Set lookup average time:  {avg_set:.6f} s")
    print(f"Improvement: {avg_list/avg_set:.2f}x faster")

if __name__ == "__main__":
    main()
