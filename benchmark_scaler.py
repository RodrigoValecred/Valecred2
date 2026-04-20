import sys
from pyspark.sql import SparkSession
from pyspark.ml.feature import StandardScaler, VectorAssembler
import time

def main():
    spark = SparkSession.builder.appName("BenchmarkScaler").getOrCreate()

    # Gera dados sintéticos
    print("Generating data...")
    num_rows = 1000000
    df = spark.range(num_rows).selectExpr("id as f1", "id * 2 as f2", "id * 0.5 as f3", "rand() as f4", "rand()*10 as f5", "id * 1.5 as f6")

    assembler = VectorAssembler(inputCols=["f1", "f2", "f3", "f4", "f5", "f6"], outputCol="features_raw")
    df_vectorized = assembler.transform(df)
    df_vectorized.cache()
    df_vectorized.count()

    print("Running with StandardScaler.fit().transform()...")
    start = time.time()
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
    scaler_model = scaler.fit(df_vectorized)
    df_scaled = scaler_model.transform(df_vectorized)
    df_scaled.count()
    end = time.time()
    print(f"Time with MLlib StandardScaler: {end - start:.2f}s")

    spark.stop()

if __name__ == "__main__":
    main()
