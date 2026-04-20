import sys
from pyspark.sql import SparkSession
from pyspark.ml.feature import StandardScaler, VectorAssembler
import pyspark.sql.functions as F
import time

def main():
    spark = SparkSession.builder.appName("BenchmarkScaler").getOrCreate()

    # Gera dados sintéticos
    print("Generating data...")
    num_rows = 1000000
    df = spark.range(num_rows).selectExpr("id as f1", "id * 2 as f2", "id * 0.5 as f3", "rand() as f4", "rand()*10 as f5", "id * 1.5 as f6")
    df.cache()
    df.count()

    print("Running pre-computed scaling (pandas/vectorized)...")
    start = time.time()

    # Calcula médias e desvios padrão diretamente
    # Em um cenário real, estes seriam carregados de uma execução anterior ou configuração
    means_stddevs = df.select(
        *[F.mean(F.col(c)).alias(f"{c}_mean") for c in df.columns],
        *[F.stddev(F.col(c)).alias(f"{c}_std") for c in df.columns]
    ).collect()[0].asDict()

    exprs = []
    for c in df.columns:
        mean_val = means_stddevs[f"{c}_mean"]
        std_val = means_stddevs[f"{c}_std"]
        if std_val == 0 or std_val is None:
            std_val = 1.0
        exprs.append(((F.col(c) - F.lit(mean_val)) / F.lit(std_val)).alias(c))

    df_scaled = df.select(*exprs)

    assembler = VectorAssembler(inputCols=["f1", "f2", "f3", "f4", "f5", "f6"], outputCol="features")
    df_vectorized = assembler.transform(df_scaled)
    df_vectorized.count()

    end = time.time()
    print(f"Time with Precomputed Native Expressions: {end - start:.2f}s")

    spark.stop()

if __name__ == "__main__":
    main()
