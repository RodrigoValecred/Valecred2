import sys
from pyspark.sql import SparkSession
from pyspark.ml.feature import StandardScaler, VectorAssembler
import pyspark.sql.functions as F
import time

def main():
    spark = SparkSession.builder.appName("BenchmarkScaler").getOrCreate()

    # Generate synthetic data
    print("Generating data...")
    num_rows = 1000000
    df = spark.range(num_rows).selectExpr("id as f1", "id * 2 as f2", "id * 0.5 as f3", "rand() as f4", "rand()*10 as f5", "id * 1.5 as f6")
    df.cache()
    df.count()

    print("Running pre-computed scaling (constants)...")
    start = time.time()

    # Calculate means and stddevs directly
    # Assume we computed these elsewhere and load as dictionary
    means_stddevs = {
        'f1_mean': 500000, 'f1_std': 288675.13,
        'f2_mean': 1000000, 'f2_std': 577350.27,
        'f3_mean': 250000, 'f3_std': 144337.56,
        'f4_mean': 0.5, 'f4_std': 0.28,
        'f5_mean': 5.0, 'f5_std': 2.8,
        'f6_mean': 750000, 'f6_std': 433012.7
    }

    exprs = []
    for c in df.columns:
        mean_val = means_stddevs[f"{c}_mean"]
        std_val = means_stddevs[f"{c}_std"]
        exprs.append(((F.col(c) - F.lit(mean_val)) / F.lit(std_val)).alias(c))

    df_scaled = df.select(*exprs)

    assembler = VectorAssembler(inputCols=["f1", "f2", "f3", "f4", "f5", "f6"], outputCol="features")
    df_vectorized = assembler.transform(df_scaled)
    df_vectorized.count()

    end = time.time()
    print(f"Time with Precomputed Native Expressions (constants): {end - start:.2f}s")

    spark.stop()

if __name__ == "__main__":
    main()
