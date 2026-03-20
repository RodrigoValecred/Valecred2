import time
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from datetime import date, timedelta
import random

def create_mock_data(spark):
    num_gerentes = 200000  # 200k para garantir diferença visível
    months = 12

    # Gerentes DataFrame
    df_gerentes = spark.range(num_gerentes).select(F.format_string("G%d", "id").alias("id_gerente"))

    # Months DataFrame (cross join resultará em 2.4M de linhas)
    # Usando range para simular meses
    df_months = spark.range(months).select(F.date_add(F.lit("2023-01-01"), (F.col("id")*30).cast("int")).alias("mes_ref"))

    # Cross Join e adiciona métrica aleatória
    df = df_gerentes.crossJoin(F.broadcast(df_months)) \
        .withColumn("resultado_operacional", (F.rand() * 10000).cast("double"))

    return df

def run_original(spark, df_final_metrics):
    mes_corte = date(2023, 1, 1)

    # 1. Filter & Agg
    df_perf_12m = df_final_metrics.filter(F.col("mes_ref") >= mes_corte) \
        .groupBy("id_gerente") \
        .agg(F.sum("resultado_operacional").alias("res_acum"))

    # 2. Quantil e Collect (O gargalo)
    try:
        corte_top = df_perf_12m.approxQuantile("res_acum", [0.75], 0.01)[0]
        top_performers = df_perf_12m.filter(F.col("res_acum") >= corte_top).select("id_gerente").rdd.flatMap(lambda x: x).collect()
    except Exception as e:
        print(f"Error in original: {e}")
        top_performers = []

    # 3. Flag using isin
    df_res = df_final_metrics.withColumn("is_top_performer", F.col("id_gerente").isin(top_performers))

    # Force evaluation
    count = df_res.filter(F.col("is_top_performer") == True).count()
    return count

def run_optimized(spark, df_final_metrics):
    mes_corte = date(2023, 1, 1)

    # 1. Filter & Agg
    df_perf_12m = df_final_metrics.filter(F.col("mes_ref") >= mes_corte) \
        .groupBy("id_gerente") \
        .agg(F.sum("resultado_operacional").alias("res_acum"))

    # 2. Quantile & Join (Optimized)
    try:
        corte_top = df_perf_12m.approxQuantile("res_acum", [0.75], 0.01)[0]

        # Get Top Performers DataFrame
        df_top_performers = df_perf_12m.filter(F.col("res_acum") >= corte_top).select("id_gerente")

        # Usa a lógica de Left Semi Join para sinalizar
        # We want to keep all rows in df_final_metrics and add a flag column

        # Abordagem: Join com coluna constante, depois coalesce
        df_top_with_flag = df_top_performers.withColumn("is_top_flag", F.lit(True))

        df_res = df_final_metrics.join(df_top_with_flag, on="id_gerente", how="left") \
            .withColumn("is_top_performer", F.coalesce(F.col("is_top_flag"), F.lit(False))) \
            .drop("is_top_flag")

    except Exception as e:
        print(f"Error in optimized: {e}")
        df_res = df_final_metrics.withColumn("is_top_performer", F.lit(False))

    # Force evaluation
    count = df_res.filter(F.col("is_top_performer") == True).count()
    return count

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Benchmark Top Performers") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print("Generating data (200k gerentes x 12 months)...")
    df = create_mock_data(spark)
    df.cache()
    print(f"Data count: {df.count()}")

    # Warmup
    print("Warming up...")
    df.groupBy("id_gerente").count().count()

    print("\n--- Running Original (collect + isin) ---")
    start = time.time()
    c1 = run_original(spark, df)
    t1 = time.time() - start
    print(f"Original Time: {t1:.4f}s, Count: {c1}")

    print("\n--- Running Optimized (join) ---")
    start = time.time()
    c2 = run_optimized(spark, df)
    t2 = time.time() - start
    print(f"Optimized Time: {t2:.4f}s, Count: {c2}")

    if c1 == c2:
        print("\nSUCCESS: Counts match!")
    else:
        print(f"\nFAILURE: Counts mismatch! {c1} vs {c2}")
        sys.exit(1)

    spark.stop()
