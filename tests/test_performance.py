import sys
import time
from unittest.mock import MagicMock

# Adiciona o notebook utilities ao path se necessário
# ou faz mock do spark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce, trim

def resolve_columns_old(df, target_cols):
    df_resolved = df
    df_cols = set(df.columns)
    for col_name in target_cols:
        col_op = f"{col_name}_op"
        if col_name in df_cols:
            col_target = when(trim(col(col_name)) == "", None).otherwise(col(col_name))
            df_resolved = df_resolved.withColumn(col_name, coalesce(col_target, col(col_op)))
        elif col_op in df_cols:
            df_resolved = df_resolved.withColumnRenamed(col_op, col_name)
    return df_resolved

def resolve_columns_new(df, target_cols):
    df_resolved = df
    df_cols = set(df.columns)

    exprs = {}
    renames = {}
    for col_name in target_cols:
        col_op = f"{col_name}_op"
        if col_name in df_cols:
            col_target = when(trim(col(col_name)) == "", None).otherwise(col(col_name))
            exprs[col_name] = coalesce(col_target, col(col_op))
        elif col_op in df_cols:
            renames[col_op] = col_name

    # Apply renames
    for old_col, new_col in renames.items():
        df_resolved = df_resolved.withColumnRenamed(old_col, new_col)

    # Aplicar expressões usando withColumns ou select
    if exprs:
        # No pyspark >= 3.3, withColumns pode receber um dicionário
        df_resolved = df_resolved.withColumns(exprs)

    return df_resolved

def test_performance():
    spark = SparkSession.builder.appName("perf_test").getOrCreate()

    # Cria um dataframe fictício com muitas colunas
    data = [{"col_" + str(i): "val" for i in range(100)}]
    data[0].update({"col_" + str(i) + "_op": "val_op" for i in range(100)})
    df = spark.createDataFrame(data)

    target_cols = ["col_" + str(i) for i in range(100)]

    # Warmup
    _ = resolve_columns_old(df, target_cols)

    start = time.time()
    for _ in range(10):
        df_out = resolve_columns_old(df, target_cols)
        df_out.select("col_0").explain() # To force plan generation, but let's just measure DAG construction
    end = time.time()
    print(f"Old approach took: {end - start:.4f}s")

    start = time.time()
    for _ in range(10):
        df_out = resolve_columns_new(df, target_cols)
        df_out.select("col_0").explain()
    end = time.time()
    print(f"New approach took: {end - start:.4f}s")

if __name__ == "__main__":
    test_performance()
