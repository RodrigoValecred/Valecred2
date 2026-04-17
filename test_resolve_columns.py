import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce, trim

spark = SparkSession.builder.appName("test_resolve").getOrCreate()

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

    for old_col, new_col in renames.items():
        df_resolved = df_resolved.withColumnRenamed(old_col, new_col)

    if exprs:
        # Pyspark withColumns usage
        df_resolved = df_resolved.withColumns(exprs)

    return df_resolved

# test data
data = [{"col1": "A", "col1_op": "A_op", "col2_op": "B_op", "col3": "  ", "col3_op": "C_op", "col4": "D"}]
df = spark.createDataFrame(data)

target_cols = ["col1", "col2", "col3"]

df_out = resolve_columns_new(df, target_cols)
df_out.show()
