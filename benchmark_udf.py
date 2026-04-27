import time
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import pyspark.sql.functions as F
from pyspark.sql.functions import pandas_udf

spark = SparkSession.builder.appName("BenchmarkUDF").config("spark.sql.execution.arrow.pyspark.enabled", "true").getOrCreate()

data = [("Here is some text with &Ccedil; and &nbsp; and &amp; and also &quot;.",) for _ in range(1000000)]
df = spark.createDataFrame(data, ["text"])
df.cache()
df.count()

@pandas_udf(StringType())
def unescape_udf(text: pd.Series) -> pd.Series:
    import html
    return text.str.replace(r'&[a-zA-Z0-9#]+;', lambda m: html.unescape(m.group(0)), regex=True)

print("Running with Pandas UDF...")
start = time.time()
df_udf = df.withColumn("text_clean", unescape_udf(F.col("text")))
df_udf.select(F.max(F.length("text_clean"))).collect()
print(f"Time: {time.time() - start:.2f}s")

print("Running with PySpark Native regex...")
start = time.time()
# Mapeamento para decodificação manual (simplificado para demonstração)
# Na prática, se usarmos replace multiplos
mapping = {"&Ccedil;": "Ç", "&nbsp;": " ", "&amp;": "&", "&quot;": "\""}

col_expr = F.col("text")
for k, v in mapping.items():
    col_expr = F.regexp_replace(col_expr, k, v)

df_native = df.withColumn("text_clean", col_expr)
df_native.select(F.max(F.length("text_clean"))).collect()
print(f"Time: {time.time() - start:.2f}s")
