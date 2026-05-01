import pandas as pd
from typing import Iterator, Tuple
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import DoubleType

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([(1.0, 2.0), (3.0, 4.0)], ["a", "b"])

@pandas_udf(DoubleType())
def predict_udf(iterator: Iterator[Tuple[pd.Series, ...]]) -> Iterator[pd.Series]:
    # loaded once
    multiplier = 2.0
    for cols in iterator:
        s1, s2 = cols
        yield s1 + s2 * multiplier

df.withColumn("res", predict_udf(col("a"), col("b"))).show()
