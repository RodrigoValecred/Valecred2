import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
try:
    df = spark.table("LH_Bronze.Bronze_Operacoes_Intraday")
    print(df.columns)
except Exception as e:
    print(e)
