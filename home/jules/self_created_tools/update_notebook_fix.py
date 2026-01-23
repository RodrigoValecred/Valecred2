
import sys
import re

# Read the file
filepath = "VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Curadoria_Gold.Notebook/notebook-content.py"
with open(filepath, "r") as f:
    content = f.read()

# --- 1. Fix Imports in Célula 1.2 ---
# Search for: from pyspark.sql.functions import unix_timestamp, ceil, abs, hour, month, weekofyear, dayofmonth
# Replace with: from pyspark.sql.functions import unix_timestamp, ceil, abs, hour, month, weekofyear, dayofmonth, last_day, months_between, floor

search_imports = "from pyspark.sql.functions import unix_timestamp, ceil, abs, hour, month, weekofyear, dayofmonth"
replace_imports = "from pyspark.sql.functions import unix_timestamp, ceil, abs, hour, month, weekofyear, dayofmonth, last_day, months_between, floor"

content = content.replace(search_imports, replace_imports)

# --- 2. Fix Escrow Schema in Célula 0.2 ---
# Search for: StructField("ESCROW", StringType(), True)
# Replace with: StructField("ESCROW", BooleanType(), True)
# Note: I need to make sure BooleanType is imported or use "boolean" string in DDL or just assume it is imported.
# Célula 0.1 imports: StructType, StructField, StringType, LongType, TimestampType, DoubleType, DateType.
# BooleanType is NOT imported.
# So I should use BooleanType from pyspark.sql.types.
# Or add BooleanType to import in Célula 0.1.
# Easier to add BooleanType to import in Célula 0.1 first.

search_types_import = "from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, DoubleType, DateType"
replace_types_import = "from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, DoubleType, DateType, BooleanType"

content = content.replace(search_types_import, replace_types_import)

# Now fix the schema definition
search_escrow_schema = 'StructField("ESCROW", StringType(), True)'
replace_escrow_schema = 'StructField("ESCROW", BooleanType(), True)'

content = content.replace(search_escrow_schema, replace_escrow_schema)

# --- 3. Fix Status Escrow Logic in Célula 1.2 ---
# Search for: .withColumn("status_escrow", when(col("flag_escrow") == True, "sim").otherwise("não")) \
# Replace with safe logic
search_status_escrow = '.withColumn("status_escrow", when(col("flag_escrow") == True, "sim").otherwise("não")) \\'
replace_status_escrow = '.withColumn("status_escrow", when(col("flag_escrow").cast("boolean") == True, "sim").otherwise("não")) \\'

content = content.replace(search_status_escrow, replace_status_escrow)

# Write back
with open(filepath, "w") as f:
    f.write(content)
