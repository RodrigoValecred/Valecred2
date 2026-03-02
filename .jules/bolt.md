## 2024-05-22 - [Optimizing PySpark Data Lineage]
**Learning:** Persisting foreign keys (like `cod_cliente`) in large fact tables (`fato_titulos`) can eliminate expensive downstream joins with other fact tables (`fato_operacoes`), significantly reducing shuffle and compute costs.
**Action:** Always check if a frequently joined column can be denormalized into the target fact table during its creation.

## 2026-02-18 - [Optimizing PySpark Pivots]
**Learning:** Multiple pivot operations on the same grouping key (e.g., one for MAX and one for MIN) double the shuffle cost. They can be combined into a single pivot with multiple aggregations (e.g., `pivot(...).agg(max(...), min(...))`) and then projected into separate DataFrames.
**Action:** Consolidate multiple pivot calls into a single pass whenever possible.

## 2025-03-03 - [Optimize PySpark to Pandas Conversion]
**Learning:** In Fabric/Spark pipelines, using `.toPandas()` on large DataFrames blocks the driver and creates huge bottlenecks, often leading to OOM errors. It's much better to rewrite logic using native PySpark functions (like `.withColumn()` and `when()`). For times when `.toPandas()` is unavoidable for plotting or interfacing with external ML libraries, configuring PyArrow (`spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")`) offers a massive speedup by reducing serialization overhead.
**Action:** When reviewing PySpark notebooks, always check for `.toPandas()`. If the logic can be rewritten in PySpark, refactor it. If pandas is necessary, ensure PyArrow is enabled.
