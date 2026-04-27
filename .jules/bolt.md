## 2024-05-24 - Pandas .apply() Overhead with Pure Python Functions
**Learning:** In this codebase's Pandas workflows (specifically formatting UI strings in reporting notebooks), using `.apply()` with pure Python functions (like `format_currency_br` or `lambda` string formatters) incurs severe overhead due to Series indexing and function call wrapping per row. It is significantly slower than bypassing Pandas entirely.
**Action:** Always replace `.apply()` with native Python list comprehensions (e.g., `[func(x) for x in df['col']]`) when applying simple Python string manipulations or custom formatting functions across Pandas columns.

## 2024-06-12 - PySpark Catalyst Predicate Pushdown and crossJoin with Watermarks
**Learning:** In PySpark incremental load patterns, using `.crossJoin()` with a single-row aggregated watermark DataFrame against a large source table (e.g., Bronze) completely disables the Catalyst optimizer's ability to perform Predicate Pushdown. Catalyst treats it as a `BroadcastNestedLoopJoin` and forces a full table scan of the source before filtering. This is a common but highly inefficient pattern.
**Action:** Always extract the scalar watermark value directly into a Python variable using `.first()[0]`, and then use `lit(watermark_val)` in the `.filter()` condition. This guarantees that Spark can evaluate the bounds at the Parquet/Delta storage level and skip reading irrelevant data entirely.

## 2024-10-24 - Python UDF Overhead in Massive PySpark Cross Joins
**Learning:** In PySpark, using a custom Python UDF (e.g., `haversine_udf` mapping to a `math` based pure Python formula) during a `crossJoin` or other massive tabular expansion causes extreme overhead. Catalyst is forced to serialize and deserialize data row-by-row between the JVM and Python driver to execute the logic, which prevents predicate pushdown and Tungsten code generation.
**Action:** Always implement mathematical formulas (e.g., Haversine distance) using native PySpark SQL functions (`F.sin`, `F.cos`, `F.pow`, `F.radians`, `F.atan2`) inside `.withColumn` calls instead of a Python UDF. This executes entirely within the JVM/C++ backend and natively integrates with Spark optimizations, often running ~4x faster or more on large datasets.

## 2024-03-25 - PySpark Catalyst Plan Re-evaluation with Multiple Actions
**Learning:** In PySpark workflows (e.g., `NB_Curadoria_Gold.Notebook`), performing multiple actions (like `count()`, `sum()`, `collect()`) on the same DataFrame forces the Catalyst optimizer to re-evaluate the entire logical and physical plan from scratch, resulting in redundant full table scans for every action.
**Action:** Always explicitly call `.cache()` on the DataFrame before executing multiple actions, and `.unpersist()` immediately after to clear memory. This ensures the data is read into memory once, drastically reducing I/O and execution time.

## 2025-02-28 - PySpark DataFrame count() vs isEmpty()
**Learning:** Using `df.count() > 0` to check if a DataFrame has records (such as in incremental logic for `NB_Gold_Esteira_Propostas.Notebook`) triggers a full execution of the Catalyst physical plan across all partitions, acting as a massive bottleneck even for empty DataFrames.
**Action:** Always replace `df.count() > 0` with `not df.isEmpty()`. This restricts the scan operation to evaluating only the first partition and returns immediately upon finding a single record, avoiding full DAG materialization and saving precious computation time.

## 2025-03-05 - PySpark Eager Evaluation due to .count() Logging
**Learning:** In PySpark workflows, adding `.count()` calls to combined/union DataFrames purely for logging purposes forces an eager evaluation of the lineage up to that point. Without explicitly caching the DataFrame first, this can double the I/O and processing time when the same DataFrame is later used in transformations.
**Action:** Before calling `.count()` on intermediate DataFrames for logging, explicitly insert `df.cache()` (along with a documented Bolt optimization block) and make sure to append `df.unpersist()` after the final operation involving the dataframe to preserve optimal DAG evaluation paths and memory usage.
## 2025-04-02 - PySpark Catalyst StackOverflow with .withColumnRenamed() Chains
**Learning:** In PySpark, chaining multiple `.withColumnRenamed()` calls (e.g., more than 10) creates deeply nested `Project` nodes in the Catalyst Logical Plan. This forces the optimizer into excessive recursion during rule evaluation, causing severe performance overhead during plan generation and risking a `StackOverflowError` on large schemas.
**Action:** Always replace chained `.withColumnRenamed()` operations with a dictionary mapping or list comprehension applied via a single `df.toDF(*new_columns)` projection. This flattens the logical plan DAG to a single `Project` node, saving significant compilation time.
## 2025-05-18 - PySpark Eager Evaluation due to Log count() in Joins
**Learning:** In PySpark DataFrames, when a DataFrame is used simply to print a log `count()` and then immediately reused in a subsequent `.join()` operation, the Catalyst optimizer evaluates the physical plan twice. Without caching, the initial full table scan, along with any filters applied, is executed once for the count and a second time for the downstream join process. Replacing `count() > 0` with `isEmpty()` is not viable if the actual record count value is explicitly needed for logging immediately before the boolean check.
**Action:** When a DataFrame's actual record count is needed for logging prior to being utilized in downstream logic (such as joins or aggregations), always apply `.cache()` directly before `.count()` is invoked, and remember to append `.unpersist()` after the DataFrame is no longer needed.

## 2024-05-19 - Dangling Variables and DataFrame count()
**Learning:** When removing logging or debug statements that use `.count()` on large DataFrames to improve performance, beware of downstream variables (like `final_count`) that might still be referenced in later conditional checks. In PySpark, full `.count()` just to check if a dataframe has rows is extremely heavy; `.isEmpty()` must be used instead, but partial removals can cause `NameError` bugs.
**Action:** Always search the entire file for usages of any variables removed during a performance optimization, and replace PySpark `.count() > 0` checks with `.isEmpty()` systematically.

## 2024-06-25 - PySpark Broadcast Joins for Dimension Tables
**Learning:** In PySpark notebooks, when joining large fact tables (like `df_ops`) with small dimension DataFrames (like `df_usuarios` aliases, `df_motivos`, or `df_gerentes_enrich`), Catalyst optimizer might not automatically trigger BroadcastHashJoins. This forces a SortMergeJoin, which requires expensive, cluster-wide network shuffles of the massive fact table, severely degrading performance.
**Action:** Always explicitly wrap small dimension DataFrames with `pyspark.sql.functions.broadcast(df_dimension)` inside the `.join()` call to force a BroadcastHashJoin. This broadcasts the tiny table to all executors and executes the join purely locally, eliminating the shuffle phase entirely.

## 2026-04-16 - Testing Mock Challenges with PySpark Broadcast Joins
**Learning:** Adding PySpark structural functions like `broadcast()` to DataFrame method chains inside dynamically extracted functions (using `exec()`) can severely break existing mock setups. The original mock chains (`MagicMock().join().drop()`) fail because `broadcast` expects a recognizable object, and its injection raises `NameError` if it isn't passed through `exec_globals`.
**Action:** When adding structural optimizers like `broadcast()` to code tested via `exec()`, explicitly inject a passthrough lambda (e.g., `'broadcast': lambda x: x`) into the test's `exec_globals` to ensure the mock DataFrame chaining remains intact without throwing structural errors.

## 2024-05-20 - PySpark Forceful Termination Bypassing finally Block
**Learning:** In PySpark notebooks (like Fabric, Synapse), wrapping a DataFrame's `.unpersist()` in a `try...finally` block is essential for guaranteeing that memory is cleared even if an exception occurs during actions like writing data to delta. Without this, failed jobs can leave massive cached datasets in memory, degrading the performance of subsequent queries or notebooks running on the same cluster.
**Action:** When calling `.cache()` on DataFrames that are subsequently used for actions like `.count()`, `.display()` or `.write`, always wrap the action and any subsequent commands in a `try` block, placing `.unpersist()` in the `finally` block to guarantee execution. If using `mssparkutils.notebook.exit()`, ensure it runs after the `finally` block or that resources are explicitly cleaned up before exiting.

## 2026-04-18 - Testing Mock Challenges with PySpark Broadcast Joins
**Learning:** Adding PySpark structural functions like `broadcast()` to DataFrame method chains inside dynamically extracted functions (using `exec()`) can severely break existing mock setups. The original mock chains (`MagicMock().join().drop()`) fail because `broadcast` expects a recognizable object, and its injection raises `NameError` if it isn't passed through `exec_globals`.
**Action:** When adding structural optimizers like `broadcast()` to code tested via `exec()`, explicitly inject a passthrough lambda (e.g., `'broadcast': lambda x: x`) into the test's `exec_globals` to ensure the mock DataFrame chaining remains intact without throwing structural errors.

## 2024-05-28 - PySpark DataFrame Columns Access Overhead in Loops
**Learning:** In PySpark, calling `df.columns` inside a loop (e.g., when iterating through a large list of target columns to resolve or rename) is extremely inefficient because it triggers a remote procedure call (RPC) to the driver node on every iteration to fetch the schema metadata.
**Action:** Always cache the DataFrame columns into a local Python set before the loop using `cols_set = set(df.columns)`. This reduces the N remote RPC calls to a single call and provides fast O(1) lookups during the iteration, significantly decreasing loop execution time.

## 2024-05-19 - Flatten PySpark withColumn Logic
**Learning:** In PySpark, deeply chained `withColumn` statements generate deeply nested Catalyst Logical Plans filled with sequential `Project` nodes. When the DAG becomes large, this leads to an $O(N^2)$ compilation slowdown and increases the risk of driver `StackOverflowError`.
**Action:** Always combine chained sequential `.withColumn` transformations into a single `withColumns(dict)` block, or project the resulting expressions simultaneously in a single `.select()` statement.

## 2025-05-19 - Flatten PySpark Python reduce/map loops for withColumn
**Learning:** Iteratively building a PySpark DataFrame by using Python `reduce()` or loops to chain `df.withColumn(c, expr)` builds massive nested Catalyst Logical Plans. When applied over many columns (e.g. converting a large list of columns to uppercase), the Catalyst optimizer spends excessive time traversing `Project` nodes, causing severe compilation delays.
**Action:** Avoid using `reduce` with lambda functions to chain `withColumn`. Instead, generate a list of PySpark expressions (`[upper(col(c)).alias(c) if c in cols else col(c) for c in df.columns]`) and unpack it into a single `df.select(*expr_list)`. This reduces the Catalyst plan to a single `Project` node, speeding up compilation dramatically.
