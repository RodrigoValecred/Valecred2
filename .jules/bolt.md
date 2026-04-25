## 2025-04-25 - Consolidating PySpark Driver Collections
**Learning:** PySpark operations like `.show()`, `.collect()`, and `.first()` are terminal actions that each trigger a separate Spark job and DAG re-evaluation. Calling these sequentially on an uncached DataFrame results in duplicate cluster overhead and table scans.
**Action:** When a small subset of data is needed for display and subsequent Python-side iteration, perform a single `.limit(N).collect()` and process the resulting Python list natively on the driver.
