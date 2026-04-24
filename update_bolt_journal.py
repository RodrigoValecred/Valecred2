import os

journal_path = ".jules/bolt.md"
with open(journal_path, "a") as f:
    f.write("\n\n## 2025-05-29 - Preserving Positional Ordering During Column Caching\n")
    f.write("**Learning:** When optimizing list comprehensions in PySpark by caching `df.columns` to avoid repeated remote procedure calls (RPC) to the Spark driver, extracting `df.columns` into a Python `set` destroys the original positional ordering. This causes massive data corruption bugs when applying the cached columns later via positional functions like `df.toDF(*new_columns)`.\n")
    f.write("**Action:** When extracting PySpark DataFrame columns for local caching prior to positional mapping or selection loops, ALWAYS cast the columns to a Python `tuple` (`tuple(df.columns)`) or a `list` instead of a `set`. This preserves the crucial deterministic column order while still avoiding O(N) RPC calls during the loop execution.\n")

print("Journal updated.")
