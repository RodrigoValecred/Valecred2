import os
from datetime import date

journal_path = ".jules/bolt.md"
os.makedirs(os.path.dirname(journal_path), exist_ok=True)

entry = f"""
## {date.today()} - Consolidating Sequential Actions in PySpark
**Learning:** Sequential terminal actions (like `.show()`, `.collect()`, and `.first()`) on uncached PySpark DataFrames cause the Spark Catalyst to re-evaluate the DAG entirely, triggering expensive duplicate operations (like `orderBy` global sorts) for every call.
**Action:** When working with PySpark pipelines, consolidate terminal actions whenever possible (e.g., combining them into a single `.limit(N).collect()` call and reading the resulting local array) to prevent multiple full passes over the dataset and reduce calculation overhead.
"""

# Append to file
with open(journal_path, "a") as f:
    f.write(entry)

print("Updated bolt.md")
