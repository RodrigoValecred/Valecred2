import os
from datetime import datetime

file_path = ".jules/bolt.md"
os.makedirs(os.path.dirname(file_path), exist_ok=True)

date_str = datetime.now().strftime("%Y-%m-%d")
entry = f"""## {date_str} - Testing Mock Challenges with PySpark Broadcast Joins
**Learning:** Adding PySpark structural functions like `broadcast()` to DataFrame method chains inside dynamically extracted functions (using `exec()`) can severely break existing mock setups. The original mock chains (`MagicMock().join().drop()`) fail because `broadcast` expects a recognizable object, and its injection raises `NameError` if it isn't passed through `exec_globals`.
**Action:** When adding structural optimizers like `broadcast()` to code tested via `exec()`, explicitly inject a passthrough lambda (e.g., `'broadcast': lambda x: x`) into the test's `exec_globals` to ensure the mock DataFrame chaining remains intact without throwing structural errors.
"""

with open(file_path, "a") as f:
    f.write("\n" + entry)

print("Updated bolt.md")
import re

def update_bolt_journal():
    filepath = '.jules/bolt.md'
    with open(filepath, 'r') as f:
        content = f.read()

    new_entry = """
## 2024-05-28 - PySpark DataFrame Columns Access Overhead in Loops
**Learning:** In PySpark, calling `df.columns` inside a loop (e.g., when iterating through a large list of target columns to resolve or rename) is extremely inefficient because it triggers a remote procedure call (RPC) to the driver node on every iteration to fetch the schema metadata.
**Action:** Always cache the DataFrame columns into a local Python set before the loop using `cols_set = set(df.columns)`. This reduces the N remote RPC calls to a single call and provides fast O(1) lookups during the iteration, significantly decreasing loop execution time.
"""
    if "PySpark DataFrame Columns Access Overhead in Loops" not in content:
        with open(filepath, 'a') as f:
            f.write(new_entry)

if __name__ == "__main__":
    update_bolt_journal()
