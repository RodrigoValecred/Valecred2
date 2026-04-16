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
