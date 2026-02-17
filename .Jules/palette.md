## 2025-05-21 - Notebook UX
**Learning:** UX in data engineering contexts often means better logs and summaries. Operators appreciate immediate, readable feedback in notebook outputs rather than raw data dumps.
**Action:** Always add a summary block at the end of data processing scripts to provide a quick health check.

## 2025-06-25 - ASCII Dashboard Alignment
**Learning:** Terminal ASCII dashboards require precise character counting, especially when emojis are involved (display width vs string length). Fixed-width layouts with generous padding are more robust than tight `printf` alignment.
**Action:** Use a helper function or careful manual calculation (W=52, inner=48) for box layouts, and test with actual output.
