## 2025-05-21 - Notebook UX
**Learning:** UX in data engineering contexts often means better logs and summaries. Operators appreciate immediate, readable feedback in notebook outputs rather than raw data dumps.
**Action:** Always add a summary block at the end of data processing scripts to provide a quick health check.

## 2025-06-25 - ASCII Dashboard Alignment
**Learning:** Terminal ASCII dashboards require precise character counting, especially when emojis are involved (display width vs string length). Fixed-width layouts with generous padding are more robust than tight `printf` alignment.
**Action:** Use a helper function or careful manual calculation (W=52, inner=48) for box layouts, and test with actual output.

## 2025-07-02 - Robust ASCII Reporting
**Learning:** Emojis break ASCII box alignment because their visual width (usually 2) often differs from their string length (usually 1), causing misaligned vertical borders.
**Action:** Use a "Horizontal Lines Only" design for content rows in text reports (top/bottom borders are fine, side borders are omitted) to maintain professional alignment without fragility.
## 2025-05-23 - Notebook UX Enhancement
**Learning:** Data engineers appreciate visual feedback too. Adding simple ANSI color coding and ASCII progress bars to notebook outputs transforms a wall of text into an actionable dashboard, even without a frontend.
**Action:** Use `Colors` class and ASCII bars in other critical notebooks (like `NB_Gera_Relatorio_Diario_Clientes`) to standardize the "CLI Dashboard" experience.
