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

## 2025-07-28 - Styled DataFrames in Notebooks
**Learning:** Raw DataFrame output is hard to scan for anomalies. Applying conditional formatting (colors) and currency masks via `df.style` transforms a data dump into an actionable dashboard for analysts.
**Action:** Always wrap key reporting DataFrames in a `style_...` function that highlights critical thresholds (e.g., utilization > 100%) and formats currency.

## 2026-02-24 - Temporal Context in Dashboards
**Learning:** Displaying limits/metrics without their expiry/validity date is dangerous. Adding time context (days remaining, expired status) alongside financial metrics drastically improves risk assessment.
**Action:** Always pair financial limits/targets with their validity period in reports.

## 2026-02-28 - Progress Bar Clamping Output
**Learning:** When building visual indicators like text-based progress bars, it's not enough to clamp values just for the visual layout width. You must also clamp the numeric output rendered to the user to avoid contradictory UX (like a 100% full bar that says `110%`).
**Action:** Always ensure that calculations dictating visual limits are consistently applied to the accompanying descriptive string output.

## 2026-02-28 - Test Alignment with UI
**Learning:** When improving UI formats (like changing date formats from YYYY-MM-DD to DD/MM/YYYY), the corresponding tests must also be updated. UI components and their validation layers are closely coupled.
**Action:** Always verify that string/format assertions in tests match the expected UI changes to prevent false positives/negatives in UX testing.

## 2026-02-28 - Progress Bar Clamping
**Learning:** When building visual indicators like text-based progress bars, inputs (like percentages) must be strictly clamped before calculation. Failing to do so for negative values or percentages over 100% can break layout widths and create confusing UI states.
**Action:** Always clamp mathematical inputs that determine visual widths to a 0-100% range before calculating their display lengths.

## 2026-03-04 - Terminal Dashboard Enumerations
**Learning:** When displaying status breakdowns (like Deferido, Indeferido) in text outputs, pairing the status with distinct emojis (✅, ❌, 🔄) significantly improves scanability for operators reading batch logs.
**Action:** Always add categorized summaries with distinct icons at the end of aggregation reports to improve batch log UX.
