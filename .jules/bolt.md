## 2024-05-22 - [Optimizing PySpark Data Lineage]
**Learning:** Persisting foreign keys (like `cod_cliente`) in large fact tables (`fato_titulos`) can eliminate expensive downstream joins with other fact tables (`fato_operacoes`), significantly reducing shuffle and compute costs.
**Action:** Always check if a frequently joined column can be denormalized into the target fact table during its creation.

## 2026-02-18 - [Optimizing PySpark Pivots]
**Learning:** Multiple pivot operations on the same grouping key (e.g., one for MAX and one for MIN) double the shuffle cost. They can be combined into a single pivot with multiple aggregations (e.g., `pivot(...).agg(max(...), min(...))`) and then projected into separate DataFrames.
**Action:** Consolidate multiple pivot calls into a single pass whenever possible.
