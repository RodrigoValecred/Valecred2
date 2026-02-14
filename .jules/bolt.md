## 2024-05-22 - [Optimizing PySpark Data Lineage]
**Learning:** Persisting foreign keys (like `cod_cliente`) in large fact tables (`fato_titulos`) can eliminate expensive downstream joins with other fact tables (`fato_operacoes`), significantly reducing shuffle and compute costs.
**Action:** Always check if a frequently joined column can be denormalized into the target fact table during its creation.

## 2024-05-23 - [Optimizing Dimension Joins with Deterministic Keys]
**Learning:** For date-based surrogate keys (like `sk_data = YYYYMMDD`), calculating the key directly in the fact table using date functions (`date_format`) is significantly faster than joining with a dimension table (`dim_calendario`), as it avoids broadcast/shuffle overhead entirely.
**Action:** Replace joins with deterministic key calculations whenever the key logic is simple and self-contained.
