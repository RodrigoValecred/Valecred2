## 2024-05-22 - [Optimizing PySpark Data Lineage]
**Learning:** Persisting foreign keys (like `cod_cliente`) in large fact tables (`fato_titulos`) can eliminate expensive downstream joins with other fact tables (`fato_operacoes`), significantly reducing shuffle and compute costs.
**Action:** Always check if a frequently joined column can be denormalized into the target fact table during its creation.
