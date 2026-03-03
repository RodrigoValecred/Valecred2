Wait, in `df_final = df_union.select(...)`, the `.orderBy("mes_referencia", "tipo_produto")` doesn't have `nome_cliente` anymore, which is good.
Is there anything else that expects `cod_cliente`? `granular_cols` array had `"cod_cliente"`, and `df_map_ops` selects `cod_cliente`. `resolve_columns` uses `granular_cols`. So `cod_cliente` is still in the schema before `df_final.select`, but it's not grouped by anymore. Wait, if we don't group by `cod_cliente`, `df_union` will NOT have `cod_cliente`! Let's check `process_prorrogacoes_stream` and `process_mora_stream`.
Oh, in `process_prorrogacoes_stream`:
```python
    df_stream_prorrog = df_prorrog_calc \
        .withColumn("mes_ref", trunc(col("data_inclusao"), "MM")) \
        .groupBy("mes_ref", "cod_operacao", "nbordero", "chave_produto", "nome_plataforma", "data_deferimento") \
```
Wait! I removed `cod_cliente` from `groupBy` in `process_mora_stream` and `process_prorrogacoes_stream`, so `cod_cliente` is lost after the aggregation! This is completely fine since we don't select it in `df_final`. But `granular_cols` has `cod_cliente`. So `resolve_columns` handles `cod_cliente` *before* the aggregation. This works perfectly.

Let's test the logic.
