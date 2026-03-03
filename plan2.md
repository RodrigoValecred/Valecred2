Let's double check `process_operacoes_stream` logic.
```python
    df_titulos_dates = df_titulos.join(df_ops.select("cod_operacao", "data_deferimento"), "cod_operacao", "inner") \
        .withColumn("prazo_original_dias", datediff(col("vencimento"), col("data_deferimento"))) \
        .withColumn("valor_vezes_prazo_original", col("valor") * col("prazo_original_dias"))
```
I need to change it to:
```python
    df_titulos_dates = df_titulos.join(df_ops.select("cod_operacao", "data_aceite"), "cod_operacao", "inner") \
        .withColumn("prazo_original_dias", datediff(col("vencimento"), col("data_aceite"))) \
        .withColumn("valor_vezes_prazo_original", col("valor") * col("prazo_original_dias"))
```
Wait, the user wants `floating`. "trazer o campo floating da operação, é o número médio de dias acrescido ao prazo médio de títulos. para conferir o cálculo."
So I just need to add `floating` to `df_ops.select` and also include it in the `groupBy`. Wait, `floating` doesn't need to be recalculated, it comes directly from the operations table.
So `floating` should be selected from `fato_operacoes`, kept in the stream output, and mapped in the final `select()`.

Let's do the exact modifications and save them.
