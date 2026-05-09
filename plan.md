1.  **Analyze the Bottleneck**: In `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/Relatorios/NB_Gold_Risco_Cliente.Notebook/notebook-content.py`, the code uses `reduce(lambda a, b: a + b, tto_cols)` inside a `.withColumn("RiscoTotal", ...)` statement. As noted in the memory (and `.jules/bolt.md`), using Python `reduce()` to chain operations creates deeply nested Catalyst Logical Plans loaded with `Project` nodes. This causes massive O(N^2) compilation slowdowns.
2.  **Implementation**: Replace the `reduce()` operation with a flat expression that sums the columns. I can use `expr(" + ".join([f"COALESCE(`{c}`, 0)" for c in df_risco_produto.columns if c != 'cod_cliente']))` or simply `sum(*tto_cols)` or `expr(f"({' + '.join([c._jc.toString() for c in tto_cols])})")` but the simplest and most performant way in PySpark is to use a single SQL expression: `expr(' + '.join([f"coalesce(`{c}`, 0)" for c in df_risco_produto.columns if c != 'cod_cliente']))` OR we can just use python built-in `sum` since `sum()` in pyspark doesn't work for multiple columns as easily without building the expression tree properly. Actually, wait. Let's build the expression tree iteratively without `reduce`, but wait, `reduce` on `a + b` where `a` and `b` are `Column` objects literally builds an abstract syntax tree (AST) `(col(A) + col(B)) + col(C)`. This is fine for 5 columns (`produtos_cliente = ['NO', 'FC', 'CM', 'RN', 'GR']`), it's just 5 additions. The issue in memory says "deeply chained `.withColumn` statements", but here it's ONE `withColumn` with a `reduce` building the expression. Wait, memory says: "Avoid using `reduce` with lambda functions to chain `withColumn`. Instead, generate a list of PySpark expressions (`[upper(col(c)).alias(c) if c in cols else col(c) for c in df.columns]`) and unpack it into a single `df.select(*expr_list)`". Here, the `reduce` is used to build the expression, not chain `withColumn` calls. Let me double check the codebase for other `reduce` usages.

Wait, let's look at `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Silver_Carteira_PDD.Notebook/notebook-content.py`:
`df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), dfs)`
This loops unionByName. It creates a nested tree of Unions.

Wait! What about `tests/test_gold_relatorio_novos_clientes.py`? That's just a test.
What if the performance bottleneck is exactly what `bolt.md` says:
"2025-05-18 - PySpark Eager Evaluation due to Log count() in Joins"
Wait, did I see a `.count()` in `NB_Gold_Risco_Cliente.Notebook`? No.
Let's check `NB_Gold_Risco_Cliente.Notebook` for `broadcast()`.
It does:
`df_mestra_spark = df_mestra_spark.join(df_cedentes_deduplicado.select("cod_cliente", "cpf_cnpj"), on="cod_cliente", how="left")`
`df_cedentes` is `LH_Silver.staging_clientes`. Is it a small dimension table?
"In PySpark notebooks, when joining large fact tables (like `df_ops`) with small dimension DataFrames (like `df_usuarios` aliases, `df_motivos`, or `df_gerentes_enrich`), Catalyst optimizer might not automatically trigger BroadcastHashJoins."
`df_cedentes` is small dimension? Let's check `df_cedentes`.
Memory: "In PySpark joins, explicitly use `pyspark.sql.functions.broadcast()` for small dimension tables (like `dim_cliente`) to avoid costly network shuffles (converting SortMergeJoin to BroadcastHashJoin). However, NEVER broadcast large staging tables (like `staging_cad_geral_limpa` or `df_clientes_staging`) to avoid `OutOfMemoryError`; rely on Adaptive Query Execution (AQE) for large tables."
Ah! `df_cedentes` is loaded from `LH_Silver.staging_clientes`, which is a large staging table. So we shouldn't broadcast it.

Let's check `NB_Gold_Risco_Cliente.Notebook` again.
Is there any multiple action on same dataframe?
`df_risco_produto = df_risco_aberto.groupBy("cod_cliente").pivot("tto_operacao").agg(sum("valor_devido")).na.fill(0)`
Wait, `.pivot()` is notoriously slow if the pivot values are not explicitly provided!
"In PySpark, executing multiple aggregation actions... trigger separate Spark jobs..."
In `pivot("tto_operacao")` without passing the explicit list of products, PySpark has to do a full pass over the dataset to find the distinct values of `tto_operacao`!
Ah!
`df_risco_produto = df_risco_aberto.groupBy("cod_cliente").pivot("tto_operacao").agg(sum("valor_devido")).na.fill(0)`
Wait, the notebook previously does:
`produtos_cliente = ['NO', 'FC', 'CM', 'RN', 'GR']`
`df_risco_aberto = df_risco_aberto.filter(col('tto_operacao').isin(produtos_cliente))`
So we KNOW the exact values `tto_operacao` can take!
But `pivot("tto_operacao")` doesn't know that. It will trigger a Spark job to compute `df_risco_aberto.select("tto_operacao").distinct().collect()` to get the columns!
If we provide `pivot("tto_operacao", produtos_cliente)`, we save a complete, expensive full-table scan and DAG materialization!
This is a HUGE performance win!
