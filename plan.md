1. **Optimize DataFrame Joins in `ML_Previsao_Inadimplencia_2025.Notebook/notebook-content.py`**
   - The current code performs multiple joins:
     `df_mestra_spark = df_titulos.join(df_operacoes, on="CODOPERACAO", how="inner")`
     `df_mestra_spark = df_mestra_spark.join(df_cedentes, on="CODCLIENTE", how="left")`
     `df_mestra_spark = df_mestra_spark.join(df_cad_geral.select(...).dropDuplicates(...), on="CPFCNPJ", how="left")`
   - Since `df_cedentes` and `df_cad_geral` are dimension tables and likely smaller than the main fact table, we should use `broadcast` to broadcast these small DataFrames during the join.
   - This prevents shuffling of the large fact table across the cluster, which is a major performance bottleneck.

2. **Complete pre-commit steps to ensure proper testing, verification, review, and reflection are done.**
   - Run `pre_commit_instructions` tool.
   - Execute all checks.

3. **Submit the PR**
   - Create PR using `submit` tool with proper titles and formats requested by the prompt.
