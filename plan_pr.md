1. **Remove `data_aceite` from `NB_Curadoria_Gold.Notebook/notebook-content.py`**:
   - Revert `select_fato_operacoes_columns` to remove `col("data_aceite")`.

2. **Revert `data_aceite` back to `data_deferimento` in `NB_Gold_Relatorio_Produtos_Mensal.Notebook/notebook-content.py`**:
   - In `process_operacoes_stream`:
     - Change `datediff(col("vencimento"), col("data_aceite"))` back to `datediff(col("vencimento"), col("data_deferimento"))`.
     - Change the join `df_titulos.join(df_ops.select("cod_operacao", "data_aceite"), ...)` back to `df_titulos.join(df_ops.select("cod_operacao", "data_deferimento"), ...)`.

3. **Reply to the PR comment**: Let the user know the change is reverted.
4. **Run tests**.
5. **Submit changes on the same branch (`fix-relatorio-produtos-operacoes`)**.
