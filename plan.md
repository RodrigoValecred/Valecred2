1. **Optimize Pandas fillna in loop in `01-Treino_Risco_Semanal.Notebook`**
    - The current code uses a loop to apply `fillna` to specific columns of a Pandas DataFrame:
      ```python
      for col in feature_cols:
          if col in df_pandas.columns:
              df_pandas[col] = df_pandas[col].fillna(0)
      ```
    - This is mentioned in memory as a performance anti-pattern. We should replace it with a single vectorized `.fillna()` call.
    - ```python
      # Replace with:
      fill_dict = {col: 0 for col in feature_cols if col in df_pandas.columns}
      df_pandas.fillna(value=fill_dict, inplace=True)
      ```
2. **Complete pre-commit steps to ensure proper testing, verification, review, and reflection are done.**
3. **Submit PR with standard formatting**
    - "⚡ Bolt: Replace loop with vectorized fillna in 01-Treino_Risco_Semanal"
