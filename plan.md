1. **Optimize HHI Calculation in Curadoria_Gold**
   - In `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Curadoria_Gold.Notebook/notebook-content.py`, the HHI calculations for cedente and sacado currently use `.collect()[0][0]` multiple times.
   - I will replace `.collect()[0][0]` with `.first()[0]`.
   - The rationale is exactly in Tensor's memory: "🧠 Tensor: Substituir .collect()[0][0] por .first()[0] para preservar predicate pushdown e evitar materialização de lista".
   - I'll add the required Tensor comments for the optimization, matching the agent personality and exact headers (`💡 What`, `🎯 Why`, `📊 Impact`, `🔬 Measurement`).

2. **Pre-commit checks**
   - Call `pre_commit_instructions` to ensure proper testing, verification, review, and reflection are done before committing.
   - Run pytest and ensure tests pass.

3. **Submit the optimization**
   - Commit the changes and submit the code using the format "🧠 Tensor: [optimization name]".
