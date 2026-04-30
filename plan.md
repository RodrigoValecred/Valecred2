1.  *Optimize `process_pareceres_clientes_esteira` in `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Prepara_Tabela_Cadastros.Notebook/notebook-content.py`*
    -   Use `replace_with_git_merge_diff` to replace the chained `.withColumn` calls when setting `status_anterior`, `data_anterior`, `macroprocesso_anterior`, and `fase_anterior` with a single `.select("*", *expr_list)` operation. This will flatten the Catalyst Execution Plan and reduce compilation overhead.
    -   Use `replace_with_git_merge_diff` to replace the chained `.withColumn` and `.withColumnRenamed` calls when setting `devolucao` and `recebida` (lines 629-637) with `.select("*", *expr_list)`

2.  *Verify changes in `NB_Prepara_Tabela_Cadastros.Notebook/notebook-content.py`*
    -   Run `git diff VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Prepara_Tabela_Cadastros.Notebook/notebook-content.py` to confirm the modifications were applied correctly.

3.  *Optimize logic for `df_final_pareceres` flags block in `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Prepara_Tabela_Operacoes.Notebook/notebook-content.py`*
    -   Use `replace_with_git_merge_diff` to replace the chained `.withColumn` calls when setting `ESCROW`, `ALCADA_SPENCER`, `ALCADA_CAIO`, `ALCADA_DAIANE`, and `IS_LIMITE_PLUS` (lines 531-535) with a single `.select("*", *expr_list)` operation. This avoids sequential `Project` node generation.

4.  *Verify changes in `NB_Prepara_Tabela_Operacoes.Notebook/notebook-content.py`*
    -   Run `git diff VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Prepara_Tabela_Operacoes.Notebook/notebook-content.py` to confirm the modifications were applied correctly.

5.  *Run Unit Tests*
    -   Run tests on the modified files to make sure nothing is broken. I will use the exact bash command `export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64; export _JAVA_OPTIONS="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"; for file in tests/test_*.py; do /home/jules/.local/share/pipx/venvs/pytest/bin/python -m pytest "$file"; done`.

6.  *Complete pre commit steps*
    -   Complete pre-commit steps to ensure proper testing, verification, review, and reflection are done.

7.  *Submit the change*
    -   Submit the change with `submit` using PR title "⚡ Bolt: Flatten withColumn chains in PySpark Silver Notebooks". The PR description will be:
```
💡 What: Replaced deeply chained `.withColumn()` calls in `process_pareceres_clientes_esteira` (Cadastros) and the flags block (Operações) with flattened `.select("*", *expr_list)` operations.
🎯 Why: Iteratively appending columns using `.withColumn()` forces the Spark Catalyst Optimizer to generate deeply nested `Project` nodes in the logical plan. This causes massive O(N^2) compilation slowdowns (the "Plan Explosion" phenomenon) and risks StackOverflowError during DAG evaluation. Flattening to a single `.select()` resolves this overhead.
📊 Impact: Flattens the execution DAG, substantially reducing the Catalyst logical plan compilation and optimization time for these tasks, speeding up execution and significantly lowering driver memory overhead.
🔬 Measurement: Compare Spark UI "SQL" tab compilation times before and after. The physical execution plan will display a single unified `Project` stage instead of multiple sequential `Project` layers.
```
