1. **Optimize `VALECRED_DEV/5_Notebooks/ValeCred_Artificial_Intelligence/01-Treino_Risco_Semanal.Notebook/notebook-content.py`**
   - Use `sed` or Python to replace `df_full = df_join_ops.join(` with:
     ```python
     # ⚡ Bolt: Forçar Broadcast Join para Tabela Dimensão
     # 💡 O que: Usado `F.broadcast()` no DataFrame de dimensão ao realizar join.
     # 🎯 Por que: Evita embaralhamento (shuffle) global da rede em joins com tabelas de fatos muito maiores.
     # 📊 Impacto: Diminui drasticamente o uso de I/O de rede e acelera o tempo de compilação da query do Catalyst.
     # 🔬 Measurement: Profiling mostrará remoção do stage de SortMergeJoin no Spark UI.
     df_full = df_join_ops.join(
         F.broadcast(df_produtos),
     ```
   - Make sure to replace `df_produtos,` with `F.broadcast(df_produtos),`.
2. **Execute pre-commit steps**
   - Complete pre-commit steps to ensure proper testing, verification, review, and reflection are done.
3. **Submit the PR**
   - Submit PR with:
     - Branch: `bolt-broadcast-join`
     - Commit Message: `⚡ Bolt: Forçar Broadcast Join para df_produtos`
     - Title: `⚡ Bolt: Forçar Broadcast Join para Tabela Dimensão`
     - Description: `💡 What: Usado \`F.broadcast()\` no DataFrame de dimensão ao realizar join em \`df_produtos\` no \`01-Treino_Risco_Semanal\`.\n🎯 Why: Evita embaralhamento (shuffle) global da rede em joins com tabelas de fatos muito maiores.\n📊 Impact: Diminui drasticamente o uso de I/O de rede e acelera o tempo de compilação da query do Catalyst.\n🔬 Measurement: Profiling mostrará remoção do stage de SortMergeJoin no Spark UI.`
