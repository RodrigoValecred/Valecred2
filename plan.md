1. **Otimizar joins em `NB_Gold_Dim_Limites.Notebook`:**
   - Adicionar a importação de `broadcast` da `pyspark.sql.functions` no arquivo `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Gold_Dim_Limites.Notebook/notebook-content.py` se ainda não estiver presente.
   - Modificar os `joins` para utilizar `broadcast` nas dimensões: `df_clientes_staging`, `df_limites_ep_clientes` (caso a agregação caiba na memória e atue como dimensão na query) ou as outras referenciadas na tabela `df_nomes_clientes`, `df_grupos_prep`.
   - Adicionar o comentário obrigatório seguindo a filosofia do agente `Tensor`.

2. **Otimizar verificação de contagem no `NB_Fechamento_Prorrogacao_Mensal.Notebook`:**
   - Trocar verificação `.count()` na função `display_summary` do `NB_Fechamento_Prorrogacao_Mensal` para usar `.isEmpty()` e `.take(1)` ou similar, conforme a memória "In PySpark, replace `count() > 0` checks used for control flow with `not df.isEmpty()`".
   - *Atenção:* Em `display_summary(df)`, se a verificação for apenas log, podemos otimizar o check `.count()` de verificação vazia no DataFrame original `if df.isEmpty(): print("VAZIO"); return`. O código atual faz `counts = df.groupBy("status_final_prorrogacao").count().collect()`.

3. **Verificar testes e formatar (Pre-commit):**
   - Rodar testes locais.
   - Completar as instruções de pre-commit e atualizar `INVENTORY.md` e os arquivos do `.jules` se necessário.
