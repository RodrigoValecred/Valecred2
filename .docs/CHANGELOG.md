# Scribe's Daily Documentation Sync

## Change Log

### Changed

| Component | Path | Description | Change |
| :--- | :--- | :--- | :--- |
| Cluster Missing Diagnosis Script | `tests/diagnose_cluster_missing.py` | Updated the script to use a `full_outer` join when generating features from `df_metrics_pagos` and `df_metrics_risco` to diagnose cluster missing clients. | Changed |
| `DF_Preparacao_Silver.Dataflow` | `VALECRED_DEV/1_Dataflows/Dataflows_Silver/DF_Preparacao_Silver.Dataflow/mashup.pq` | Removed `FLOATING` column extraction. | Removed |
| `VAI_Inferencia_Online.Notebook` | `VALECRED_DEV/5_Notebooks/ValeCred_Artificial_Intelligence/VAI_Inferencia_Online.Notebook/notebook-content.py` | Added functions `create_progress_bar` and `display_terminal_dashboard`. | Added |
| `NB_Curadoria_Gold.Notebook` | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Curadoria_Gold.Notebook/notebook-content.py` | Refactored `df_operacoes_enriquecida` creation using `df_operacoes_enriquecida_blk1` (via `select()`) to group `withColumn` operations. | Changed |
| `testes.Notebook` | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/testes.Notebook/notebook-content.py` | Created script to query Power BI API for workspace logs (`myorg/admin/activityevents`). | Added |
| `tests/conftest.py` | `tests/conftest.py` | Added file to append the project root to `sys.path`. | Added |
| Relatório de Risco Sacado | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/Relatorios/NB_Gold_Risco_Sacado.Notebook/notebook-content.py` | Criado relatório de risco em aberto por sacado na camada Gold. | Added |
| Relatório de Limites Específicos | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/Relatorios/NB_Gold_Relatorio_Limites_Especificos.Notebook/notebook-content.py` | Adicionado notebook para geração do relatório de limites específicos por produto. | Added |
| Relatório de Dashboards Inativos | `VALECRED_DEV/7_Reports/RP_Dashboards_Inativos.Report/` | Criado relatório Power BI para rastrear painéis não utilizados. | Added |
| Consulta de Workspaces | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/testes.Notebook/notebook-content.py` | Atualizado notebook para realizar cruzamento de logs de acesso do Power BI com a tabela de inventário. | Changed |
| Download Receita Federal | `VALECRED_DEV/5_Notebooks/Dados_Externos/Receita Federal/NB_Load_Bronze_Receita_Federal_Full.Notebook/notebook-content.py` | Otimização de performance: aumentado o `chunk_size` de 8KB para 1MB no download de arquivos ZIP. | Changed |
| Barra de Progresso VAI | `VALECRED_DEV/5_Notebooks/ValeCred_Artificial_Intelligence/VAI_Inferencia_Online.Notebook/notebook-content.py` | Corrigido problema de overflow na largura da barra de progresso, limitando os valores entre 0 e 100. | Changed |
| Lógica de Escrow (Curadoria Gold) | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Curadoria_Gold.Notebook/notebook-content.py` | Refatorada a lógica de carregamento de Escrow para a função `get_escrow_data` e implementado cache do dataframe `df_carteira_ativa` para otimizar cálculos de HHI. | Changed |
| Transformações Limites | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Prepara_Tabela_Cadastros.Notebook/notebook-content.py` | Refatoradas múltiplas chamadas encadeadas de `.withColumn` para uma única projeção `.select` na função `process_limites` para evitar plan explosion. | Changed |
| Consolidação withColumns | `VALECRED_DEV/5_Notebooks/ValeCred_Artificial_Intelligence/01-Treino_Risco_Semanal.Notebook/notebook-content.py` | Consolidado um loop for de `.withColumn()` para uma única chamada `.withColumns()` (Spark 3.3+). | Changed |
| Testes Unitários de Escrow | `tests/test_curadoria_gold_escrow.py` | Adicionados testes unitários para a função `get_escrow_data` abordando caminhos de sucesso e fallback (erro). | Added |
| Benchmark de Chunk Size | `benchmark_chunk_size.py` | Script de medição de desempenho criado para avaliar diferentes tamanhos de chunk em downloads. | Added |
