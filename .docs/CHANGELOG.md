# Scribe's Daily Documentation Sync

## Change Log

### [2026-04-06]

| Component | Path | Description | Change |
| :--- | :--- | :--- | :--- |
| **Notebooks Gold** | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/Relatorios/` | Múltiplos notebooks adicionados (ex: `NB_Gold_Relatorio_Produtos_Mensal.Notebook`, `NB_Inadimplencia_Mensal.Notebook`, `NB_Risk_Aggregation.Notebook`). Foco em agregação e geração de relatórios de risco, clientes e limites. | Added |
| **Notebooks Silver** | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/` | Conjunto massivo de notebooks para preparação, carga e curadoria (ex: `NB_Prepara_Tabela_Cadastros.Notebook`, `NB_Silver_Carteira_PDD.Notebook`, `NB_Generic_Silver.Notebook`). | Added |
| **Notebooks Machine Learning (V.A.I)** | `VALECRED_DEV/5_Notebooks/ValeCred_Artificial_Intelligence/` | Notebooks e Experimentos ML adicionados (ex: `01-Treino_Risco_Semanal.Notebook`, `NB_Analise_Cluster_Clientes.Notebook`, `VAI_Inferencia_Online.Notebook`). | Added |
| **Modelos Preditivos** | `VALECRED_DEV/6_Machine_Learning/` | Notebooks para inferência de risco e inadimplência (ex: `ML_Gerador_Score_Risco.Notebook`, `ML_Previsao_Inadimplencia_2025.Notebook`). | Added |
| **Dashboards / Relatórios** | `VALECRED_DEV/7_Reports/` e `VALECRED_DEV/8_RealTime/` | Relatório PBI (`RP_Dashboards_Inativos.Report`) e scripts KQL / KPI TV adicionados. | Added |
| **Scripts Utilitários** | Raiz do Projeto | Adicionados scripts de automação/benchmark: `benchmark_chunk_size.py`, `generate_inventory.py`, `optimize_hhi.py`, `update_silver_mashup.py`, etc. | Added |
| **Suíte de Testes (Unitários)** | `tests/` | Repositório agora conta com suíte extensiva de testes via pytest validando curadoria gold, lógica silver, ml, security, ui, entre outros (`test_curadoria_gold_*.py`, `test_ml_*.py`, etc). | Added |


### [2026-03-27]

| Component | Path | Description | Change |
| :--- | :--- | :--- | :--- |
| `NB_Analise_Cluster_Clientes.Notebook` | `VALECRED_DEV/5_Notebooks/ValeCred_Artificial_Intelligence/NB_Analise_Cluster_Clientes.Notebook/notebook-content.py` | ⚡ Bolt: Adicionado `.cache()` antes do `count()` para otimizar execução do KMeans. 🧠 Tensor: Aplicadas otimizações de early stopping (`maxIter`, `tol`, `distanceMeasure`) no KMeans. | Changed |
| `generate_inventory.py` | `generate_inventory.py` | ⚡ Bolt: Otimizada a concatenação de strings na função `generate_markdown`. | Changed |
| `NB_Gold_Esteira_Propostas.Notebook` | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Gold_Esteira_Propostas.Notebook/notebook-content.py` | ⚡ Bolt: Substituído `df.count() > 0` por `not df.isEmpty()` para melhorar a performance. | Changed |
| `VAI_Inferencia_Online.Notebook` | `VALECRED_DEV/5_Notebooks/ValeCred_Artificial_Intelligence/VAI_Inferencia_Online.Notebook/notebook-content.py` | Implementada regra de detecção de fraude "Intercia" cruzando grupos econômicos e limites aprovados. Corrigido clamping da barra de progresso. | Changed |
| `NB_Gold_Relatorio_Limites_Especificos.Notebook` | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/Relatorios/NB_Gold_Relatorio_Limites_Especificos.Notebook/notebook-content.py` | Modificada lógica para calcular limites e agregar risco com base na raiz do CNPJ do sacado. | Changed |
| Testes Unitários | `tests/` | 🧪 Adicionados múltiplos testes: `parse_inventory`, tratamento de exceção em `check_should_skip`, cobertura de falhas em `safe_read_table`, validação de `sanitize_column_name` e prevenção de Zip Slip em `safe_extract`. | Added |
| Múltiplos Arquivos | Vários | 🌐 Traduzidos comentários do inglês para o Português Brasileiro conforme regras de estilo. | Changed |

### [2026-03-26]

| Component | Path | Description | Change |
| :--- | :--- | :--- | :--- |
| `PL_Relatorios_Gold_Diaria.DataPipeline` | `VALECRED_DEV/2_Pipelines/PL_Relatorios_Gold_Diaria.DataPipeline/pipeline-content.json` | Creation of daily pipeline for Gold layer reports orchestration. | Added |
| `PL_Relatorios_Gold_Diaria_v1.2.DataPipeline` | `VALECRED_DEV/2_Pipelines/PL_Relatorios_Gold_Diaria_v1.2.DataPipeline/pipeline-content.json` | Creation of a new version of the daily reports pipeline. | Added |
| `NB_Gold_Cockpit_KPIs.Notebook` | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Gold_Cockpit_KPIs.Notebook/item.metadata.json` | Added KPIs Cockpit notebook in the Gold layer. | Added |
| `NB_Gold_Empresas_RFB_Target.Notebook` | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Gold_Empresas_RFB_Target.Notebook/item.metadata.json` | Added RFB target companies notebook in the Gold layer. | Added |
| `NB_Gold_Risco_Sacado.Notebook` | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/Relatorios/NB_Gold_Risco_Sacado.Notebook/notebook-content.py` | Created open risk report by drawee (sacado) and corrected aggregation logic using `valor_devido` instead of `valor`. | Added |
| `NB_Relatorio_Limites_Vencendo.Notebook` | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/Relatorios/NB_Relatorio_Limites_Vencendo.Notebook/notebook-settings.json` | Added expiring limits report notebook. | Added |
| `NB_Curadoria_Gold.Notebook` | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Curadoria_Gold.Notebook/notebook-content.py` | Prevented duplication in risk calculation by applying `dropDuplicates(["cod_operacao"])` and `dropDuplicates(["cod_titulo"])` on fact tables. | Changed |
| `NB_Gold_Carteira_Valor_Diario.Notebook` | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/Relatorios/NB_Gold_Carteira_Valor_Diario.Notebook/notebook-content.py` | ⚡ Bolt: Removed unnecessary `count()` actions that forced full table scans and caused overhead. | Changed |
| `NB_Gold_Relatorio_Limites_Especificos.Notebook` | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/Relatorios/NB_Gold_Relatorio_Limites_Especificos.Notebook/notebook-content.py` | Corrected logic for identifying active titles using `dropDuplicates(["cod_titulo"])`. | Changed |
| `NB_Prepara_Tabela_Produtos.Notebook` | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Prepara_Tabela_Produtos.Notebook/notebook-content.py` | 🧠 Tensor: Optimized PySpark execution plan by replacing a for loop of `withColumn` calls with `withColumns` in product processing. | Changed |
| Multiple Gold Notebooks | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/Relatorios/` | Moved multiple reporting notebooks (`NB_Analyze_FIDC_Performance`, `NB_Gold_Relatorio_Novos_Clientes`, `NB_Gold_Risco_Cliente`, `NB_Inadimplencia_Mensal`, `NB_Risk_Aggregation`, etc.) from the base Gold directory into the `Relatorios/` folder. | Changed |
| `test_tables.py` | `test_tables.py` | Added a new utility script (likely for local testing). | Added |
| `organize2.py` | `organize2.py` | Added a new script to infer notebook dependencies based on read/write patterns. | Added |

### [2026-03-25]

| Component | Path | Description | Change |
| :--- | :--- | :--- | :--- |
| Cluster Missing Diagnosis Script | `tests/diagnose_cluster_missing.py` | Updated the script to use a `full_outer` join when generating features from `df_metrics_pagos` and `df_metrics_risco` to diagnose cluster missing clients. | Changed |
| `DF_Preparacao_Silver.Dataflow` | `VALECRED_DEV/1_Dataflows/Dataflows_Silver/DF_Preparacao_Silver.Dataflow/mashup.pq` | Removed `FLOATING` column extraction. | Removed |
| `VAI_Inferencia_Online.Notebook` | `VALECRED_DEV/5_Notebooks/ValeCred_Artificial_Intelligence/VAI_Inferencia_Online.Notebook/notebook-content.py` | Added functions `create_progress_bar` and `display_terminal_dashboard`. | Added |
| `NB_Curadoria_Gold.Notebook` | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Curadoria_Gold.Notebook/notebook-content.py` | Refactored `df_operacoes_enriquecida` creation using `df_operacoes_enriquecida_blk1` (via `select()`) to group `withColumn` operations. | Changed |
| Consulta de Workspaces | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/testes.Notebook/notebook-content.py` | Criado script para consultar a API do Power BI e ler logs de acesso de workspaces (`myorg/admin/activityevents`). | Added |
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
