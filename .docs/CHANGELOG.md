# Scribe's Daily Documentation Sync

## Change Log

### [2026-04-12]

| Component | Path | Description | Change |
| :--- | :--- | :--- | :--- |
| `NB_Load_From_CVM.Notebook` | `VALECRED_DEV/5_Notebooks/Dados_Externos/CVM/NB_Load_From_CVM.Notebook/notebook-content.py` | ⚡ Bolt: Aumento do `chunk_size` no download de 8KB para 1MB. | Changed |
| `NB_Gold_Relatorio_Limites_Especificos.Notebook` | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/Relatorios/NB_Gold_Relatorio_Limites_Especificos.Notebook/notebook-content.py` | 🧠 Tensor: Uso de `broadcast()` nos DataFrames de dimensão para evitar shuffle nas junções. | Changed |
| `NB_Gold_Relatorio_Produtos_Mensal.Notebook` | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/Relatorios/NB_Gold_Relatorio_Produtos_Mensal.Notebook/notebook-content.py` | 🧠 Tensor: Inserção de `broadcast()` em DataFrames de dimensão menores para otimizar os joins. | Changed |
| `NB_Relatorio_Limites_Vencendo.Notebook` | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/Relatorios/NB_Relatorio_Limites_Vencendo.Notebook/notebook-content.py` | 🧠 Tensor: Uso de `broadcast()` nas dimensões menores para acelerar as junções. | Changed |
| `NB_Extrai_Observacoes_Contratos.Notebook` | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Extrai_Observacoes_Contratos.Notebook/notebook-content.py` | Refatoração das conversões de strings no PySpark utilizando diretamente expressões (`regexp_replace`, `decode`), em vez da cadeia longa de `withColumn`. | Changed |
| `NB_Prepara_Tabela_Contabil.Notebook` | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Prepara_Tabela_Contabil.Notebook/notebook-content.py` | Encapsulamento do passo de gravação com `try/except` para tratamento e registro de falhas. | Changed |
| `VAI_Inferencia_Online.Notebook` | `VALECRED_DEV/5_Notebooks/ValeCred_Artificial_Intelligence/VAI_Inferencia_Online.Notebook/notebook-content.py` | Adicionada flag `alerta_excesso_tranche`, e atualização das regras e descrições no cálculo do XAI (`motivo_expr` e `anomaly_score`). Fix no casting de `mean_val`. | Changed |
| `test_download_and_extract.py` | `tests/test_download_and_extract.py` | 🧪 Adição de testes para utilitários de download e extração. | Added |
| `test_silver_carteira_pdd_safe_load.py` | `tests/test_silver_carteira_pdd_safe_load.py` | 🧪 Adição de testes para a carga de PDD da camada Silver. | Added |

### [2026-04-11]

| Component | Path | Description | Change |
| :--- | :--- | :--- | :--- |
| VAI_Inferencia_Online | `VALECRED_DEV/5_Notebooks/ValeCred_Artificial_Intelligence/VAI_Inferencia_Online.Notebook/notebook-content.py` | fix: prevent 'Desconhecido' XAI motive by safe-guarding null mean values in Z-score calculation | Changed |
| VAI_Inferencia_Online | `VALECRED_DEV/5_Notebooks/ValeCred_Artificial_Intelligence/VAI_Inferencia_Online.Notebook/notebook-content.py` | feat: monitor 'EXCESSO NA TRANCHE' reason in V.A.I | Changed |
| Relatório Limites Específicos (Gold) | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/Relatorios/NB_Gold_Relatorio_Limites_Especificos.Notebook/notebook-content.py` | 🧠 Tensor: Broadcast Joins for Limits Specifics Report | Changed |
| Preparação Tabela Contábil (Silver) | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Prepara_Tabela_Contabil.Notebook/notebook-content.py` | ⚡ Bolt: Fix caching memory leak in Contábil | Changed |
| Extrai Observações Contratos (Silver) | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Extrai_Observacoes_Contratos.Notebook/notebook-content.py` | ⚡ Bolt: Flatten deep PySpark Catalyst logical plan in NB_Extrai_Observacoes_Contratos | Changed |
| Relatório Produtos Mensal & Relatório Limites Vencendo (Gold) | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/Relatorios/NB_Gold_Relatorio_Produtos_Mensal.Notebook/notebook-content.py`, `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/Relatorios/NB_Relatorio_Limites_Vencendo.Notebook/notebook-content.py` | 🧠 Tensor: Optimize dimensions with Broadcast Joins | Changed |
| Testes Unitários | `tests/test_download_and_extract.py` | Add unit tests for download_and_extract function in RFB notebook | Added |
| Testes Unitários | `tests/test_silver_carteira_pdd_safe_load.py` | Add unit tests for safe_load_table function | Added |
| Carga CVM (Bronze) | `VALECRED_DEV/5_Notebooks/Dados_Externos/CVM/NB_Load_From_CVM.Notebook/notebook-content.py` | ⚡ Bolt: Increase chunk size for CVM downloads | Changed |
### [2026-04-10]

| Component | Path | Description | Change |
| :--- | :--- | :--- | :--- |
| V.A.I Inferencia Online | `VALECRED_DEV/5_Notebooks/ValeCred_Artificial_Intelligence/VAI_Inferencia_Online.Notebook/notebook-content.py` | 🧠 Tensor: Substitui Pandas UDF por expressões nativas PySpark SQL (`F.struct`, `F.array_max`, `F.abs`) para calcular Z-scores. Reduz tempo de inferência XAI pela metade. | Changed |
| V.A.I Inferencia Online | `VALECRED_DEV/5_Notebooks/ValeCred_Artificial_Intelligence/VAI_Inferencia_Online.Notebook/notebook-content.py` | ⚡ Tensor: Coleta de métricas em passagem única para evitar múltiplas varreduras completas da tabela. | Changed |
### [2026-04-08]

| Component | Path | Description | Change |
| :--- | :--- | :--- | :--- |
| VAI_Inferencia_Online | `VALECRED_DEV/5_Notebooks/ValeCred_Artificial_Intelligence/VAI_Inferencia_Online.Notebook/notebook-content.py` | Add new flag `is_cedente_novo` and update anomaly message for new clients | Changed |
| VAI_Inferencia_Online | `VALECRED_DEV/5_Notebooks/ValeCred_Artificial_Intelligence/VAI_Inferencia_Online.Notebook/notebook-content.py` | Restrict Intercia Sem Limite alert to 'Normal' products | Changed |
| Power BI API Inventory | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/testes.Notebook/notebook-content.py` | ⚡ Bolt: Refatorada a busca de relatórios por workspace para utilizar `ThreadPoolExecutor`, paralelizando as chamadas de API e reduzindo drasticamente o tempo de execução. | Changed |

| Preparação Tabela Contábil (Silver) | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Prepara_Tabela_Contabil.Notebook/notebook-content.py` | 🧠 Tensor: Adicionado `.cache()` e `.unpersist()` ao DataFrame `df_dedup` (Window function) para evitar re-execução de particionamento e shuffles redundantes. | Changed |
| Carteira de Títulos (Gold) | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Gold_Carteira_Titulos.Notebook/notebook-content.py` | ⚡ Bolt: Caching do dataframe antes do `count()` e `.unpersist()` após para prevenir reavaliação dupla do DAG do Catalyst. | Changed |
| Carga Carteira PDD (Silver) | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Silver_Carteira_PDD.Notebook/notebook-content.py` | ⚡ Bolt: Cache do DataFrame resultante de uniões antes da ação `count()` para evitar re-leitura de múltiplas carteiras. | Changed |
| CSV Data Loading | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Load_Silver_From_Manual_Uploads.Notebook/notebook-content.py` | 🧠 Tensor: Uso do leitor nativo distribuído do PySpark (`spark.read.csv`) em vez de Pandas para evitar OOM; Otimização de renomeação de colunas com `.toDF()`. | Changed |
| Testes Unitários | `tests/test_relatorio_produtos_mensal.py` | 🧪 Adicionados testes unitários abrangentes para o processamento de Mora Mensal. | Added |
### [2026-04-03]

| Component | Path | Description | Change |
| :--- | :--- | :--- | :--- |
| CSV Data Loading | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Load_Silver_From_Manual_Uploads.Notebook/notebook-content.py` | 🧠 Tensor: Optimized manual CSV reading by using Spark native distributed reader (`spark.read.csv`) instead of `pd.read_csv`, preventing driver OOM. | Changed |
| Column Renaming Optimization | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Load_Silver_From_Manual_Uploads.Notebook/notebook-content.py` | ⚡ Bolt: Refactored column sanitization to apply an explicit bulk projection via `df.toDF(*new_columns)` instead of loops with `.withColumnRenamed()` to prevent Catalyst logical plan overhead. | Changed |
| Codebase Comments Translation | Repository wide | The Translator: Translated remaining English comments to pt-BR across notebooks, scripts, and tests using AST/Tokenize, strictly preserving code integrity and technical terms. | Changed |
| PySpark Plan Optimization | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Curadoria_Gold.Notebook/notebook-content.py` (and related) | ⚡ Bolt: Optimized PySpark Catalyst Plan evaluation via `.cache()` for Window deduplication and before `count()` logs to prevent double evaluation. | Changed |
### [2026-04-06]

| Component | Path | Description | Change |
| :--- | :--- | :--- | :--- |
| ML Previsão Inadimplência | `VALECRED_DEV/6_Machine_Learning/ML_Previsao_Inadimplencia_2025.Notebook/notebook-content.py` | 🧠 Tensor: Substituído `df.count()` por `df.isEmpty()` em `df_previsao_spark` para evitar varredura completa (full table scan) ao verificar dados. | Changed |
| Carteira de Títulos (Gold) | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Gold/NB_Gold_Carteira_Titulos.Notebook/notebook-content.py` | ⚡ Bolt: Caching do dataframe (`df_carteira.cache()`) antes do `count()` e `.unpersist()` após escrita para prevenir reavaliação dupla do DAG do Catalyst. | Changed |
| Preparação Tabela Contábil (Silver) | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Prepara_Tabela_Contabil.Notebook/notebook-content.py` | 🧠 Tensor: Adicionado `.cache()` e `.unpersist()` ao DataFrame `df_dedup` resultante de função Window para evitar re-execução do particionamento e shuffle redundantes. | Changed |
| Carga Carteira PDD (Silver) | `VALECRED_DEV/5_Notebooks/Engenharia_de_Dados/Silver/NB_Silver_Carteira_PDD.Notebook/notebook-content.py` | ⚡ Bolt: Cache do DataFrame consolidado antes da ação `count()` no log para evitar re-leitura de múltiplas tabelas da camada Bronze. | Changed |
| Testes Unitários de Mora | `tests/test_relatorio_produtos_mensal.py` | 🧪 Adicionados testes unitários abrangentes para a lógica de processamento do stream de Mora. | Added |
| Inferência Online (V.A.I) | `VALECRED_DEV/5_Notebooks/ValeCred_Artificial_Intelligence/VAI_Inferencia_Online.Notebook/notebook-content.py` | Implementada regra `check_sequential_invoices` para flag de notas sequenciais (risco de fuga) com forçamento de anomalia (-1.0) e adicionado clamping na função `create_progress_bar`. | Changed |
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
