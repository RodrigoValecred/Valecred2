# Data Lineage

This document outlines the data lineage of the VALECRED project, tracing the flow of data from its sources to its final destinations.

## 1. Bronze Layer Ingestion

### DF_LBFactor.Dataflow
- **Source:** MySQL Database (`lbfactor.tab_titulos`)
- **Destination:** `LH_Bronze`
- **Description:** This is the initial entry point for raw data into the data platform.

### Manual Uploads
- **Source:** Manual file uploads (Excel and CSVs) in `Files/manual_uploads`
- **Destination:** `LH_Silver` (via `NB_Load_Silver_From_Manual_Uploads.Notebook`)
- **Description:** Various supplementary data files are loaded manually and then processed by a dedicated notebook.

## 2. Silver Layer Preparation

### NB_Preparacao_Silver.Notebook
- **Source:** `LH_Bronze` (multiple tables: `tab_titulos`, `cad_clientes`, `cad_geral_pf_pj`, etc.)
- **Destination:** `LH_Silver` (multiple staging tables: `staging_titulos_limpa`, `staging_clientes_limpa`, etc.)
- **Description:** This notebook is a key part of the data preparation process. It takes raw data from the Bronze layer, applies a series of cleaning and business logic transformations, and saves the results as staging tables in the Silver layer.

### NB_Load_Silver_From_Manual_Uploads.Notebook
- **Source:** `Files/manual_uploads`
- **Destination:** `LH_Silver` (multiple `sup_*` tables)
- **Description:** This notebook processes manually uploaded files and saves them as dimension/support tables in the Silver layer.

### NB_Build_Bridge_Cliente_Gerente.Notebook
- **Source:** `LH_Bronze` (`rlc_brokers_clientes_historico`, `rlc_brokers_clientes`)
- **Destination:** `LH_Silver` (`bridge_cliente_gerente`)
- **Description:** Creates a historical bridge table that maps the relationship between clients and managers.

### NB_Process_Contact_Info.Notebook
- **Source:** `LH_Bronze` (`cad_geral_pf_pj`)
- **Destination:** `LH_Silver` (`staging_email_limpa`, `staging_telefones_limpa`, `staging_enderecos_limpa`)
- **Description:** Cleans, unfolds, and deduplicates contact information.

### NB_Prepara_Tabela_Contabil.Notebook
- **Source:** `LH_Bronze` (`tab_lancamentos_contabeis`)
- **Destination:** `LH_Silver` (`staging_lancamentos_contabeis`)
- **Description:** Processes accounting entries, standardizing column names and applying incremental loading logic.

### NB_Silver_Carteira_PDD.Notebook
- **Source:** `LH_Bronze` (`ctrl_*` tables)
- **Destination:** `LH_Silver` (`carteira_pdd`)
- **Description:** Processes raw controller data to generate the final `carteira_pdd` table in the Silver layer.

## 3. Gold Layer - Dimensions and Facts

### DF_Dim_Clientes_Gold.Dataflow
- **Source:** `LH_Silver` (`staging_clientes`, `staging_cad_geral`)
- **Destination:** `WH_Gold` (`dim_clientes`)
- **Description:** Creates the customer dimension table in the Gold Warehouse.

### DF_Dim_Gerentes_Gold.Dataflow
- **Source:** `LH_Silver` (`staging_gerentes`, `staging_cad_geral`, `staging_plataformas`)
- **Destination:** `WH_Gold` (`dim_gerentes`)
- **Description:** Creates the manager dimension table.

### DF_Produto_Gold.Dataflow
- **Source:** `LH_Silver` (`staging_operacoes_limpa`), `LH_Bronze` (`tab_tipooperacao`, `tab_subtipooperacao`)
- **Destination:** `WH_Gold` (`dim_produto`)
- **Description:** Creates the product dimension table.

### DF_Sacado_Gold.Dataflow
- **Source:** `LH_Bronze` (`tab_titulos`, `cad_geral_pf_pj`, `cad_enderecos`)
- **Destination:** `LH_Gold` (`dim_sacado`)
- **Description:** Creates the "sacado" (drawee) dimension table.

### DF_Titulo_Gold.Dataflow
- **Source:** `LH_Bronze` (`tab_titulos`)
- **Destination:** `LH_Gold` (`dim_titulo`)
- **Description:** Creates the title dimension table.

### DF_Usuario_Gold.Dataflow
- **Source:** `LH_Bronze` (`cad_usuarios`)
- **Destination:** `LH_Gold` (`dim_usuario`)
- **Description:** Creates the user dimension table.

### DF_Fato_Operacoes_Gold.Dataflow
- **Source:** `LH_Silver` (`staging_titulos`, `staging_tac_m`, `staging_operacoes`)
- **Destination:** `WH_Gold` (`fato_operacoes`, `fato_operacoes_recompra`)
- **Description:** Creates the main operations fact table.

### DF_Metas_Gold.Dataflow
- **Source:** `LH_Silver` (`sup_metas`, `dim_calendario`)
- **Destination:** `WH_Gold` (`fato_metas`)
- **Description:** Creates the goals fact table.

### DF_TACM_Gold.Dataflow
- **Source:** `LH_Silver` (`dim_usuario`, `staging_tac_m`, `staging_operacoes_limpa`)
- **Destination:** `WH_Gold` (`fato_tac_m`)
- **Description:** Creates the "TAC M" fact table.

## 4. Gold Layer - Analysis

### NB_Gold_Risco_Cliente.Notebook
- **Source:** `LH_Silver` (`staging_titulos`, `staging_operacoes`, `staging_clientes`)
- **Destination:** `LH_Gold` (`risco_cliente_produto`)
- **Description:** Aggregates risk data by client and product.

### NB_Risk_Aggregation.Notebook
- **Source:** `LH_Silver` (`staging_titulos_limpa`, `staging_operacoes_limpa`, `dim_cliente`, `staging_cad_geral_limpa`)
- **Destination:** `WH_Gold` (`risco_por_cliente`)
- **Description:** Aggregates risk metrics for each client.

### NB_Analyze_FIDC_Performance.Notebook
- **Source:** `LH_Bronze` (`cvm_fidc_informe_mensal`)
- **Destination:** `LH_Gold` (`analise_fidc_performance_mensal`)
- **Description:** Analyzes the monthly performance of FIDCs.

## 5. Ad-Hoc Analysis

### NB_Analise_Cliente_Especifico.Notebook
- **Source:** `LH_Silver` (`staging_titulos_limpa`, `staging_operacoes_limpa`, `dim_cliente`, `staging_cad_geral_limpa`)
- **Destination:** None (ephemeral analysis)
- **Description:** Performs a historical analysis of a specific client.
