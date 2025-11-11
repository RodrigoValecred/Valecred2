# Inventory of Data Assets

This document provides a detailed inventory of all data assets in the VALECRED project, including Dataflows, Notebooks, Lakehouses, and Warehouses.

## Data Warehouses

### WH_Gold.Warehouse
- **Description:** The main data warehouse for the Gold layer. It stores the final, aggregated, and transformed data, ready for business intelligence and analytics.

## Lakehouses

### LH_Bronze.Lakehouse
- **Description:** The landing zone for raw data from various sources.

### LH_Silver.Lakehouse
- **Description:** An intermediate layer that stores cleaned, standardized, and enriched data from the Bronze layer.

### LH_Gold.Lakehouse
- **Description:** Stores curated data that has been modeled for specific business domains.

## Dataflows

### DF_LBFactor.Dataflow
- **Description:** Ingests raw data from an external MySQL database.
- **Source:** MySQL (`lbfactor.tab_titulos`)
- **Destination:** `LH_Bronze`
- **Transformations:** Basic data loading.

### DF_Calendario_Gold.Dataflow
- **Description:** Creates a calendar dimension table.
- **Source:** None (generates data)
- **Destination:** `WH_Gold` (`dim_calendario`)
- **Transformations:** Generates a date range and extracts various date parts.

### DF_Dim_Clientes_Gold.Dataflow
- **Description:** Creates the customer dimension table.
- **Source:** `LH_Silver` (`staging_clientes`, `staging_cad_geral`)
- **Destination:** `WH_Gold` (`dim_clientes`)
- **Transformations:** Joins and deduplicates customer data.

### DF_Dim_Empresas_Gold.Dataflow
- **Description:** (Empty)
- **Source:** -
- **Destination:** -
- **Transformations:** -

### DF_Dim_Gerentes_Gold.Dataflow
- **Description:** Creates the manager dimension table.
- **Source:** `LH_Silver` (`staging_gerentes`, `staging_cad_geral`, `staging_plataformas`)
- **Destination:** `WH_Gold` (`dim_gerentes`)
- **Transformations:** Joins manager, general registration, and platform data.

### DF_Fato_Operacoes_Gold.Dataflow
- **Description:** Creates the main operations fact table.
- **Source:** `LH_Silver` (`staging_titulos`, `staging_tac_m`, `staging_operacoes`)
- **Destination:** `WH_Gold` (`fato_operacoes`, `fato_operacoes_recompra`)
- **Transformations:** Aggregates title data, filters operations, and joins with other tables to create a comprehensive fact table.

### DF_Metas_Gold.Dataflow
- **Description:** Creates the goals fact table.
- **Source:** `LH_Silver` (`sup_metas`, `dim_calendario`)
- **Destination:** `WH_Gold` (`fato_metas`)
- **Transformations:** Joins goals data with the calendar dimension.

### DF_Plataforma_Gold.Dataflow
- **Description:** (Empty)
- **Source:** -
- **Destination:** -
- **Transformations:** -

### DF_Produto_Gold.Dataflow
- **Description:** Creates the product dimension table.
- **Source:** `LH_Silver` (`staging_operacoes_limpa`), `LH_Bronze` (`tab_tipooperacao`, `tab_subtipooperacao`)
- **Destination:** `WH_Gold` (`dim_produto`)
- **Transformations:** Joins and standardizes product information.

### DF_Sacado_Gold.Dataflow
- **Description:** Creates the "sacado" (drawee) dimension table.
- **Source:** `LH_Bronze` (`tab_titulos`, `cad_geral_pf_pj`, `cad_enderecos`)
- **Destination:** `LH_Gold` (`dim_sacado`)
- **Transformations:** Extracts and cleans drawee information from titles and general registration.

### DF_TACM_Gold.Dataflow
- **Description:** Creates the "TAC M" fact table.
- **Source:** `LH_Silver` (`dim_usuario`, `staging_tac_m`, `staging_operacoes_limpa`)
- **Destination:** `WH_Gold` (`fato_tac_m`)
- **Transformations:** Joins TAC M data with user and operations tables.

### DF_Titulo_Gold.Dataflow
- **Description:** Creates the title dimension table.
- **Source:** `LH_Bronze` (`tab_titulos`)
- **Destination:** `LH_Gold` (`dim_titulo`)
- **Transformations:** Cleans and prepares title data.

### DF_Usuario_Gold.Dataflow
- **Description:** Creates the user dimension table.
- **Source:** `LH_Bronze` (`cad_usuarios`)
- **Destination:** `LH_Gold` (`dim_usuario`)
- **Transformations:** Cleans and prepares user data.

### DF_Esteira_de_Propostas_Silver.Dataflow
- **Description:** Loads title confirmation data.
- **Source:** `LH_Bronze` (`tab_titulos_confirmacao`)
- **Destination:** (Not specified)
- **Transformations:** Basic data loading.

### DF_Preparacao_Silver.Dataflow
- **Description:** A large dataflow that prepares multiple staging tables in the Silver layer.
- **Source:** `LH_Bronze` (multiple tables)
- **Destination:** `LH_Silver` (multiple staging tables)
- **Transformations:** Cleans, filters, and standardizes a wide range of data from the Bronze layer.

## Notebooks

### NB_Analise_Cliente_Especifico.Notebook
- **Description:** Performs a historical analysis of a specific client.
- **Input:** `LH_Silver` (`staging_titulos_limpa`, `staging_operacoes_limpa`, `dim_cliente`, `staging_cad_geral_limpa`)
- **Output:** Ephemeral analysis (no output table)
- **Processing Steps:** Joins several Silver tables, applies business filters, and creates a `TARGET` variable for analysis.

### NB_Analyze_FIDC_Performance.Notebook
- **Description:** Analyzes the monthly performance of FIDCs.
- **Input:** `LH_Bronze` (`cvm_fidc_informe_mensal`)
- **Output:** `LH_Gold` (`analise_fidc_performance_mensal`)
- **Processing Steps:** Filters data for a specific period, calculates the monthly variation in Net Equity, and saves the result.

### NB_Build_Bridge_Cliente_Gerente.Notebook
- **Description:** Creates a historical bridge table that maps the relationship between clients and managers.
- **Input:** `LH_Bronze` (`rlc_brokers_clientes_historico`, `rlc_brokers_clientes`)
- **Output:** `LH_Silver` (`bridge_cliente_gerente`)
- **Processing Steps:** Unifies historical and current data, cleans it, and calculates the start and end dates for each relationship.

### NB_Gold_Risco_Cliente.Notebook
- **Description:** Aggregates risk data by client and product.
- **Input:** `LH_Silver` (`staging_titulos`, `staging_operacoes`, `staging_clientes`)
- **Output:** `LH_Gold` (`risco_cliente_produto`)
- **Processing Steps:** Joins Silver tables, applies risk rules, and aggregates the data by client and product.

### NB_Load_Silver_From_Manual_Uploads.Notebook
- **Description:** Processes manually uploaded files and saves them as dimension/support tables in the Silver layer.
- **Input:** `Files/manual_uploads` (various Excel and CSV files)
- **Output:** `LH_Silver` (multiple `sup_*` tables)
- **Processing Steps:** Reads, standardizes column names, and saves each file as a Delta table.

### NB_Preparacao_Silver.Notebook
- **Description:** A comprehensive notebook for preparing the Silver layer.
- **Input:** `LH_Bronze` (multiple tables)
- **Output:** `LH_Silver` (multiple staging tables)
- **Processing Steps:** Performs a wide range of data cleaning, deduplication, enrichment, and transformation tasks.

### NB_Process_Contact_Info.Notebook
- **Description:** Cleans, unfolds, and deduplicates contact information.
- **Input:** `LH_Bronze` (`cad_geral_pf_pj`)
- **Output:** `LH_Silver` (`staging_email_limpa`, `staging_telefones_limpa`, `staging_enderecos_limpa`)
- **Processing Steps:** Splits concatenated contact information into individual records, cleans them, and removes duplicates.

### NB_Risk_Aggregation.Notebook
- **Description:** Aggregates risk metrics for each client.
- **Input:** `LH_Silver` (`staging_titulos_limpa`, `staging_operacoes_limpa`, `dim_cliente`, `staging_cad_geral_limpa`)
- **Output:** `WH_Gold` (`risco_por_cliente`)
- **Processing Steps:** Joins Silver tables, creates a `TARGET` variable, and aggregates risk metrics by client.

### NB_Silver_Carteira_PDD.Notebook
- **Description:** Processes raw controller data to generate the final `carteira_pdd` table in the Silver layer.
- **Input:** `LH_Bronze` (`ctrl_*` tables)
- **Output:** `LH_Silver` (`carteira_pdd`)
- **Processing Steps:** Unifies multiple source files, calculates PDD ranges, enriches the data, and saves the final table.
