# Inventário de Ativos de Dados

Este documento fornece um inventário detalhado de todos os ativos de dados no projeto VALECRED, incluindo Dataflows, Notebooks, Lakehouses e Warehouses.

## Data Warehouses

### WH_Gold.Warehouse
- **Descrição:** O data warehouse principal para a camada Gold. Armazena os dados finais, agregados e transformados, prontos para business intelligence e analytics.

## Lakehouses

### LH_Bronze.Lakehouse
- **Descrição:** A zona de aterrissagem para dados brutos de várias fontes.

### LH_Gold.Lakehouse
- **Descrição:** Armazena dados curados que foram modelados para domínios de negócios específicos.

### LH_Silver.Lakehouse
- **Descrição:** Uma camada intermediária que armazena dados limpos, padronizados e enriquecidos da camada Bronze.

## Dataflows

### DF_Dim_Clientes_Gold.Dataflow
- **Descrição:** Cria a tabela de dimensão de clientes.
- **Origem:** `LH_Silver` (`staging_clientes`, `staging_cad_geral`)
- **Destino:** `WH_Gold` (`dim_clientes`)
- **Transformações:** Junta e desduplica dados de clientes.

### DF_Dim_Empresas_Gold.Dataflow
- **Descrição:** (Vazio)
- **Origem:** -
- **Destino:** -
- **Transformações:** -

### DF_Dim_Gerentes_Gold.Dataflow
- **Descrição:** Cria a tabela de dimensão de gerentes.
- **Origem:** `LH_Silver` (`staging_gerentes`, `staging_cad_geral`, `staging_plataformas`)
- **Destino:** `WH_Gold` (`dim_gerentes`)
- **Transformações:** Junta dados de gerentes, cadastro geral e plataformas.

### DF_Dim_Gerentes_Silver.Dataflow
- **Descrição:** (Descrição ausente)
- **Origem:** (Não especificado)
- **Destino:** (Não especificado)
- **Transformações:** (Não especificado)

### DF_Dim_Plataformas_Silver.Dataflow
- **Descrição:** (Descrição ausente)
- **Origem:** (Não especificado)
- **Destino:** (Não especificado)
- **Transformações:** (Não especificado)

### DF_Dim_Tipo_Cobranca.Dataflow
- **Descrição:** (Descrição ausente)
- **Origem:** (Não especificado)
- **Destino:** (Não especificado)
- **Transformações:** (Não especificado)

### DF_Dim_Usuarios_Silver.Dataflow
- **Descrição:** (Descrição ausente)
- **Origem:** (Não especificado)
- **Destino:** (Não especificado)
- **Transformações:** (Não especificado)

### DF_Fact_Checagem.Dataflow
- **Descrição:** (Descrição ausente)
- **Origem:** (Não especificado)
- **Destino:** (Não especificado)
- **Transformações:** (Não especificado)

### DF_Fato_Operacoes_Gold.Dataflow
- **Descrição:** Cria a tabela de fatos principal de operações.
- **Origem:** `LH_Silver` (`staging_titulos`, `staging_tac_m`, `staging_operacoes`)
- **Destino:** `WH_Gold` (`fato_operacoes`, `fato_operacoes_recompra`)
- **Transformações:** Agrega dados de títulos, filtra operações e junta com outras tabelas para criar uma tabela de fatos abrangente.

### DF_Fato_Operacoes_Silver.Dataflow
- **Descrição:** (Descrição ausente)
- **Origem:** (Não especificado)
- **Destino:** (Não especificado)
- **Transformações:** (Não especificado)

### DF_Metas_Gold.Dataflow
- **Descrição:** Cria a tabela de fatos de metas.
- **Origem:** `LH_Silver` (`sup_metas`, `dim_calendario`)
- **Destino:** `WH_Gold` (`fato_metas`)
- **Transformações:** Junta dados de metas com a dimensão de calendário.

### DF_Plataforma_Gold.Dataflow
- **Descrição:** (Vazio)
- **Origem:** -
- **Destino:** -
- **Transformações:** -

### DF_Preparacao_Silver.Dataflow
- **Descrição:** Um grande dataflow que prepara várias tabelas de staging na camada Silver.
- **Origem:** `LH_Bronze` (várias tabelas)
- **Destino:** `LH_Silver` (várias tabelas de staging)
- **Transformações:** Limpa, filtra e padroniza uma ampla gama de dados da camada Bronze.

### DF_Sacado_Gold.Dataflow
- **Descrição:** Cria a tabela de dimensão "sacado".
- **Origem:** `LH_Bronze` (`tab_titulos`, `cad_geral_pf_pj`, `cad_enderecos`)
- **Destino:** `LH_Gold` (`dim_sacado`)
- **Transformações:** Extrai e limpa informações do sacado dos títulos e do cadastro geral.

### DF_TACM_Gold.Dataflow
- **Descrição:** Cria a tabela de fatos "TAC M".
- **Origem:** `LH_Silver` (`dim_usuario`, `staging_tac_m`, `staging_operacoes_limpa`)
- **Destino:** `WH_Gold` (`fato_tac_m`)
- **Transformações:** Junta dados de TAC M com tabelas de usuários e operações.

### DF_Titulo_Gold.Dataflow
- **Descrição:** Cria a tabela de dimensão de títulos.
- **Origem:** `LH_Bronze` (`tab_titulos`)
- **Destino:** `LH_Gold` (`dim_titulo`)
- **Transformações:** Limpa e prepara dados de títulos.

### DF_Usuario_Gold.Dataflow
- **Descrição:** Cria a tabela de dimensão de usuários.
- **Origem:** `LH_Bronze` (`cad_usuarios`)
- **Destino:** `LH_Gold` (`dim_usuario`)
- **Transformações:** Limpa e prepara dados de usuários.

## Notebooks

### 01-Treino_Risco_Semanal.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### KPI_DA_TV.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### ML_Gerador_Score_Risco.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### ML_Previsao_Inadimplencia_2025.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Analise_Cliente_Especifico.Notebook
- **Descrição:** Realiza uma análise histórica de um cliente específico.
- **Entrada:** `LH_Silver` (`staging_titulos_limpa`, `staging_operacoes_limpa`, `dim_cliente`, `staging_cad_geral_limpa`)
- **Saída:** Análise efêmera (sem tabela de saída)
- **Passos de Processamento:** Junta várias tabelas Silver, aplica filtros de negócios e cria uma variável `TARGET` para análise.

### NB_Analise_Cluster_Clientes.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Analise_Safra_Gerentes.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Analise_Titulos_Juridicos.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Analyze_FIDC_Performance.Notebook
- **Descrição:** Analisa o desempenho mensal dos FIDCs.
- **Entrada:** `LH_Bronze` (`cvm_fidc_informe_mensal`)
- **Saída:** `LH_Gold` (`analise_fidc_performance_mensal`)
- **Passos de Processamento:** Filtra dados para um período específico, calcula a variação mensal no Patrimônio Líquido e salva o resultado.

### NB_Calendario_Gold.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Curadoria_Gold.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Diagnostico_Juridico.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Extract_Bronze_Receita_Federal_Full.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Extrai_Observacoes_Contratos.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Fechamento_Prorrogacao_Mensal.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Generic_Silver.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Gera_Relatorio_Diario_Clientes.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Gold_Carteira_Valor_Diario.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Gold_Cockpit_KPIs.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Gold_Dim_Danfe.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Gold_Dim_Empresas.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Gold_Dim_Gerentes.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Gold_Dim_Limites.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Gold_Dim_Produtos.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Gold_Dim_Sacados.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Gold_Empresas_RFB_Target.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Gold_Empresas_RFB_Target.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Gold_Esteira_Propostas.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Gold_Relatorio_Novos_Clientes.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Gold_Relatorio_Produtos_Mensal.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Gold_Risco_Cliente.Notebook
- **Descrição:** Agrega dados de risco por cliente e produto.
- **Entrada:** `LH_Silver` (`staging_titulos`, `staging_operacoes`, `staging_clientes`)
- **Saída:** `LH_Gold` (`risco_cliente_produto`)
- **Passos de Processamento:** Junta tabelas Silver, aplica regras de risco e agrega os dados por cliente e produto.

### NB_Inadimplencia_Mensal.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Load_Bronze_Receita_Federal_Full.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Load_From_CVM.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Load_Silver_From_Manual_Uploads.Notebook
- **Descrição:** Processa arquivos enviados manualmente e os salva como tabelas de dimensão/suporte na camada Silver.
- **Entrada:** `Files/manual_uploads` (vários arquivos Excel e CSV)
- **Saída:** `LH_Silver` (várias tabelas `sup_*`)
- **Passos de Processamento:** Lê, padroniza nomes de colunas e salva cada arquivo como uma tabela Delta.

### NB_PERFIL_RISCO_SACADO.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Prepara_Tabela_Cadastros.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Prepara_Tabela_Contabil.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Prepara_Tabela_Operacoes.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Prepara_Tabela_Produtos.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Prepara_Tabela_Titulos.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Preparacao_Silver.Notebook
- **Descrição:** Um notebook abrangente para preparar a camada Silver.
- **Entrada:** `LH_Bronze` (várias tabelas)
- **Saída:** `LH_Silver` (várias tabelas de staging)
- **Passos de Processamento:** Realiza uma ampla gama de tarefas de limpeza, desduplicação, enriquecimento e transformação de dados.

### NB_Process_Contact_Info.Notebook
- **Descrição:** Limpa, desdobra e desduplica informações de contato.
- **Entrada:** `LH_Bronze` (`cad_geral_pf_pj`)
- **Saída:** `LH_Silver` (`staging_email_limpa`, `staging_telefones_limpa`, `staging_enderecos_limpa`)
- **Passos de Processamento:** Divide informações de contato concatenadas em registros individuais, limpa-os e remove duplicatas.

### NB_Relatorio_Limites_Vencendo.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Report_Novos_Registros_CVM.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Risk_Aggregation.Notebook
- **Descrição:** Agrega métricas de risco para cada cliente.
- **Entrada:** `LH_Silver` (`staging_titulos_limpa`, `staging_operacoes_limpa`, `dim_cliente`, `staging_cad_geral_limpa`)
- **Saída:** `WH_Gold` (`risco_por_cliente`)
- **Passos de Processamento:** Junta tabelas Silver, cria uma variável `TARGET` e agrega métricas de risco por cliente.

### NB_Silver_Carteira_PDD.Notebook
- **Descrição:** Processa dados brutos do controlador para gerar a tabela final `carteira_pdd` na camada Silver.
- **Entrada:** `LH_Bronze` (tabelas `ctrl_*`)
- **Saída:** `LH_Silver` (`carteira_pdd`)
- **Passos de Processamento:** Unifica vários arquivos de origem, calcula faixas de PDD, enriquece os dados e salva a tabela final.

### NB_Silver_Fato_Devolucoes_Cadastro.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Silver_Fato_Devolucoes_Cadastros.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Silver_Pareceres_Keyword.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### NB_Silver_Pareceres_Keywords.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### Processamento_Completo_Clientes.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)

### VAI_Inferencia_Online.Notebook
- **Descrição:** (Descrição ausente)
- **Entrada:** (Não especificado)
- **Saída:** (Não especificado)
- **Passos de Processamento:** (Não especificado)
