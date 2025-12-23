# Plataforma de Dados Valecred

Este repositório contém o código-fonte da Plataforma de Dados Valecred, uma solução completa de engenharia e análise de dados construída no Microsoft Fabric. A plataforma processa dados brutos de sistemas operacionais, refina-os através de uma arquitetura medalhão e os disponibiliza para usuários finais para aplicações de análise e aprendizado de máquina.

## Visão Geral

O objetivo desta plataforma é centralizar e democratizar o acesso aos dados da Valecred, garantindo qualidade, governança e agilidade. Através de um fluxo de dados bem definido, transformamos dados brutos em insights acionáveis, alimentando desde dashboards de BI até modelos avançados de machine learning para previsão de risco.

## V.A.I. (ValeCred Artificial Intelligence)

A V.A.I. é a inteligência artificial da ValeCred, desenvolvida para potencializar a análise de dados e a tomada de decisão. Seu primeiro trabalho consistiu em uma análise profunda das operações realizadas no último ano, permitindo identificar e mapear o comportamento padrão das transações.

Com base nesse estudo, a V.A.I. monitora as novas operações, identificando anomalias que desviem do padrão estabelecido e classificando-as como **Alto Risco**, permitindo uma atuação proativa da equipe de gestão de risco.
🤖 V.A.I. - Vale Artificial Intelligence
Sistema de Monitoramento Preditivo de Risco para FIDC

📋 Sobre o Projeto
A V.A.I. é uma inteligência artificial desenvolvida para atuar como "Sentinela Digital" nas operações de crédito da ValeCred. Diferente de sistemas tradicionais baseados apenas em regras fixas (ex: "valor > 50k"), a V.A.I. utiliza Machine Learning (Aprendizado Não Supervisionado) para detectar anomalias estatísticas em tempo real.

O sistema analisa o comportamento histórico do sacado e cruza com as variáveis da operação atual (Taxa x Prazo x Exposição) para identificar riscos que passariam despercebidos pelo olho humano.

🚀 Arquitetura da Solução
O projeto foi construído 100% no ecossistema Microsoft Fabric, utilizando uma arquitetura Lakehouse moderna.

Fluxo de Dados:
Ingestão Otimizada (Data Factory): Pipeline com "Portão Lógico". Verifica se há novos IDs de operação na origem antes de disparar o processamento pesado.

Economia: Redução de 90% no tempo de computação ociosa.

Processamento (Spark Notebooks): Limpeza, tipagem e enriquecimento dos dados (Bronze → Gold).

Inteligência (Scikit-Learn + MLflow):

Treino: Re-treinamento semanal automático (Domingos à noite) olhando os últimos 365 dias.

Inferência: Busca dinâmica pelo "Melhor Modelo" via MLflow, garantindo que a IA esteja sempre atualizada sem intervenção manual.

Visualização (Power BI): Dashboard em tempo real instalado em TVs corporativas.

🧠 O Cérebro (Machine Learning)
A V.A.I. utiliza o algoritmo Isolation Forest para detecção de anomalias.

Features Analisadas: Valor do Título, Taxa de Aquisição, Prazo Médio, Exposição Acumulada, Concentração da Operação.

Logica: O modelo isola operações que divergem do padrão matemático do portfólio.

MLOps:

Versionamento de modelos via MLflow.

Pipeline de CI/CD para atualização do "cérebro" da IA.

📊 Dashboard & Monitoramento
A interface visual foi desenhada com identidade "Cyber-Agro" (Dark Mode com Neon Verde/Amarelo) para fácil leitura em TVs a distância.

Funcionalidades Visuais:
Alerta Hierárquico: Ícones de alerta na operação "pai" caso qualquer título "filho" apresente risco.

Tooltip Explicativo: Ao passar o mouse, a V.A.I. explica em linguagem natural o motivo do alerta (ex: "Taxa incompatível com o prazo para este perfil").

Status V.A.I.:

⚠️ ALTO RISCO: Anomalia detectada. Requer análise manual.

✅ NORMAL: Operação dentro dos padrões estatísticos.

<img width="1202" height="661" alt="image" src="https://github.com/user-attachments/assets/1b844dfe-ec9d-4211-aef8-e8b2c22fb52b" />


🛠️ Stack Tecnológica
Orquestração: Azure Data Factory (Pipelines).

Compute: Spark (PySpark) no Microsoft Fabric.

Linguagem: Python (Pandas, Scikit-Learn).

Gerenciamento de Modelo: MLflow.

Armazenamento: Delta Lake (Bronze/Gold).

Frontend: Power BI (DAX avançado para alertas visuais).

📈 Resultados e Impacto
Detecção de Fraude: Identificação de padrões atípicos (ex: alavancagem súbita) antes do desembolso.

Redução de Risco: Monitoramento de 100% das operações, inclusive as de baixo valor (Long tail).

Autonomia: Sistema "Vivo" que aprende semanalmente com novos dados, adaptando-se às mudanças do mercado.

📞 Contato / Consultoria
Desenvolvido por RBO Consultoria em Dados. Especialistas em Engenharia de Dados e IA para Mercado Financeiro.
## Como Começar

Para começar a trabalhar com a plataforma, siga os passos abaixo.

### Pré-requisitos

*   **Acesso ao Microsoft Fabric**: Você precisará de permissões adequadas no workspace da Valecred.
*   **Git**: O Git deve estar instalado e configurado na sua máquina local para clonar o repositório.
*   **Conhecimento em PySpark e SQL**: Familiaridade com essas tecnologias é essencial para o desenvolvimento de notebooks.

### Instalação e Configuração

1.  **Clone o Repositório**:
    ```bash
    git clone <URL_DO_REPOSITORIO>
    cd nome-do-repositorio
    ```
2.  **Sincronize com o Microsoft Fabric**: Conecte seu ambiente de desenvolvimento local ao workspace do Fabric para garantir que as alterações sejam sincronizadas. Siga as [diretrizes oficiais da Microsoft](https://docs.microsoft.com/fabric/git-integration/git-integration-overview) para configurar a integração.
3.  **Explore os Artefatos**: Navegue pelas pastas numeradas para entender a organização dos notebooks e pipelines. Comece pela pasta `5_Notebooks` para entender o fluxo de transformação.

## Estrutura do Repositório

O projeto é organizado em uma estrutura de pastas numeradas que reflete o fluxo de processamento dos dados.

```
/
├── 1_Dataflows/         # [LEGADO] Dataflows antigos (sendo descontinuados)
├── 2_Pipelines/         # Pipelines para orquestração de ponta a ponta
├── 3_Lakehouses/        # Definições dos Lakehouses (Bronze, Silver, Gold)
├── 4_Warehouses/        # Definições dos Warehouses (Gold)
├── 5_Notebooks/         # Notebooks PySpark para transformações complexas e análises
├── 6_Machine_Learning/  # Modelos de ML e notebooks de inferência
├── 7_Dados_Externos/    # Notebooks para ingestão de dados de fontes públicas
└── README.md            # Documentação do projeto
```

## Como Contribuir

Agradecemos o interesse em contribuir com a plataforma! Para garantir a qualidade e a consistência do código, siga os passos abaixo:

1.  **Crie uma Branch**: Nunca trabalhe diretamente na branch `main`. Crie uma nova branch a partir da `main` com um nome descritivo:
    ```bash
    git checkout -b feature/sua-nova-feature
    ```
2.  **Desenvolva e Teste**: Implemente suas alterações e teste-as exaustivamente no ambiente de desenvolvimento do Fabric.
3.  **Documente seu Código**: Adicione comentários claros e, se estiver criando novas funções, inclua docstrings completas.
4.  **Abra um Pull Request (PR)**: Após concluir o desenvolvimento, envie um Pull Request para a branch `main`. Descreva suas alterações detalhadamente e marque um revisor.
5.  **Aguarde a Revisão**: Seu código será revisado. Esteja preparado para fazer ajustes com base no feedback recebido.

## Arquitetura

A plataforma segue uma **Arquitetura Medalhão**, que organiza os dados em camadas Bronze, Prata (Silver) e Ouro (Gold). Essa abordagem garante a qualidade, governança e escalabilidade dos dados.

-   **Camada Bronze (`LH_Bronze`)**: Esta camada armazena dados brutos e inalterados, ingeridos diretamente dos sistemas de origem. Ela serve como a única fonte da verdade e permite a reprodução histórica das transformações de dados.
-   **Camada Prata (`LH_Silver`)**: Esta camada contém dados limpos, validados e enriquecidos. Os dados da camada Bronze são desduplicados, conformados e integrados aqui. Esta camada fornece uma fonte confiável para business intelligence e análises ad-hoc.
-   **Camada Ouro (`LH_Gold` e `WH_Gold`)**: Esta camada armazena dados altamente refinados e agregados. O `LH_Gold` contém tabelas e artefatos de dados avançados, como resultados de modelos de machine learning e agregações especializadas. O `WH_Gold` serve os modelos de dados finais em esquemas estrela, prontos para consumo por ferramentas de análise e relatórios, e também armazena tabelas de controle para processos de ETL/ELT.

### Definição da Camada Ouro: LH_Gold vs. WH_Gold

Para garantir clareza e governança, a camada Ouro é dividida em dois componentes com propósitos distintos: o Lakehouse Gold (`LH_Gold`) e o Data Warehouse Gold (`WH_Gold`).

#### Data Warehouse Gold (`WH_Gold`): A Vitrine de Dados para BI

O `WH_Gold` é projetado para armazenar **dados de negócio, modelados e prontos para consumo**, otimizados para relatórios e análises de Business Intelligence (BI).

**O que salvar aqui:**
*   **Modelos Dimensionais (Star Schema):** Tabelas de fatos e dimensões altamente estruturadas para ferramentas como o Power BI.
    *   *Nota sobre Dimensões:* Tabelas como `dim_calendario`, `dim_clientes` e `dim_produtos` pertencem a esta camada, pois são estruturas finais para análise. Mesmo que sejam utilizadas para cálculos na camada Silver ou em notebooks de curadoria, sua "casa" definitiva é a camada Gold.
*   **Tabelas Agregadas para Dashboards:** Dados pré-calculados que alimentam diretamente relatórios gerenciais.
*   **Tabelas de Controle de Processos (ETL/ELT):** Como a tabela `etl_watermark_control`, que é fundamental para os pipelines incrementais.

> **Nota Importante:** A camada WH_Gold deve ser a única fonte para Datasets do Power BI. Evite criar colunas calculadas no Power BI; privilegie trazer a lógica para o SQL/Spark no Warehouse ou use Medidas DAX centralizadas para garantir que o cálculo de 'Taxa de Inadimplência' seja o mesmo para toda a empresa.

Em resumo, o `WH_Gold` é a "vitrine" de dados para os usuários de negócio.

#### Lakehouse Gold (`LH_Gold`): O Laboratório de Dados para Ciência de Dados

O `LH_Gold` é mais flexível e armazena **dados refinados que não se encaixam em um modelo de BI tradicional**, mas são valiosos para ciência de dados e análises avançadas.

**O que salvar aqui:**
*   **Resultados de Modelos de Machine Learning:** Tabelas com previsões, scores de risco, etc.
*   **Agregações Especializadas:** Tabelas com agregações complexas para análises de nicho que não fazem parte do modelo dimensional principal.
*   **"Features" de Engenharia de Dados:** Tabelas contendo as variáveis criadas para treinar modelos de machine learning.

Em resumo, o `LH_Gold` funciona como um "laboratório" para data scientists e analistas de dados, contendo produtos de dados avançados.

## Componentes do Projeto

Todos os componentes da plataforma são organizados em pastas numeradas para refletir o fluxo de dados e facilitar a navegação.

### 1. Dataflows (`1_Dataflows`)

**⚠️ AVISO DE DESCONTINUAÇÃO:** Os Dataflows (Power Query Online) estão sendo **substituídos integralmente por Notebooks PySpark**.

Embora o repositório ainda possa conter artefatos legados nesta pasta, a estratégia atual e futura da plataforma é utilizar **Notebooks** para todas as camadas de transformação (Silver e Gold). Isso se deve às limitações de performance e quebra de *query folding* encontradas nos Dataflows para as complexas regras de negócio da Valecred.

Não crie novos Dataflows. Utilize os Notebooks existentes ou crie novos Notebooks na pasta `5_Notebooks`.

### 2. Pipelines de Dados (`2_Pipelines`)

O processo de orquestração e ingestão de dados é gerenciado por um conjunto de pipelines modulares. Enquanto os pipelines `PL_Load_Bronze_*` cuidam da extração de dados da origem, os pipelines `PL_Orquestracao_*` gerenciam o fluxo de transformação de ponta a ponta.

-   **`PL_Orquestracao_de_Dados_Incremental`**: Este é o pipeline principal para as operações diárias. Ele orquestra a execução de notebooks para preparar os dados na camada Silver e Gold, e em seguida executa os modelos de machine learning. Ele garante que as transformações ocorram na sequência correta após a chegada de novos dados.
-   **`PL_FastTrack_TV`**: Pipeline de atualização rápida (a cada 10 minutos) que atualiza a tabela `tab_operacoes`. Utiliza uma query otimizada para buscar apenas os dados do dia corrente. Sua principal função é alimentar o BI `TV_KPI_VOP_HOJE`, exibido nas TVs via APK do Power BI, que também possui atualização a cada 10 minutos.
-   **Pipelines de Ingestão (Bronze)**:
    -   **`PL_Load_Bronze_Incremental`**: Responsável pela ingestão incremental diária de dados de um banco de origem **MySQL** para o Lakehouse Bronze. Ele usa uma tabela de watermark para buscar apenas registros novos ou modificados.
    -   **`PL_Load_Bronze_FullOverwrite`**: Projetado para cargas de dados iniciais ou atualizações completas, copiando o conteúdo total das tabelas de origem para a camada Bronze.

### 3. Lakehouses (`3_Lakehouses`)

-   **`LH_Bronze`**: Armazena dados brutos do MySQL e os arquivos de controle dos pipelines.
-   **`LH_Silver`**: Armazena dados limpos e preparados, bem como tabelas de "staging" e ponte cliente-gerente.
-   **`LH_Gold`**: Armazena as tabelas finais de dimensões e fatos (em formato Delta), além de resultados de análises avançadas.

### 4. Warehouses (`4_Warehouses`)

-   **`WH_Gold`**: Um SQL Data Warehouse que serve os modelos de dados finais e curados para relatórios. Ele também hospeda a tabela crítica `etl_watermark_control` que orquestra os pipelines de dados incrementais.

### 5. Notebooks (`5_Notebooks`)

Os notebooks PySpark são agora o padrão oficial para toda a lógica de negócios e transformações de dados da plataforma, substituindo os antigos Dataflows.

#### Notebooks de Transformação (Bronze para Silver)

-   **`NB_Prepara_Tabela_Cadastros`**: (**Carga Full Overwrite**) Processamento de tabelas dimensionais e cadastrais (clientes, geral, telefones, endereços, contratos, bridge, limites, empresas, gerentes, plataformas, status).
-   **`NB_Prepara_Tabela_Titulos`**: (**Carga Incremental**) Processamento da tabela `tab_titulos` e tabelas relacionadas (baixas, protestos, abatimentos, boletos, danfe).
-   **`NB_Prepara_Tabela_Operacoes`**: (**Carga Incremental**) Processamento da tabela `tab_operacoes`, `tab_operacoes_devolucoes` e `tab_operacoes_tarifas_extras`.
-   **`NB_Prepara_Tabela_Contabil`**: (**Carga Incremental/Full**) Processamento da tabela `tab_lancamentos_contabeis` e padronização para a camada Silver.
-   **`NB_Process_Contact_Info`**: Processa e limpa dados de contato, tratando campos com múltiplos valores e salvando-os em tabelas de staging na camada Silver.
-   **`NB_Load_Silver_From_Manual_Uploads`**: Notebook genérico para carga de arquivos manuais (Excel/CSV) armazenados no Bronze para tabelas Silver, com padronização de colunas.
-   **`NB_Silver_Carteira_PDD`**: Processamento específico para a carteira de PDD (Provisão para Devedores Duvidosos).

#### Notebooks de Curadoria e Agregação (Silver para Gold)

-   **`NB_Curadoria_Gold`**: Centraliza a lógica de **enriquecimento e joins**. Consome as tabelas tratadas da Silver (staging) e aplica regras de negócio complexas para gerar as tabelas Fato e Dimensão finais no `LH_Gold`.
-   **`Processamento_Completo_Clientes`**: Um fluxo consolidado (Bronze -> Silver -> Gold) focado na entidade Cliente. Realiza limpeza de cadastro, telefones e endereços, gerando a tabela final `gold_cliente_completo`.
-   **`NB_Calendario_Gold`**: Gera e atualiza a tabela dimensão de calendário (`dim_calendario`), fundamental para análises temporais.
-   **`NB_Gold_Risco_Cliente`**: Cria agregações de risco por cliente, segmentado por produto.
-   **`NB_Risk_Aggregation`**: Calcula métricas históricas de risco (inadimplência, volume) e salva em tabelas agregadas no Gold.
-   **`NB_Relatorio_Limites_Vencendo`**: Gera a tabela `relatorio_limites_vencendo` no Gold, consolidando dados de contratos e clientes para monitoramento de vencimento de limites.

#### Notebooks de Utilidade e Análise

-   **`NB_Generic_Silver`**: Ingestor genérico para carga de tabelas Bronze para Silver com padronização automática e Quality Gate.
-   **`NB_Analise_Cliente_Especifico`**: Ferramenta ad-hoc para investigar o histórico detalhado de um cliente.
-   **`NB_Analyze_FIDC_Performance`**: Notebook para análise de performance do FIDC.
-   **`NB_CERC_Consulta_API`**: Notebook para integração e consulta de dados da API da CERC.

### 6. Machine Learning (`6_Machine_Learning`)

Esta seção detalha os modelos de machine learning da plataforma e os notebooks associados.

#### Modelo de Inadimplência (LightGBM)

**Por que LightGBM?**
Modelos baseados em árvore (Gradient Boosting) são superiores para dados tabulares financeiros, lidando excepcionalmente bem com valores nulos (comuns em cadastros incompletos) e classes desbalanceadas (existem mais bons pagadores do que maus pagadores).

**Impacto de Negócio:**
Redução da PDD (Provisão para Devedores Duvidosos) antecipando títulos podres antes da aquisição ou renovação.

**Ponto de Atenção (Feature Store):**
Atualmente, o notebook `NB_Gold_Risco_Cliente` é responsável por criar as agregações utilizadas pelos modelos. Existe uma iniciativa para formalizar esse processo como uma **Feature Store** nativa no Microsoft Fabric no futuro. O objetivo é garantir que o modelo de treinamento utilize exatamente as mesmas definições de variáveis que o modelo em produção (inferência), mitigando o risco de *training-serving skew*.

#### Notebooks de Aprendizado de Máquina

-   **`ML_Inadimplencia_Aprimorado(1)`**: Notebook principal para o **treinamento** do modelo de classificação (`LightGBM`) que prevê a probabilidade de inadimplência de um título.
-   **`ML_Previsao_Inadimplencia_2025`**: Notebook de **inferência em lote**. Aplica o modelo treinado aos títulos em aberto.
-   **`ML_Gerador_Score_Risco`**: Ferramenta interativa para obter score de risco em tempo real para um cliente.

### 7. Dados Externos (`7_Dados_Externos`)

Notebooks responsáveis por ingerir dados de fontes externas para a camada Bronze.

-   **`NB_Load_Bronze_From_BrasilIO`**: Dados de empresas e sócios do Brasil.IO.
-   **`NB_Load_Bronze_From_SERPRO`**: Dados de licitações do Portal da Transparência (filtro SERPRO).
-   **`NB_Load_From_CVM`**: Ingestão dos informes mensais de FIDC da CVM (substitui referência antiga).
-   **`NB_Report_Novos_Registros_CVM`**: Gera relatórios de novos registros FIDC na CVM.
-   **`NB_Load_Bronze_Receita_Federal_Full`**: Processamento dos dados públicos de CNPJ da Receita Federal.

### 8. RealTime (`8_RealTime`)

Componentes focados em processamento de baixa latência.

-   **`KPI_DA_TV.Notebook`**: Alimenta o relatório `TV_KPI_VOP_HOJE` em tempo real.

## Fluxo de Dados de Ponta a Ponta

O fluxo de dados principal é orquestrado pelo pipeline `PL_Orquestracao_de_Dados_Incremental` e segue a arquitetura medalhão.

1.  **Ingestão (Bronze)**: O processo começa com o pipeline `PL_Load_Bronze_Incremental`, que copia dados novos e atualizados do sistema de origem (MySQL) para as tabelas no `LH_Bronze`.

2.  **Transformação (Prata)**: O pipeline executa os notebooks de preparação (ex: `NB_Prepara_Tabela_Cadastros`, `NB_Prepara_Tabela_Titulos`, `NB_Prepara_Tabela_Operacoes`), que limpam e padronizam os dados.

3.  **Curadoria e Agregação (Ouro)**:
    -   O notebook **`NB_Curadoria_Gold`** realiza os joins e regras de negócio para criar as tabelas finais.
    -   Modelos de ML (`ML_Previsao_Inadimplencia_2025`) são executados para enriquecer os dados com previsões.

4.  **Consumo**: Os dados refinados no `WH_Gold` e `LH_Gold` são consumidos pelo Power BI e analistas.

## Padrões e Boas Práticas de Desenvolvimento

Esta seção documenta padrões de código recomendados para garantir a robustez e a performance dos processos na plataforma.

### Ingestão de Arquivos Externos em Notebooks

**Problema:** O código sugere ler o arquivo com pandas e depois converter para Spark (`spark.createDataFrame(pandas_df)`). **O Risco:** O Pandas roda na memória do driver (single-node). Se a Valecred receber um arquivo de cessão de crédito de 5GB, seu pipeline vai quebrar com Out of Memory (OOM). Isso não é escalável.

**Solução:** Force o uso do leitor nativo do Spark, que é distribuído. Se o problema for encoding (comum no Brasil, 'latin1' ou 'ISO-8859-1'), o Spark trata isso nativamente.

**Exemplo de Implementação:**

```python
# Padrão Recomendado: Leitura Distribuída com Spark
# Evita gargalo de memória no Driver

path_arquivo = "Files/temp/meu_arquivo.csv" # Caminho relativo funciona se montado corretamente, ou use ABFSS

df_spark = (spark.read
    .format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .option("encoding", "ISO-8859-1") # Fundamental para acentos PT-BR
    .option("inferSchema", "false")   # Performance: defina schema manual se possível
    .load(path_arquivo)
)

# Salvar como Delta
df_spark.write.format("delta").mode("overwrite").saveAsTable("LH_Bronze.minha_nova_tabela")
```

### Carga de Tabelas Manuais (Excel/CSV)

Utilize o notebook genérico `NB_Load_Silver_From_Manual_Uploads` para carregar arquivos de suporte. Configure o mapeamento arquivo -> tabela dentro do notebook e execute-o sob demanda. Isso garante a padronização automática dos nomes de colunas para `snake_case`.

### Convenções de Nomenclatura

-   **Notebooks**: `NB_Nome_Da_Acao` ou `ML_Nome_Do_Modelo`.
-   **Colunas**: `snake_case` (ex: `cod_cliente`, `data_inclusao`).
-   **Tabelas de Suporte**: Prefixo `sup_`.

## Procedimentos Operacionais

### Atualização Semanal dos Dados da CVM

1.  **Abra o Notebook**: Navegue até `7_Dados_Externos/CVM/NB_Load_From_CVM`.
2.  **Ajuste os Parâmetros**: Modifique as variáveis `ano` e `mes`.
3.  **Execute o Notebook**: O processo fará a ingestão com substituição de partição dinâmica.
