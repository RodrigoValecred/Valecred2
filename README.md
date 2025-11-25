# Plataforma de Dados Valecred

Este repositório contém o código-fonte da Plataforma de Dados Valecred, uma solução completa de engenharia e análise de dados construída no Microsoft Fabric. A plataforma processa dados brutos de sistemas operacionais, refina-os através de uma arquitetura medalhão e os disponibiliza para usuários finais para aplicações de análise e aprendizado de máquina.

## Visão Geral

O objetivo desta plataforma é centralizar e democratizar o acesso aos dados da Valecred, garantindo qualidade, governança e agilidade. Através de um fluxo de dados bem definido, transformamos dados brutos em insights acionáveis, alimentando desde dashboards de BI até modelos avançados de machine learning para previsão de risco.

## Como Começar

Para começar a trabalhar com a plataforma, siga os passos abaixo.

### Pré-requisitos

*   **Acesso ao Microsoft Fabric**: Você precisará de permissões adequadas no workspace da Valecred.
*   **Git**: O Git deve estar instalado e configurado na sua máquina local para clonar o repositório.
*   **Conhecimento em PySpark e SQL**: Familiaridade com essas tecnologias é essencial para o desenvolvimento de notebooks e dataflows.

### Instalação e Configuração

1.  **Clone o Repositório**:
    ```bash
    git clone <URL_DO_REPOSITORIO>
    cd nome-do-repositorio
    ```
2.  **Sincronize com o Microsoft Fabric**: Conecte seu ambiente de desenvolvimento local ao workspace do Fabric para garantir que as alterações sejam sincronizadas. Siga as [diretrizes oficiais da Microsoft](https://docs.microsoft.com/fabric/git-integration/git-integration-overview) para configurar a integração.
3.  **Explore os Artefatos**: Navegue pelas pastas numeradas para entender a organização dos dataflows, notebooks e pipelines. Comece pela pasta `1_Dataflows` e siga a sequência numérica para entender o fluxo de dados.

## Estrutura do Repositório

O projeto é organizado em uma estrutura de pastas numeradas que reflete o fluxo de processamento dos dados.

```
/
├── 1_Dataflows/         # Dataflows para ingestão e transformação leve (Camada Silver)
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

**Nota sobre a Estratégia Atual:** A arquitetura da plataforma evoluiu para priorizar o uso de **Notebooks** nas camadas Prata (Silver) e Ouro (Gold).

Embora o repositório contenha artefatos de Dataflows (que foram utilizados em versões anteriores da arquitetura), identificou-se que a complexidade dos cálculos de negócio frequentemente quebrava o *query folding* do Dataflow Gen2, impactando a performance.

Portanto, a estratégia vigente é:
-   Utilizar **Notebooks** para todas as transformações complexas, joins e regras de negócio.
-   Manter os Dataflows apenas para tarefas legadas ou ingestões muito simples, caso necessário.

### 2. Pipelines de Dados (`2_Pipelines`)

O processo de orquestração e ingestão de dados é gerenciado por um conjunto de pipelines modulares. Enquanto os pipelines `PL_Load_Bronze_*` cuidam da extração de dados da origem, os pipelines `PL_Orquestracao_*` gerenciam o fluxo de transformação de ponta a ponta.

-   **`PL_Orquestracao_de_Dados_Incremental`**: Este é o pipeline principal para as operações diárias. Ele orquestra a execução de notebooks para preparar os dados na camada Silver, atualiza o dataflow principal (`DF_Preparacao_Silver`) e, em seguida, executa os modelos de machine learning. Ele garante que as transformações ocorram na sequência correta após a chegada de novos dados.
-   **Outros Pipelines de Orquestração**:
    -   **`PL_Orquestracao_de_Dados_Sem_Extracao`**: Uma variação do pipeline principal que executa o fluxo de transformação (Silver para Gold) sem acionar a ingestão da camada Bronze. Útil para reprocessamentos ou execuções de desenvolvimento.
    -   **`PL_Orquestracao_de_Dados_sem_Carga_de_Dados_Inicial`**: Similar ao anterior, projetado para cenários onde a carga de dados inicial é pulada.

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

Os notebooks PySpark são o coração da lógica de negócios e das transformações de dados da plataforma.

#### Notebooks de Transformação (Bronze para Silver)

-   **`NB_Preparacao_Silver`**: Agora focado exclusivamente na **limpeza e tratamento** das tabelas da camada Bronze. Sua responsabilidade é padronizar tipos de dados, remover caracteres indesejados e preparar as tabelas para o enriquecimento, garantindo uma base confiável para as etapas seguintes.
-   **`NB_Build_Bridge_Cliente_Gerente`**: Constrói a tabela ponte `bridge_cliente_gerente` com **SCD Tipo 2** para rastrear o histórico do relacionamento entre clientes e gerentes.
-   **`NB_Process_Contact_Info`**: Processa e limpa dados de contato, tratando campos com múltiplos valores e salvando-os em tabelas de staging na camada Silver.

#### Notebooks de Curadoria e Agregação (Silver para Gold)

-   **`NB_Curadoria_Gold`**: Este notebook centraliza a lógica de **enriquecimento e joins** das tabelas. Ele consome os dados tratados da camada Silver, aplica as regras de negócio complexas (que anteriormente residiam em Dataflows) e consolida as tabelas Fato e Dimensão da camada Ouro. As principais tabelas geradas por este notebook e salvas no `LH_Gold` são:
    -   `dim_produto`
    -   `fato_baixas`
    -   `fato_titulos`
    -   `esteira_de_propostas`
-   **`NB_Gold_Customer_Dashboard`**: Gera tabelas agregadas no `WH_Gold` para o dashboard de acompanhamento da carteira do cliente, identificando títulos em aberto e vencidos.
-   **`NB_Gold_Risco_Cliente`**: Cria uma tabela agregada no `LH_Gold` que sumariza o valor total em risco para cada cliente, segmentado por tipo de produto.
-   **`NB_Risk_Aggregation`**: Calcula métricas históricas de risco por cliente, como taxa de inadimplência e volume transacionado, e salva o resultado na tabela `risco_por_cliente` no `WH_Gold`.

#### Notebooks de Utilidade e Análise Ad-Hoc

-   **`NB_Analise_Cliente_Especifico`**: Ferramenta de análise que replica a lógica do modelo de ML para permitir uma investigação detalhada do comportamento histórico de um cliente específico.
-   **`NB_Backup_Bronze_Table`**: Notebook operacional que cria uma cópia de segurança (backup) de uma tabela específica na camada Bronze para garantir a recuperação de dados.

### 6. Machine Learning (`6_Machine_Learning`)

Esta seção detalha os modelos de machine learning da plataforma e os notebooks associados.

#### Notebooks de Aprendizado de Máquina

-   **`ML_Inadimplencia_Aprimorado(1)`**: Notebook principal para o **treinamento** do modelo de classificação (`LightGBM`) que prevê a probabilidade de inadimplência de um título. Ele realiza a engenharia de features e salva o artefato do modelo treinado no Lakehouse.
-   **`ML_Previsao_Inadimplencia_2025`**: Notebook de **inferência em lote**. Ele carrega o modelo treinado e o aplica a um conjunto de títulos em aberto, salvando as previsões em uma tabela no Lakehouse.
-   **`NB_Gerador_Score_Risco`**: Ferramenta de **análise interativa**. Permite que um analista insira o CPF/CNPJ de um cliente e obtenha um score de risco em tempo real para seus títulos em aberto, juntamente com os principais fatores que contribuem para o score.

#### Modelo: Previsão de Inadimplência

O notebook `ML_Inadimplencia_Aprimorado(1)` desenvolve um modelo de classificação para prever a probabilidade de um título se tornar inadimplente. Esta seção detalha as bases utilizadas pelo modelo para realizar seus cálculos.

**Algoritmo Utilizado**
O modelo é baseado no algoritmo `LightGBM`, uma implementação de Gradient Boosting altamente eficiente, conhecida por sua velocidade e precisão.

**Base para o Cálculo da Probabilidade (Features)**
A probabilidade de inadimplência é calculada com base em um conjunto de variáveis (features) extraídas dos dados da operação, do título e do cliente. As principais features utilizadas são:

-   `VALOR`: O valor monetário do título.
-   `PRAZO`: O número de dias até o vencimento do título.
-   `DESAGIO`: A taxa de desconto aplicada na operação de compra do título.
-   `TTO_OPERACAO`: O tipo da operação (ex: compra, adiantamento).
-   `STTO`: O subtipo da operação, que detalha a `TTO_OPERACAO`.
-   `CODSTATUSCLIENTE`: O status cadastral do cliente (ex: ativo, inativo).
-   `CODRATING_CEDENTE`: A classificação de risco (rating) atribuída ao cedente (cliente).
-   `FATOR`: O fator utilizado no cálculo da operação.
-   `TARIFA`: A tarifa cobrada na operação.
-   `CIDADE`: A cidade de localização do cliente.
-   `UF`: O estado de localização do cliente.

**Variável Alvo (Target)**
O modelo é treinado para prever a variável `TARGET`, que é definida da seguinte forma:

-   **0 (Adimplente)**: Se o título foi pago (`MOTIVO` = 'PG').
-   **1 (Inadimplente)**: Para qualquer outro motivo de baixa do título.

Essa abordagem permite que o modelo aprenda os padrões nos dados que historicamente levaram a casos de inadimplência, e então aplique esse conhecimento para calcular a probabilidade de novos títulos se tornarem inadimplentes.

### 7. Dados Externos (`7_Dados_Externos`)

Esta seção descreve os notebooks responsáveis por ingerir dados de fontes externas para a camada Bronze.

-   **`NB_Load_Bronze_From_BrasilIO`**: Baixa datasets públicos sobre empresas e sócios do portal [Brasil.IO](https://brasil.io/dataset/socios-brasil/files/) e os carrega na camada Bronze.
-   **`NB_Load_Bronze_From_SERPRO`**: Ingesta dados de licitações do [Portal da Transparência](https://portaldatransparencia.gov.br/download-de-dados/licitacoes), filtrando para manter apenas registros do SERPRO.
-   **`NB_Load_Informes_da_CVM`**: Baixa os informes mensais de FIDC (Fundos de Investimento em Direitos Creditórios) do portal de dados abertos da CVM e os carrega de forma incremental na camada Bronze, utilizando particionamento dinâmico para evitar a perda de dados históricos.
-   **`NB_Load_Bronze_Receita_Federal_Full`**: Orquestra a ingestão e o processamento dos dados públicos de CNPJ da Receita Federal, convertendo-os para um formato limpo na camada Bronze.

## Fluxo de Dados de Ponta a Ponta

O fluxo de dados principal é orquestrado pelo pipeline `PL_Orquestracao_de_Dados_Incremental` e segue a arquitetura medalhão.

1.  **Ingestão (Bronze)**: O processo começa com o pipeline `PL_Load_Bronze_Incremental`, que copia dados novos e atualizados do sistema de origem (MySQL) para as tabelas no `LH_Bronze`. Pipelines de ingestão de dados externos (`NB_Load_Bronze_*`) também podem ser executados para enriquecer a camada Bronze com dados públicos.

2.  **Transformação (Prata)**: O pipeline `PL_Orquestracao_de_Dados_Incremental` assume o controle e executa a **Preparação da Camada Silver**:
    -   O notebook **`NB_Preparacao_Silver`** é executado para realizar a limpeza e tratamento dos dados brutos da camada Bronze.
    -   Notebooks auxiliares (como `NB_Build_Bridge_Cliente_Gerente`) rodam em paralelo para construir estruturas de suporte.
    -   Os dados tratados são persistidos no `LH_Silver`, prontos para serem consumidos.

3.  **Curadoria e Agregação (Ouro)**:
    a. **Enriquecimento e Modelagem**: O notebook **`NB_Curadoria_Gold`** entra em ação para realizar os joins entre as tabelas tratadas e aplicar regras de negócio complexas, populando as tabelas da camada Ouro (`LH_Gold` e `WH_Gold`).
    b. **Execução de Modelos de ML**: O pipeline executa o notebook de inferência `ML_Previsao_Inadimplencia_2025` (referenciado no pipeline como `ML_Prob_Inad_Cart`), que carrega o modelo treinado e aplica as previsões de risco.
    c. **Criação de Tabelas de Negócio**: Outros notebooks específicos podem ser executados para criar agregações adicionais.

4.  **Consumo**: Os dados refinados no `WH_Gold` e `LH_Gold` estão prontos para serem consumidos por ferramentas de BI, relatórios e análises ad-hoc, como as realizadas pelos notebooks `NB_Gerador_Score_Risco` e `NB_Analise_Cliente_Especifico`.

Essa abordagem modular e orquestrada garante que os dados sejam processados de forma eficiente, consistente e confiável em toda a plataforma.

## Tabelas da Camada Ouro (Gold)

A camada Ouro é a camada final da arquitetura, onde os dados são modelados e agregados para atender às necessidades específicas de negócio, como relatórios de BI, análises avançadas e aplicações de machine learning.

### Princípios de Modelagem

Antes de listar as tabelas, é importante ressaltar os princípios que guiam a modelagem de dados nesta camada. O objetivo não é simplesmente minimizar ou maximizar o número de tabelas, mas **modelar o processo de negócio da forma mais clara e correta possível**. Um modelo bem estruturado, geralmente com tabelas mais especializadas (seguindo um esquema estrela), traz os seguintes benefícios:

-   **Clareza e Manutenção:** Cada tabela possui um propósito único e bem definido, tornando o modelo mais fácil de entender e manter.
-   **Poder Analítico:** Uma estrutura granular permite cruzar informações de múltiplas formas, destravando análises mais profundas e complexas.
-   **Consistência e Eficiência:** Evita a redundância de dados, garantindo que as informações sejam armazenadas em um único local e promovendo a consistência em toda a plataforma.
-   **Granularidade Correta:** As tabelas de fatos são definidas no nível mais baixo possível, garantindo que os dados possam ser agregados para responder a uma vasta gama de perguntas de negócio.

Abaixo está uma lista preliminar das tabelas planejadas para a camada Gold, seguindo esses princípios.

**Nota:** Esta lista é um esboço inicial. Nomes podem ser alterados, algumas tabelas podem ser desconsideradas e novas tabelas podem ser adicionadas conforme o projeto evolui.

### Tabelas de Fatos (Facts)
As tabelas de fatos registram as medições ou métricas de um evento de negócio.

-   `fato_abatimentos`
-   `fato_baixas`
-   `fato_checagens`
-   `fato_cobrancas`
-   `fato_notificacoes`
-   `fato_operacoes`
-   `fato_pareceres`
-   `fato_pendencias`
-   `fato_prorrogacoes`
-   `fato_recompras`
-   `fato_tarifas_esporadicas`

### Tabelas de Dimensões (Dimensions)
As tabelas de dimensões fornecem o contexto descritivo para os eventos nas tabelas de fatos.

-   `dim_atividade_economica`
-   `dim_calendario`
-   `dim_chaveDanfe`
-   `dim_clientes`
-   `dim_empresas`
-   `dim_geografica`
-   `dim_gerentes`
-   `dim_grupos_economicos_de_clientes`
-   `dim_operacoes`
-   `dim_plataformas`
-   `dim_produtos`
-   `dim_sacados`
-   `dim_titulos`

### Tabelas de Relacionamento (Relationship Tables)
Tabelas que mapeiam relações complexas entre dimensões.

-   `rlc_gerente_cliente`

### Tabelas Agregadas e Específicas (Gold Tables)
Tabelas que contêm agregações pré-calculadas ou são criadas para propósitos analíticos específicos.

-   `gold_carteiras_diarias_por_cliente`
-   `gold_esteira_de_status_da_proposta`
-   `gold_metas`
-   `gold_risco_por_cliente_vs_produto`


## Análise de Re-execução da Carga de Dados

Uma questão comum em pipelines de dados é o impacto de re-executar um processo de carga várias vezes no mesmo dia. Esta seção analisa os riscos e o comportamento do pipeline `PL_Orquestracao_de_Dados_Incremental` quando executado múltiplas vezes.

**Conclusão: É seguro executar o pipeline de carga incremental várias vezes ao dia.**

A re-execução do pipeline não resultará em **duplicidade de dados, perda de dados ou inconsistências**. A plataforma foi projetada com princípios de **idempotência**, o que significa que múltiplas execuções idênticas produzem o mesmo resultado que uma única execução.

### Análise dos Componentes

1.  **Notebooks de Preparação (`NB_Preparacao_Silver`, `NB_Build_Bridge_Cliente_Gerente`)**:
    -   **Estratégia de Carga**: A grande maioria das tabelas geradas por estes notebooks (ex: `staging_titulos_limpa`, `fato_baixas`, `bridge_cliente_gerente`) é gravada usando o modo `overwrite` (`write.mode("overwrite")`).
    -   **Impacto**: Este modo apaga completamente os dados existentes na tabela de destino antes de escrever os novos dados. Portanto, a cada execução, as tabelas são reconstruídas do zero a partir da fonte (camada Bronze), garantindo que o resultado final seja sempre o mais atual, sem duplicatas.
    -   **Lógica Incremental**: A única exceção é o processamento da tabela `cad_geral_pareceres`, que utiliza um sistema de *watermark* (marca d'água). Se o pipeline for executado uma segunda vez no mesmo dia, o notebook detectará que não há dados novos desde a última execução e simplesmente não processará nenhum registro, evitando qualquer alteração ou duplicação.

2.  **Notebook de Curadoria (`NB_Curadoria_Gold`)**:
    -   **Estratégia de Carga**: Assim como os notebooks de preparação, o `NB_Curadoria_Gold` utiliza o modo `overwrite` ao salvar as tabelas da camada Ouro.
    -   **Impacto**: A cada execução, ele recalcula os joins e as regras de negócio sobre os dados mais recentes da camada Silver e recria as tabelas Fato e Dimensão. Isso garante a consistência e a idempotência do processo, eliminando riscos de duplicidade na camada final.

Em resumo, a combinação de cargas com `overwrite` e lógicas incrementais baseadas em *watermark* garante que o pipeline de orquestração possa ser executado quantas vezes forem necessárias, sem efeitos colaterais negativos. Na verdade, re-executar o pipeline é a maneira correta de garantir que os dados reflitam o estado mais recente da camada Bronze.

### Detalhamento por Tipo de Carga

Para maior clareza, abaixo está a lista de tabelas geradas pelo pipeline e suas respectivas estratégias de carga:

*   **Tabelas com Carga Completa (`Overwrite`)**: A maioria das tabelas é completamente recriada a cada execução. Isso garante que os dados estejam sempre sincronizados com a fonte, sem risco de duplicação.
    *   `LH_Silver.staging_titulos_limpa`
    *   `LH_Silver.staging_clientes_limpa`
    *   `LH_Silver.staging_cad_geral_limpa`
    *   `LH_Silver.staging_protestos`
    *   `LH_Silver.staging_operacoes_limpa`
    *   `LH_Silver.staging_chave_danfe_detalhada`
    *   `LH_Silver.staging_baixas_limpa`
    *   `LH_Silver.bridge_cliente_gerente`
    *   `LH_Silver.relacionamento_cliente_gerente_atual`
    *   Todas as tabelas de fatos e dimensões geradas pelo Notebook `NB_Curadoria_Gold`.

*   **Tabelas com Carga Incremental (`Merge`)**: Apenas um número muito pequeno de tabelas é atualizado de forma incremental para preservar o histórico ou controlar o processo.
    *   `LH_Silver.pareceres_de_alteracao_de_status`: Atualizada via `MERGE` para adicionar apenas os novos pareceres capturados desde a última execução.
    *   `LH_Silver.etl_watermark_control`: A tabela de controle de watermark é atualizada via `MERGE` para registrar o ponto de parada da última carga bem-sucedida.

## Padrões e Boas Práticas de Desenvolvimento

Esta seção documenta padrões de código recomendados para garantir a robustez e a performance dos processos na plataforma.

### Otimização de Performance na Camada Silver

Para garantir que os processos de ETL, especialmente os Dataflows, sejam executados com a máxima eficiência e aproveitem o *query folding* (a capacidade de delegar o processamento para o sistema de origem), a ordem das etapas de transformação é crucial. Seguir a ordem correta reduz a quantidade de dados trafegados e processados em memória.

A sequência recomendada de operações é:

1.  **Filtrar Linhas o Mais Cedo Possível**:
    -   **O que fazer**: Aplique filtros (`Table.SelectRows` no Power Query) para remover registros desnecessários logo no início do fluxo.
    -   **Por que funciona**: Reduzir o número de linhas é a maneira mais eficaz de diminuir o volume de dados. Isso garante que as transformações subsequentes operem em um conjunto de dados muito menor, acelerando todo o processo.

2.  **Remover Colunas Não Utilizadas**:
    -   **O que fazer**: Use `Table.SelectColumns` para manter apenas as colunas que são estritamente necessárias para as etapas seguintes ou para o resultado final. Evite usar `Table.RemoveColumns`, que é menos performático.
    -   **Por que funciona**: Assim como filtrar linhas, remover colunas desnecessárias reduz a "largura" dos dados, diminuindo o consumo de memória e a carga de processamento. Fazer isso antes de transformações complexas evita que o motor processe dados que serão descartados.

3.  **Aplicar Transformações e Regras de Negócio**:
    -   **O que fazer**: Execute as operações mais "caras" (tratamento de texto, cálculos, junções) por último.
    -   **Por que funciona**: Neste ponto, o conjunto de dados já está otimizado (menos linhas e menos colunas), então o custo computacional para aplicar estas transformações é significativamente menor.

**Exemplo Prático (Power Query M):**

```m
// Ruim: Transformações antes de filtrar
let
    Fonte = Origem_De_Dados,
    ColunasRenomeadas = Table.RenameColumns(Fonte, {{"Nome Completo", "nome_cliente"}}),
    TextoMaiusculo = Table.TransformColumns(ColunasRenomeadas, {{"nome_cliente", Text.Upper, type text}}),
    LinhasFiltradas = Table.SelectRows(TextoMaiusculo, each [ativo] = true),
    ColunasSelecionadas = Table.SelectColumns(LinhasFiltradas, {"id_cliente", "nome_cliente"})
in
    ColunasSelecionadas

// Bom: Filtrar e selecionar colunas primeiro
let
    Fonte = Origem_De_Dados,
    LinhasFiltradas = Table.SelectRows(Fonte, each [ativo] = true),
    ColunasSelecionadas = Table.SelectColumns(LinhasFiltradas, {"id_cliente", "Nome Completo"}),
    ColunasRenomeadas = Table.RenameColumns(ColunasSelecionadas, {{"Nome Completo", "nome_cliente"}}),
    TextoMaiusculo = Table.TransformColumns(ColunasRenomeadas, {{"nome_cliente", Text.Upper, type text}})
in
    TextoMaiusculo
```

Seguir esta ordem garante que o *query folding* seja mantido pelo maior tempo possível e que o motor do Fabric processe a menor quantidade de dados necessária.

### Convenções de Nomenclatura

Para manter a clareza e a consistência em todo o projeto, seguimos um conjunto de convenções de nomenclatura para os artefatos.

-   **Dataflows, Pipelines e Notebooks**: Os nomes devem ser descritivos e usar o formato `Prefix_Contexto_Camada`. Por exemplo, `DF_Preparacao_Silver`, `NB_Load_Bronze_From_BrasilIO`.
-   **Colunas de Tabelas**: Todas as colunas devem seguir o padrão `snake_case` (letras minúsculas, com palavras separadas por `_`).
-   **Tabelas de Suporte (Manuais)**: Tabelas carregadas a partir de arquivos manuais (Excel, CSV) e que servem como dados de apoio, parametrização ou "de-para" devem usar o prefixo `sup_`. A abreviação `sup` vem de "suporte". Exemplos: `sup_metas`, `sup_regiao`.
    - `sup_clientes_desconsiderados_do_pdd`
    - `sup_clientes_diretoria_info_mercado`
    - `sup_clientes_em_perdas`
    - `sup_cor_clientes_inadimplencia`
    - `sup_grupos_economicos`
    - `sup_metas`
    - `sup_motivos_de_indeferimento`
    - `sup_nivel_maturidade`
    - `sup_nivel_usuario`
    - `sup_regiao`
    - `sup_status_de_clientes_da_esteira`
    - `sup_uf`

### Ingestão de Arquivos Externos em Notebooks

**Problema:** Ao baixar arquivos de fontes externas (ex: APIs, sites de dados abertos) e salvá-los no diretório `Files` do Lakehouse, o Spark pode falhar ao tentar ler esses arquivos diretamente com `spark.read.csv()`. Isso ocorre devido a inconsistências na resolução de caminhos entre o nó driver do Spark e os nós de trabalho no ambiente Microsoft Fabric, resultando em erros de `Path not found`.

**Solução Padrão:** A abordagem mais robusta é ler o arquivo na memória do driver usando uma biblioteca como o `pandas` e, em seguida, criar um DataFrame Spark a partir do DataFrame pandas em memória. Este método evita completamente os problemas de resolução de caminho do Spark.

**Exemplo de Implementação:**

```python
import pandas as pd
import os

# 1. Salvar o arquivo no diretório 'Files', que é localmente acessível ao driver
local_path = "Files/temp/meu_arquivo.csv"
# ... (lógica para baixar e salvar o arquivo em local_path) ...

# 2. Ler o arquivo para um DataFrame pandas
# O pandas executa no driver e consegue acessar o caminho local sem problemas
pandas_df = pd.read_csv(local_path, sep=';', encoding='ISO-8859-1')

# 3. Criar um DataFrame Spark a partir do DataFrame pandas
# O Spark distribui eficientemente o DataFrame em memória para o cluster
spark_df = spark.createDataFrame(pandas_df)

# Agora, spark_df pode ser usado para transformações e salvamento
spark_df.write.format("delta").mode("overwrite").saveAsTable("LH_Bronze.minha_nova_tabela")
```

Este padrão deve ser o método preferencial para todos os novos notebooks que realizam a ingestão de arquivos externos.

### Carga de Tabelas Manuais (Excel/CSV)

**Problema:** Tabelas de suporte ou de parâmetros que são mantidas manualmente em arquivos Excel ou CSV precisam de um processo padronizado e escalável para serem incorporadas à plataforma de dados.

**Solução Padrão:** O processo a seguir garante que esses arquivos sejam ingeridos de forma consistente, utilizando um notebook genérico que centraliza a lógica de carga e padronização.

1.  **Armazenamento no SharePoint**: O arquivo original (fonte da verdade) deve ser armazenado em uma pasta designada no SharePoint da equipe de Tecnologia.
    -   **Caminho**: `Documentos > E. Tecnologia e Infraestrutura > 06. Ciência de Dados > 02. Diretrizes... > Tabelas manuais para o banco de dados`

2.  **Upload para o Lakehouse Bronze**: O arquivo é carregado (upload) para a pasta `Files/manual_uploads/` dentro do Lakehouse `LH_Bronze`.

3.  **Uso de um Notebook de Carga Genérico**: Em vez de criar um notebook para cada arquivo, utilize o notebook genérico `NB_Load_Bronze_From_Manual_Uploads`. Este notebook é projetado para processar múltiplos arquivos de forma configurável.
    -   **Configuração**: Para adicionar um novo arquivo, basta adicionar uma nova entrada na lista `files_to_process` dentro do notebook, mapeando o nome do arquivo de origem para o nome da tabela de destino no `LH_Silver`.
    -   **Lógica do Notebook**: O notebook irá iterar sobre a lista de configuração e para cada arquivo:
        -   Ler o arquivo (`.xlsx` ou `.csv`) da pasta `Files/manual_uploads/` usando `pandas`.
        -   **Padronizar os Nomes das Colunas**: Automaticamente limpará os nomes das colunas para remover espaços, acentos e caracteres especiais, convertendo-os para o formato `snake_case`. Este passo é crucial para evitar erros ao salvar em formato Delta.
        -   Converter o DataFrame do `pandas` para um DataFrame Spark.
        -   Salvar a tabela na camada Silver, sobrescrevendo a versão anterior.

4.  **Execução do Processamento**: Como estas tabelas são atualizadas com pouca frequência, o notebook `NB_Load_Bronze_From_Manual_Uploads` deve ser executado sob demanda através de um pipeline dedicado (ex: `PL_Process_Manual_Uploads`).
    -   **Execução**: Após atualizar o arquivo no SharePoint e fazer o upload para o `LH_Bronze`, execute o pipeline dedicado manualmente para propagar as alterações para a camada Silver. Não adicione este processo ao pipeline incremental diário para evitar execuções desnecessárias.

Este fluxo garante que os dados manuais sejam tratados com a mesma governança que os dados automáticos, passando por um processo de ETL robusto e de fácil manutenção.

### Armadilhas Comuns e Lições Aprendidas no Fabric

Esta seção documenta erros comuns encontrados durante o desenvolvimento e as soluções corretas para evitar retrabalho.

1.  **Acessando Arquivos no Lakehouse com `pandas`**
    -   **O que não funciona**: Usar caminhos relativos (ex: `Files/meu_arquivo.xlsx`) para ler arquivos com bibliotecas como o `pandas`. Isso resultará em um erro `FileNotFoundError`.
    -   **O que funciona**: Sempre use o **caminho absoluto** que começa com `/lakehouse/default/`. Por exemplo: `pd.read_excel("/lakehouse/default/Files/manual_uploads/meu_arquivo.xlsx")`.

2.  **Diferença entre Arquivos e Tabelas no Lakehouse**
    -   **O que não funciona**: Assumir que o upload de um arquivo (ex: `.xlsx`, `.csv`) para a pasta `Files` cria automaticamente uma tabela que pode ser lida com `spark.read.table("nome_da_tabela")`. Isso resultará em um erro `TABLE_OR_VIEW_NOT_FOUND`.
    -   **O que funciona**: O upload para a pasta `Files` apenas armazena o arquivo. Para lê-lo, você deve usar seu caminho absoluto, como descrito no ponto anterior. Tabelas consultáveis via `spark.read.table()` são aquelas que aparecem na seção `Tables` da interface do Lakehouse, geralmente em formato Delta.

3.  **Uso de Utilitários Específicos da Plataforma**
    -   **O que não funciona**: Usar comandos específicos do Databricks, como `dbutils.notebook.exit()`, no ambiente Microsoft Fabric. Isso causará um `NameError`, pois o módulo `dbutils` não existe no Fabric.
    -   **O que funciona**: Use construções padrão do Python para controle de fluxo e tratamento de erros. Para parar a execução de um notebook e sinalizar um erro, use `raise Exception("Sua mensagem de erro")`.

4.  **Nomes de Colunas Inválidos ao Salvar em Delta**
    -   **O que não funciona**: Tentar salvar um DataFrame em formato Delta quando suas colunas contêm espaços, acentos ou caracteres especiais (ex: "Razão Social", "Código UF"). Isso resultará em um erro `DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES`.
    -   **O que funciona**: Sempre padronize os nomes das colunas *antes* de salvar a tabela. A melhor prática é adicionar uma etapa no seu notebook para limpar programaticamente os nomes das colunas, convertendo-os para um formato como `snake_case` (minúsculas, com underscores em vez de espaços) e removendo todos os caracteres não-alfanuméricos.

5.  **Acessando Tabelas Delta Gerenciadas para Operações de MERGE/UPDATE**
    -   **O que não funciona**: Tentar acessar uma tabela Delta gerenciada (uma que aparece na UI do Lakehouse) usando `DeltaTable.forPath(spark, "Tables/nome_da_tabela")`. Embora isso possa funcionar em alguns cenários, é uma abordagem frágil que depende de caminhos relativos e pode facilmente levar a um erro `[DELTA_MISSING_DELTA_TABLE]`.
    -   **O que funciona**: Sempre use `DeltaTable.forName(spark, "nome_do_lakehouse.nome_da_tabela")`. Este método utiliza o metastore do Spark para resolver o caminho correto e completo da tabela, tornando o código mais robusto e portátil para operações como `MERGE`, `UPDATE` e `DELETE`.

6.  **Criação de Novos Artefatos (Dataflows, Pipelines, etc.)**
    -   **O que não funciona**: Criar um novo artefato (ex: um Dataflow) diretamente no repositório Git, criando manualmente a pasta e os arquivos (`mashup.pq`/`queryMetadata.json`). Isso resultará em erros de sincronização e o artefato não será reconhecido pelo serviço do Fabric.
    -   **O que funciona**: O fluxo de trabalho correto é:
        1.  **Criar na Interface do Fabric**: Crie o novo artefato (ex: um Dataflow em branco) primeiro na interface web do Microsoft Fabric.
        2.  **Sincronizar com o Git**: Salve o artefato e sincronize o workspace com o repositório Git. Este processo criará a estrutura de pastas e os arquivos base corretos no repositório.
        3.  **Obter o ID Real**: Para o novo artefato, copie seu ID real da URL no navegador. Este ID é essencial para referenciá-lo em outros artefatos, como pipelines.
        4.  **Desenvolver no Repositório**: Agora você pode editar os arquivos base (ex: `mashup.pq`, `pipeline-content.json`) no seu ambiente de desenvolvimento local. Ao adicionar uma referência a este novo artefato em um pipeline, use o ID real obtido na etapa anterior para evitar erros de dependência na implantação.

**Aviso Importante:** A criação de novos artefatos **deve** ser iniciada na interface de usuário (UI) do Microsoft Fabric, e não diretamente no repositório Git.
    >
    > -   **Dataflows e Pipelines:** Tentar criar um novo Dataflow ou Pipeline adicionando seus arquivos de definição (`mashup.pq`, `pipeline-content.json`, etc.) diretamente no Git resultará em **falha na importação**. O Fabric não reconhecerá o artefato.
    > -   **Tabelas do Data Warehouse:** Da mesma forma, adicionar um novo arquivo `.sql` no diretório do Warehouse via Git **não criará a tabela** correspondente no serviço do Fabric.
    >
    > O fluxo de trabalho correto e obrigatório é:
    > 1.  **Criar o artefato em branco na UI do Fabric** (ex: um novo Dataflow, uma nova tabela no Warehouse).
    > 2.  **Sincronizar o workspace com o Git** para que o Fabric crie a estrutura de arquivos base no repositório.
    > 3.  A partir daí, **incrementar o código** dos arquivos gerados (ex: preencher a lógica do `mashup.pq` ou o DDL do arquivo `.sql`) através do Git.

## Procedimentos Operacionais

Esta seção descreve os procedimentos manuais ou recorrentes necessários para manter a plataforma de dados atualizada.

### Atualização Semanal dos Dados da CVM

Os dados de FIDC (Fundos de Investimento em Direitos Creditórios) são atualizados pela CVM toda terça-feira. Para carregar os dados mais recentes na plataforma, siga os passos abaixo:

1.  **Abra o Notebook**: Navegue até `7_Dados_Externos/NB_Load_Informes_da_CVM`.
2.  **Ajuste os Parâmetros**: Na segunda célula do notebook, modifique as variáveis `ano` e `mes` para refletir o período que você deseja carregar. Por exemplo, para carregar os dados de setembro de 2025, a célula deve ficar assim:
    ```python
    ano = "2025"
    mes = "09"
    ```
3.  **Execute o Notebook**: Clique em "Executar tudo" para iniciar o processo. O notebook irá baixar os dados do site da CVM, processá-los e carregá-los na tabela `LH_Bronze.cvm_fidc_informe_mensal`.

O notebook está configurado para usar **substituição de partição dinâmica**. Isso significa que:
-   Se você carregar dados para um novo mês, eles serão adicionados à tabela.
-   Se você executar novamente para um mês que já existe, os dados daquele mês serão atualizados sem afetar os outros meses.
