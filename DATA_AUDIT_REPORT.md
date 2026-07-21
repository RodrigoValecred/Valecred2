# Relatório de Auditoria de Dados e Lineage

## Objetivo
Este relatório apresenta os resultados da auditoria de dados (Data Lineage), focada em identificar tabelas sem uso (pontas soltas), processos duplicados e potenciais quebras da "fonte única da verdade" no ambiente do Microsoft Fabric.

## 1. Tabelas sem uso ("Pontas Soltas")
Identificamos as seguintes tabelas que são criadas ou processadas em Notebooks (`saveAsTable`), mas não identificamos nenhum processo downstream realizando leitura delas (`spark.read.table` ou `spark.table`).

*Essas tabelas podem estar gerando custo de processamento e armazenamento sem entregar valor, ou estão sendo consumidas apenas diretamente pelo Power BI (fora do rastreamento de código).*

- `LH_Silver.analise_pareceres_keywords`
- `LH_Silver.fato_devolucoes_cadastro`
- `LH_Silver.staging_gerentes`
- `LH_Silver.staging_plataformas`
- `LH_Silver.staging_status_clientes_esteira`
- `LH_Silver.staging_tac_m`

## 2. Processos Duplicados (Escritas Múltiplas)
Identificamos tabelas sendo gravadas ou subscritas por mais de um Notebook, o que pode indicar lógica duplicada ou sobreposição indesejada, **quebrando a fonte única da verdade (Single Source of Truth)**.

- **`LH_Bronze.rfb_empresas_full`**: Gravada por `NB_Extract_Bronze_Receita_Federal_Full.Notebook` e `NB_Load_Bronze_Receita_Federal_Full.Notebook`.
- **`LH_Bronze.rfb_estabelecimentos_full`**: Gravada por `NB_Extract_Bronze_Receita_Federal_Full.Notebook` e `NB_Load_Bronze_Receita_Federal_Full.Notebook`.
- **`LH_Silver.analise_pareceres_keywords`**: Gravada por `NB_Silver_Pareceres_Keyword.Notebook` e `NB_Silver_Pareceres_Keywords.Notebook` (Ponta Solta). Existe clara duplicação de cadernos.
- **`LH_Silver.fato_devolucoes_cadastro`**: Gravada por `NB_Silver_Fato_Devolucoes_Cadastro.Notebook` e `NB_Silver_Fato_Devolucoes_Cadastros.Notebook` (Ponta Solta). Erro de nomenclatura no plural/singular.
- **`LH_Silver.staging_abatimentos`**: Gravada por `NB_Prepara_Tabela_Titulos.Notebook` e `NB_Preparacao_Silver.Notebook`.
- **`LH_Silver.staging_boletos_titulos`**: Gravada por `NB_Prepara_Tabela_Titulos.Notebook` e `NB_Preparacao_Silver.Notebook`.
- **`LH_Silver.staging_notificacoes`**: Gravada por `NB_Prepara_Tabela_Titulos.Notebook` e `NB_Preparacao_Silver.Notebook`.

## 3. Recomendações
1. **Consolidação de Notebooks Plurais/Singulares:** Deletar os cadernos redundantes `NB_Silver_Pareceres_Keyword.Notebook` e `NB_Silver_Fato_Devolucoes_Cadastros.Notebook` e manter apenas a versão final oficial.
2. **Revisão da Staging Silver:** A rotina `NB_Preparacao_Silver.Notebook` parece estar redundante em relação a scripts menores focados como `NB_Prepara_Tabela_Titulos.Notebook`. Avaliar se `NB_Preparacao_Silver` foi depreciada ou se deve orquestrar a carga de fato.
3. **Validar Consumo PBI:** Verificar se as tabelas listadas em "Pontas Soltas" são modelos semânticos diretos no Power BI. Caso não sejam consumidas, os processos de carga devem ser removidos dos Pipelines.
