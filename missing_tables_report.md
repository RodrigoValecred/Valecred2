# Relatório de Tabelas Não Encontradas no Pipeline LH_Gold

Após varredura completa no repositório `VALECRED_DEV` (Notebooks e Pipelines), as seguintes tabelas da lista fornecida **não foram encontradas** como alvos de escrita (atualização) em nenhum processo:

1. **`perfil_analitico_sacado`**
2. **`perfil_risco_sacado`**
3. **`gold_carteira_historico`**
4. **`gold_carteira_valor_diario`**

*Nota: Conforme indicado pelo usuário, estas tabelas podem estar sendo atualizadas por pipelines externos a este repositório.*

### Tabelas Deletadas/Depreciadas

Conforme confirmado pelo usuário:

*   **`dim_produto`** (Substituída por `LH_Gold.dim_produtos`)
*   **`operacoes`** (Substituída por `LH_Gold.fato_operacoes`)

### Tabelas Confirmadas (Atualizadas)

As demais tabelas da lista foram localizadas nos respectivos processos de orquestração:

**Atualizadas via Pipeline `PL_FastTrack_TV`:**

*   **`TV_KPI_VOP_DIA_2`**: Atualizada diretamente pela atividade `Operacoes_do_dia_copy1_copy1` (Copy Data).
*   **`alertas_risco_tv`**: Atualizada pelo notebook `VAI_Inferencia_Online.Notebook` (invocado pela atividade `Valecred_Artificial_Inteligence_copy1`).

**Atualizadas via Notebooks de Engenharia de Dados:**

*   `dim_calendario` -> `NB_Calendario_Gold.Notebook`
*   `relatorio_novos_clientes` -> `NB_Gold_Relatorio_Novos_Clientes.Notebook`
*   `dim_empresas` -> `NB_Gold_Dim_Empresas.Notebook`
*   `dim_danfe` -> `NB_Gold_Dim_Danfe.Notebook`
*   `fato_tarifas_esporadicas` -> `NB_Curadoria_Gold.Notebook`
*   `fato_operacoes_recompra` -> `NB_Curadoria_Gold.Notebook`
*   `gold_cliente_completo` -> `Processamento_Completo_Clientes.Notebook`
*   `metricas_carteira_hhi` -> `NB_Curadoria_Gold.Notebook`
*   `fato_limites_credito` -> `NB_Curadoria_Gold.Notebook`
*   `analise_prazos_esteira` -> `NB_Curadoria_Gold.Notebook`
*   `analise_score_clientes` -> `NB_Curadoria_Gold.Notebook`
*   `dim_clientes` -> `NB_Curadoria_Gold.Notebook`
*   `dim_gerentes` -> `NB_Gold_Dim_Gerentes.Notebook`
*   `fato_titulos` -> `NB_Curadoria_Gold.Notebook`
*   `fato_prorrogacoes_de_titulos` -> `NB_Curadoria_Gold.Notebook`
*   `fato_operacoes_prorrogacao` -> `NB_Curadoria_Gold.Notebook`
*   `relatorio_rentabilidade_clientes_2025` -> `NB_Gold_Analise_Rentabilidade_Clientes_2025.Notebook`
*   `relatorio_limites_vencendo` -> `NB_Relatorio_Limites_Vencendo.Notebook`
*   `risco_cliente_produto` -> `NB_Gold_Risco_Cliente.Notebook`
*   `dim_sacados` -> `NB_Gold_Dim_Sacados.Notebook`
*   `fato_baixas` -> `NB_Curadoria_Gold.Notebook`
*   `esteira_de_propostas` -> `NB_Gold_Esteira_Propostas.Notebook`
*   `dim_historico_inadimplencia` -> `NB_Inadimplencia_Mensal.Notebook`
*   `dim_produtos` -> `NB_Gold_Dim_Produtos.Notebook`
*   `fato_operacoes` -> `NB_Curadoria_Gold.Notebook`
