# Relatório de Tabelas Não Encontradas no Pipeline LH_Gold

Após varredura completa no repositório `VALECRED_DEV`, as seguintes tabelas da lista fornecida **não foram encontradas** como alvos de escrita (atualização) nos notebooks de engenharia de dados:

1. **`perfil_analitico_sacado`**
2. **`alertas_risco_tv`**
3. **`perfil_risco_sacado`**
4. **`gold_carteira_historico`**
5. **`gold_carteira_valor_diario`**
6. **`TV_KPI_VOP_DIA_2`**

### Tabelas com Nomes Similares (Possíveis Divergências)

As seguintes tabelas da lista não foram encontradas com a grafia exata, mas existem tabelas muito similares sendo atualizadas:

*   **`dim_produto`** (Singular): Não encontrada.
    *   **Encontrada:** `LH_Gold.dim_produtos` (Plural) é atualizada em `NB_Gold_Dim_Produtos.Notebook`.
*   **`operacoes`** (Sem prefixo): Não encontrada.
    *   **Encontrada:** `LH_Gold.fato_operacoes` é atualizada em `NB_Curadoria_Gold.Notebook`.

### Tabelas Confirmadas (Atualizadas)

Todas as demais tabelas da lista foram encontradas e estão sendo atualizadas pelos respectivos notebooks:

*   `dim_calendario`
*   `relatorio_novos_clientes`
*   `dim_empresas`
*   `dim_danfe`
*   `fato_tarifas_esporadicas`
*   `fato_operacoes_recompra`
*   `gold_cliente_completo`
*   `metricas_carteira_hhi`
*   `fato_limites_credito`
*   `analise_prazos_esteira`
*   `analise_score_clientes`
*   `dim_clientes`
*   `dim_gerentes`
*   `fato_titulos`
*   `fato_prorrogacoes_de_titulos`
*   `fato_operacoes_prorrogacao`
*   `relatorio_rentabilidade_clientes_2025`
*   `relatorio_limites_vencendo`
*   `risco_cliente_produto`
*   `dim_sacados`
*   `fato_baixas`
*   `esteira_de_propostas`
*   `dim_historico_inadimplencia`
