
# Scribe Documentation Update

## Notebook: `NB_Load_From_CVM.Notebook`
- **Change**: Otimização no download de arquivos CVM.
- **Description**: Aumento do `chunk_size` no `response.iter_content` de 8192 (8KB) para 1048576 (1MB).

## Notebook: `NB_Gold_Relatorio_Limites_Especificos.Notebook`
- **Change**: Otimização nas junções (joins) de dados.
- **Description**: Uso da função `broadcast()` nos DataFrames de dimensão `df_clientes_nome`, `df_grupos_nome` e `df_sacados_dedup` para melhorar a performance.

## Notebook: `NB_Gold_Relatorio_Produtos_Mensal.Notebook`
- **Change**: Otimização nas junções (joins) de dados.
- **Description**: Inserção de `broadcast()` em junções com DataFrames de dimensão pequenos, como `df_gerentes`, `df_plataformas`, `df_cli_plat_map`, `df_titulos_dates`, `df_clientes`, `df_score`, `df_prorrogacao_agg` e `df_cliente_agg`.

## Notebook: `NB_Relatorio_Limites_Vencendo.Notebook`
- **Change**: Otimização nas junções (joins) de dados.
- **Description**: Uso de `broadcast()` para otimizar junções das tabelas grandes com DataFrames de dimensão menores, como `df_clientes` e `df_geral`.

## Notebook: `NB_Extrai_Observacoes_Contratos.Notebook`
- **Change**: Modificação de lógica do PySpark.
- **Description**: Refatoração das conversões de strings utilizando diretamente expressões com a API do PySpark (`regexp_replace`, `decode`), em vez da cadeia longa de funções no `withColumn`.

## Notebook: `NB_Prepara_Tabela_Contabil.Notebook`
- **Change**: Adicionado tratamento de erros (Try/Except).
- **Description**: Refatoração do passo de Escrita, encapsulando-o com try/except e incluindo logs detalhados e checagem de erros utilizando `mssparkutils.notebook.exit`.

## Notebook: `VAI_Inferencia_Online.Notebook`
- **Change**: Adicionadas novas regras e verificações.
- **Description**:
  - Adicionado bloco de verificação de excesso de tranche com `alerta_excesso_tranche`.
  - Atualização do cálculo de `anomaly_score` considerando a nova variável de tranche (`alerta_excesso_tranche`).
  - Atualização da regra de `motivo_expr` com `Excesso na Tranche` para explicar falhas quando aplicável.
  - Fix na função que gera explicações no caso do score Z quando `mean_val` era None, convertendo corretamente antes da comparação matemática.

## Tests
- **Change**: Novos arquivos de teste.
- **Description**: Foram incluídos `test_download_and_extract.py` para testar lógicas de download e `test_silver_carteira_pdd_safe_load.py` para testar os tratamentos na carga do PDD da camada silver.
