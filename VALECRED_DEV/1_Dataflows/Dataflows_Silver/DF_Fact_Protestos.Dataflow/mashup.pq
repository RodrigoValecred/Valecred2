[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_protestos_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared fact_protestos = let
    // Conecta-se a um banco de dados MySQL para obter ocorrências de cobrança de títulos a partir de 2023.
    origem = staging_rlc_titulos_ocorrencias_cobranca,
    
    // Filtra ocorrências com 'CODOCORINTERNA' <> 2, 'TOCORRENCIA' = 2 e 'CODOCORCOBRBANCO' 19 ou 23.
    linhasFiltradas = Table.SelectRows(origem, each ([CODOCORINTERNA] <> 2) and [TOCORRENCIA] = 2 and ([CODOCORCOBRBANCO] = 19 or [CODOCORCOBRBANCO] = 23)),

    // Adiciona a coluna 'TIPO' como "PROTESTADO" ou "NÃO PROTESTADO".
    colunaCondicionalAdicionada = Table.AddColumn(linhasFiltradas, "tipo", each if [CODOCORINTERNA] >= 8 then "PROTESTADO" else "NÃO PROTESTADO", type text),
    
    // Remove colunas desnecessárias.
    // Nota: Este passo foi inferido do contexto anterior, pois estava faltando.
    colunasRemovidas = Table.RemoveColumns(colunaCondicionalAdicionada, {"VALORABATIM", "VALORDESCONTO", "CODTITULOOCORCOB", "CODOPERACAO", "CODBANCO", "CODAGENCIA", "CODCC", "CARTEIRA", "NOSSONUMERO", "SEUNUMERO", "DATAVENCIMENTO", "DATAOCORRENCIA", "DATACREDITO", "VALORDOCUMENTO", "VALORMORAJUROS", "VALORDESPCOBR", "VALORIOF", "VALOROUTRADESP", "VALOROUTROCRED", "VALORCREDITO", "DATAINCLUSAO", "USUAINCLUSAO", "DATAALTERACAO", "CODOCORINTERNA", "CODOCORCOBRBANCO", "TOCORRENCIA", "USUAALTERACAO", "AGENCIARECEBEDORA", "BANCORECEBEDOR", "MD5", "EMAILENVIADO", "STATUS", "CODCONTACOBRANCA"}),
    
    // Remove linhas duplicadas com base na coluna 'CODTITULO'.
    duplicatasRemovidas = Table.Distinct(colunasRemovidas, {"CODTITULO"}),
  #"Colunas renomeadas" = Table.RenameColumns(duplicatasRemovidas, {{"CODTITULO", "cod_titulo"}})
in
    #"Colunas renomeadas";
shared source_rlc_titulos_ocorrencias_cobranca = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
  Navegação = navegacao_2{[Id = "rlc_titulos_ocorrencias_cobranca", ItemKind = "Table"]}[Data]
in
    Navegação;
shared staging_rlc_titulos_ocorrencias_cobranca = let
  Origem = source_rlc_titulos_ocorrencias_cobranca,
    #"Tipo Alterado" = Table.TransformColumnTypes(Origem,{{"DATAOCORRENCIA", type date}, {"DATAVENCIMENTO", type date}, {"DATACREDITO", type date}}),
    // Classifica as linhas pela 'DATAOCORRENCIA' em ordem decrescente.
    linhasClassificadas = Table.Sort(#"Tipo Alterado",{{"DATAOCORRENCIA", Order.Descending}}),
    
    // Filtra as linhas com data de ocorrência posterior a 2024-01-01.
    linhasFiltradas = Table.SelectRows(linhasClassificadas, each [DATAOCORRENCIA] > #date(2024, 1, 1)),
    
    // Cria uma coluna mesclada 'Mesclado' a partir de TOCORRENCIA, CODOCORINTERNA e CODBANCO.
    colunaMescladaInserida = Table.AddColumn(linhasFiltradas, "chave_ocorrencia", each Text.Combine({Text.From([TOCORRENCIA], "pt-BR"), "-", Text.From([CODOCORINTERNA], "pt-BR"), "-", Text.From([CODBANCO], "pt-BR")}), type text),
    
    // Remove colunas desnecessárias.
    colunasRemovidas = Table.RemoveColumns(colunaMescladaInserida,{"MOTIVOCODOCORCOBRBANCO", "MOTIVOCONFINSTRUCAO", "CODOCORCOBR", "DATAGRAVACAO"}),
    
    // Mescla com 'lb_tipo_cobranca' usando 'Mesclado' e 'chave_ocorrencia'.
    consultasMescladas = Table.NestedJoin(colunasRemovidas, {"chave_ocorrencia"}, dim_tipo_cobranca, {"chave_ocorrencia"}, "lb_tipo_cobranca", JoinKind.LeftOuter),
    
    // Expande a coluna 'lb_tipo_cobranca' para incluir 'DESCRICAO'.
    tipoCobrancaExpandido = Table.ExpandTableColumn(consultasMescladas, "lb_tipo_cobranca", {"descricao"}, {"descricao"})
in
    tipoCobrancaExpandido;
shared dim_tipo_cobranca = let
  Origem = Lakehouse.Contents([HierarchicalNavigation = null]),
  #"Navegação 1" = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 3" = #"Navegação 2"{[Id = "dim_tipo_cobranca", ItemKind = "Table"]}[Data]
in
  #"Navegação 3";
shared fact_protestos_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_protestos", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
