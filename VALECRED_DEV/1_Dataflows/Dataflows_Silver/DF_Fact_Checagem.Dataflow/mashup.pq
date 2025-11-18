[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared source_tab_titulos_confirmacao = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
  Navegação = navegacao_2{[Id = "tab_titulos_confirmacao", ItemKind = "Table"]}[Data]
in
    Navegação;
shared staging_limpeza_confirmacao = let
  Origem = source_tab_titulos_confirmacao,
  #"As outras colunas foram removidas" = Table.SelectColumns(Origem, {"CODOPERACAO", "CODTITULO", "CONFAO", "CODOCORCONFIRMACAO", "DATAINCLUSAO"}),
  #"Colunas renomeadas" = Table.RenameColumns(#"As outras colunas foram removidas", {{"CODOPERACAO", "cod_operacao"}, {"CODTITULO", "cod_titulo"}, {"CONFAO", "confirmacao"}, {"CODOCORCONFIRMACAO", "cod_ocor_confirmacao"}, {"DATAINCLUSAO", "data_inclusao"}})
in
    #"Colunas renomeadas";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_ultima_confirmacao_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared fact_ultima_confirmacao = let
  Origem = staging_limpeza_confirmacao,
    #"Linhas Classificadas" = Table.Sort(Origem,{{"data_inclusao", Order.Descending}}),
    #"Duplicatas Removidas" = Table.Distinct(#"Linhas Classificadas", {"cod_titulo"})
in
    #"Duplicatas Removidas";
shared fact_ultima_confirmacao_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_ultima_confirmacao", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
