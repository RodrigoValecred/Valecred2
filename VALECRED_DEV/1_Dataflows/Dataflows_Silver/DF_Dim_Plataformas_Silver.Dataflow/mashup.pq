[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_plataformas_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_plataformas = let
  Origem = source_cad_agencias,
    colunas_selecionadas = Table.SelectColumns(Origem, {"CODAGENCIA", "NOME"}),
    colunas_renomeadas = Table.RenameColumns(colunas_selecionadas, {{"CODAGENCIA", "cod_agencia"}, {"NOME", "nome_plataforma"}}),
  #"Consultas mescladas" = Table.NestedJoin(colunas_renomeadas, {"cod_agencia"}, sup_gestor_de_plataforma, {"cod_agencia"}, "sup_gestor_de_plataforma", JoinKind.LeftOuter),
  #"Expandido sup_gestor_de_plataforma" = Table.ExpandTableColumn(#"Consultas mescladas", "sup_gestor_de_plataforma", {"gestor_da_plataforma"}, {"gestor_da_plataforma"}),
  #"Inserted Text After Delimiter" = Table.AddColumn(#"Expandido sup_gestor_de_plataforma", "plataforma", each Text.AfterDelimiter([nome_plataforma], " "), type text)
in
    #"Inserted Text After Delimiter";
shared source_cad_agencias = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "cad_agencias", ItemKind = "Table"]}[Data]
in
    tabela_origem;
shared sup_gestor_de_plataforma = let
  Origem = Lakehouse.Contents(null),
  Navegação = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 1" = Navegação{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Id = "sup_gestor_de_plataforma", ItemKind = "Table"]}[Data]
in
  #"Navegação 2";
shared dim_plataformas_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_plataformas", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
