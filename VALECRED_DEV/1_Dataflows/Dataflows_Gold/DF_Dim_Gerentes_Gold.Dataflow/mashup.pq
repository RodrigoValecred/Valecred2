[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared staging_cad_geral = let
  Origem = Lakehouse.Contents(null),
  Navegação = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 1" = Navegação{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Id = "staging_cad_geral", ItemKind = "Table"]}[Data]
in
  #"Navegação 2";
shared staging_gerentes = let
  Origem = Lakehouse.Contents(null),
  Navegação = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 1" = Navegação{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Id = "staging_gerentes", ItemKind = "Table"]}[Data]
in
  #"Navegação 2";
shared staging_plataformas = let
  Origem = Lakehouse.Contents(null),
  Navegação = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 1" = Navegação{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Id = "staging_plataformas", ItemKind = "Table"]}[Data]
in
  #"Navegação 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_gerentes_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "cod_broker", DestinationColumnName = "cod_broker"], [SourceColumnName = "cod_usuario", DestinationColumnName = "cod_usuario"], [SourceColumnName = "cpf_cnpj", DestinationColumnName = "cpf_cnpj"], [SourceColumnName = "cod_agencia", DestinationColumnName = "cod_agencia"], [SourceColumnName = "nome_gerente", DestinationColumnName = "nome_gerente"], [SourceColumnName = "nome_gestor_da_plataforma", DestinationColumnName = "nome_gestor_da_plataforma"], [SourceColumnName = "nome_plataforma", DestinationColumnName = "nome_plataforma"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared dim_gerentes = let
  Origem = staging_gerentes,
  #"Consultas mescladas" = Table.NestedJoin(Origem, {"cpf_cnpj"}, staging_cad_geral, {"cpf_cnpj"}, "staging_cad_geral", JoinKind.LeftOuter),
  #"Expandido staging_cad_geral" = Table.ExpandTableColumn(#"Consultas mescladas", "staging_cad_geral", {"nome"}, {"nome_gerente"}),
  #"Consultas mescladas 1" = Table.NestedJoin(#"Expandido staging_cad_geral", {"cod_agencia"}, staging_plataformas, {"cod_agencia"}, "staging_plataformas", JoinKind.LeftOuter),
  #"Expandido staging_plataformas" = Table.ExpandTableColumn(#"Consultas mescladas 1", "staging_plataformas", {"gestor_da_plataforma", "plataforma"}, {"nome_gestor_da_plataforma", "nome_plataforma"})
in
  #"Expandido staging_plataformas";
shared dim_gerentes_DataDestination = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "94658e27-4b41-4bdf-b511-de32f3e1c90b"]}[Data],
  TableNavigation = Navigation_2{[Item = "dim_gerentes", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
