[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared staging_clientes = let
  Origem = Lakehouse.Contents(null),
  Navegação = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 1" = Navegação{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Id = "staging_clientes", ItemKind = "Table"]}[Data]
in
  #"Navegação 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_clientes_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "cod_cliente", DestinationColumnName = "cod_cliente"], [SourceColumnName = "cpf_cnpj", DestinationColumnName = "cpf_cnpj"], [SourceColumnName = "cnae", DestinationColumnName = "cnae"], [SourceColumnName = "data_status", DestinationColumnName = "data_status"], [SourceColumnName = "cod_status_cliente", DestinationColumnName = "cod_status_cliente"], [SourceColumnName = "cod_rating", DestinationColumnName = "cod_rating"], [SourceColumnName = "cod_rating_sug", DestinationColumnName = "cod_rating_sug"], [SourceColumnName = "data_inclusao", DestinationColumnName = "data_inclusao"], [SourceColumnName = "data_alteracao", DestinationColumnName = "data_alteracao"], [SourceColumnName = "recuperacao_judicial", DestinationColumnName = "recuperacao_judicial"], [SourceColumnName = "nome_cliente", DestinationColumnName = "nome_cliente"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared dim_clientes = let
  Origem = staging_clientes,
  #"Consultas mescladas" = Table.NestedJoin(Origem, {"cpf_cnpj"}, staging_cad_geral, {"cpf_cnpj"}, "staging_cad_geral", JoinKind.LeftOuter),
  #"Expandido staging_cad_geral" = Table.ExpandTableColumn(#"Consultas mescladas", "staging_cad_geral", {"nome"}, {"nome_cliente"}),
  #"Duplicatas removidas" = Table.Distinct(#"Expandido staging_cad_geral", {"cod_cliente"})
in
  #"Duplicatas removidas";
shared staging_cad_geral = let
  Origem = Lakehouse.Contents(null),
  Navegação = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 1" = Navegação{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Id = "staging_cad_geral", ItemKind = "Table"]}[Data]
in
  #"Navegação 2";
shared dim_clientes_DataDestination = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "94658e27-4b41-4bdf-b511-de32f3e1c90b"]}[Data],
  TableNavigation = Navigation_2{[Item = "dim_clientes", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
