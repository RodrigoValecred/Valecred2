[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared dim_usuario = let
  Origem = Lakehouse.Contents(null),
  Navegação = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 1" = Navegação{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Id = "dim_usuario", ItemKind = "Table"]}[Data]
in
  #"Navegação 2";
shared staging_tac_m = let
  Origem = Lakehouse.Contents(null),
  Navegação = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 1" = Navegação{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Id = "staging_tac_m", ItemKind = "Table"]}[Data]
in
  #"Navegação 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fato_tac_m_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "cod_tarifa_extra", DestinationColumnName = "cod_tarifa_extra"], [SourceColumnName = "cod_operacao", DestinationColumnName = "cod_operacao"], [SourceColumnName = "descricao", DestinationColumnName = "descricao"], [SourceColumnName = "total", DestinationColumnName = "total"], [SourceColumnName = "data_inclusao", DestinationColumnName = "data_inclusao"], [SourceColumnName = "cod_usuario", DestinationColumnName = "cod_usuario"], [SourceColumnName = "analista", DestinationColumnName = "analista"], [SourceColumnName = "cod_empresa", DestinationColumnName = "cod_empresa"], [SourceColumnName = "cod_cliente", DestinationColumnName = "cod_cliente"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared fato_tac_m = let
  Origem = Table.NestedJoin(staging_tac_m, {"cod_usuario"}, dim_usuario, {"CODUSUARIO"}, "dim_usuario", JoinKind.LeftOuter),
  #"Expandido dim_usuario" = Table.ExpandTableColumn(Origem, "dim_usuario", {"APELIDO"}, {"analista"}),
  #"Linhas classificadas" = Table.Sort(#"Expandido dim_usuario", {{"data_inclusao", Order.Descending}}),
  #"Consultas mescladas" = Table.NestedJoin(#"Linhas classificadas", {"cod_operacao"}, staging_operacoes_limpa, {"CODOPERACAO"}, "staging_operacoes_limpa", JoinKind.LeftOuter),
  #"Expandido staging_operacoes_limpa" = Table.ExpandTableColumn(#"Consultas mescladas", "staging_operacoes_limpa", {"CODEMPRESA", "CODCLIENTE"}, {"cod_empresa", "cod_cliente"})
in
  #"Expandido staging_operacoes_limpa";
shared staging_operacoes_limpa = let
  Origem = Lakehouse.Contents(null),
  Navegação = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 1" = Navegação{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Id = "staging_operacoes_limpa", ItemKind = "Table"]}[Data]
in
  #"Navegação 2";
shared fato_tac_m_DataDestination = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "94658e27-4b41-4bdf-b511-de32f3e1c90b"]}[Data],
  TableNavigation = Navigation_2{[Item = "fato_tac_m", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
