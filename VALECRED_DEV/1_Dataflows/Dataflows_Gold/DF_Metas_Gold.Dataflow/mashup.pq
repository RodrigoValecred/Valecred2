[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared sup_metas = let
    Origem = Lakehouse.Contents([]),
    Workspace = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    Navegação = Workspace{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
    #"Navegação 1" = Navegação{[Id = "sup_metas", ItemKind = "Table"]}[Data]
in
    #"Navegação 1";
shared dim_calendario = let
    Origem = Lakehouse.Contents([]),
    Navegação = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    #"Navegação 1" = Navegação{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
    #"Navegação 2" = #"Navegação 1"{[Id = "dim_calendario", ItemKind = "Table"]}[Data]
in
    #"Navegação 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fato_metas_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "sk_data", DestinationColumnName = "sk_data"], [SourceColumnName = "sk_empresa", DestinationColumnName = "sk_empresa"], [SourceColumnName = "sk_plataforma", DestinationColumnName = "sk_plataforma"], [SourceColumnName = "vop_meta", DestinationColumnName = "vop_meta"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared fato_metas = let
    // Source from sup_metas, assuming columns 'data' and 'valor'
    FonteMetas = sup_metas,

    // Transformar coluna data para tipo de data para join
    AlterarTipoData = Table.TransformColumnTypes(FonteMetas,{{"mes", type date}}),

    // Join com dim_calendario para obter sk_data
    JuntarDimCalendario = Table.NestedJoin(AlterarTipoData, {"mes"}, dim_calendario, {"data"}, "dim_calendario", JoinKind.LeftOuter),
    ExpandirDimCalendario = Table.ExpandTableColumn(JuntarDimCalendario, "dim_calendario", {"sk_data"}, {"sk_data"}),

    // Adicionar SKs placeholder para dimensões futuras
    AdicionarSKsEmpresa = Table.AddColumn(ExpandirDimCalendario, "sk_empresa", each -1, Int64.Type),
    AdicionarSKsPlataforma = Table.AddColumn(AdicionarSKsEmpresa, "sk_plataforma", each -1, Int64.Type),

    // Selecionar e renomear colunas finais
    ColunasFinais = Table.SelectColumns(AdicionarSKsPlataforma, {"sk_data", "sk_empresa", "sk_plataforma", "vop"}),
    RenomearColunas = Table.RenameColumns(ColunasFinais,{{"vop", "vop_meta"}})
in
    RenomearColunas;
shared fato_metas_DataDestination = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "94658e27-4b41-4bdf-b511-de32f3e1c90b"]}[Data],
  TableNavigation = Navigation_2{[Item = "fato_metas", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
