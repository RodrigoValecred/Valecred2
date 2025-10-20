section Section1;
shared staging_operacoes_limpa = let
  Origem = Lakehouse.Contents([]),
  #"Navegação 1" = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 3" = #"Navegação 2"{[Id = "staging_operacoes_limpa", ItemKind = "Table"]}[Data]
in
  #"Navegação 3";
shared Bronze_tab_tipooperacao = let
  Origem = Lakehouse.Contents([]),
  #"Navegação 1" = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
  Navegação = #"Navegação 2"{[Id = "tab_tipooperacao", ItemKind = "Table"]}[Data]
in
  Navegação;
shared Bronze_tab_subtipooperacao = let
  Origem = Lakehouse.Contents([]),
  #"Navegação 1" = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
  Navegação = #"Navegação 2"{[Id = "tab_subtipooperacao", ItemKind = "Table"]}[Data]
in
  Navegação;
shared staging_produto_lbfactor = let
    // Source data from staging_operacoes_limpa
    Source = staging_operacoes_limpa,
    // Select only the columns needed for the product dimension
    SelectTtoAndSttoColumns = Table.SelectColumns(Source, {"STTO", "TTO"}),
    // Remove duplicate TTO and STTO combinations
    RemoveDuplicateTtoStto = Table.Distinct(SelectTtoAndSttoColumns, {"STTO", "TTO"}),
    // Join with tipo operacao to get the product type description
    JoinWithTipoOperacao = Table.NestedJoin(RemoveDuplicateTtoStto, {"TTO"}, Bronze_tab_tipooperacao, {"CODTTO"}, "Bronze_tab_tipooperacao", JoinKind.LeftOuter),
    ExpandTipoProduto = Table.ExpandTableColumn(JoinWithTipoOperacao, "Bronze_tab_tipooperacao", {"DESCRICAO"}, {"TipoProduto"}),
    // Join with sub tipo operacao to get the product subtype description
    JoinWithSubTipoOperacao = Table.NestedJoin(ExpandTipoProduto, {"STTO"}, Bronze_tab_subtipooperacao, {"CODSTTO"}, "Bronze_tab_subtipooperacao", JoinKind.LeftOuter),
    ExpandSubTipoProduto = Table.ExpandTableColumn(JoinWithSubTipoOperacao, "Bronze_tab_subtipooperacao", {"DESCRICAO"}, {"SubTipoProduto"}),
    // Create a composite product key
    AddProductKey = Table.AddColumn(ExpandSubTipoProduto, "chave_produto", each Text.Combine({[TTO],[STTO]}), type text),
    // Create a user-friendly product name
    AddProductName = Table.AddColumn(AddProductKey, "Produto", each if [SubTipoProduto] = null then [TipoProduto] else Text.Combine({[SubTipoProduto]," - ",[TipoProduto]}), type text),
    // Sort by product name
    SortByProductName = Table.Sort(AddProductName, {{"Produto", Order.Ascending}}),
    // Standardize product names
    ReplaceComissaria = Table.ReplaceValue(SortByProductName, "COMISSÁRIA", "COMISSARIA SIMPLES", Replacer.ReplaceValue, {"Produto"}),
    ReplaceComissariaSimples = Table.ReplaceValue(ReplaceComissaria, "COMISSARIA SIMPLES - COMISSÁRIA", "COMISSARIA SIMPLES", Replacer.ReplaceValue, {"Produto"}),
    // Duplicate product column for market information
    DuplicateProductColumn = Table.DuplicateColumn(ReplaceComissariaSimples, "Produto", "ProdutoInformacaoMercado"),
    // Standardize market information product names
    ReplaceNormalForDesconto = Table.ReplaceValue(DuplicateProductColumn, "NORMAL", "DESCONTO", Replacer.ReplaceValue, {"ProdutoInformacaoMercado"}),
    // Select final columns
    SelectFinalColumns = Table.SelectColumns(ReplaceNormalForDesconto, {"ProdutoInformacaoMercado", "Produto", "chave_produto"})
in
    SelectFinalColumns;
shared dim_produto_manual = let
  Origem = Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("i45Wcg5W0lFydg0OPtzsj8SKjQUA", BinaryEncoding.Base64), Compression.Deflate)), let _t = ((type nullable text) meta [Serialized.Text = true]) in type table [chave_produto = _t, Produto = _t, ProdutoInformacaoMercado = _t])
in
  Origem;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_produto_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "ProdutoInformacaoMercado", DestinationColumnName = "ProdutoInformacaoMercado"], [SourceColumnName = "Produto", DestinationColumnName = "Produto"], [SourceColumnName = "chave_produto", DestinationColumnName = "chave_produto"], [SourceColumnName = "sk_produto", DestinationColumnName = "sk_produto"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared dim_produto = let
    // Source from the main product query
    Source = staging_produto_lbfactor,
    // Add an index column to act as a surrogate key
    AddIndexColumn = Table.AddIndexColumn(Source, "Índice", 0, 1, Int64.Type),
    // Rename index to surrogate key
    RenameToSurrogateKey = Table.RenameColumns(AddIndexColumn,{{"Índice", "sk_produto"}}),
    // Append manually defined products
    AppendManualProducts = Table.Combine({RenameToSurrogateKey, dim_produto_manual}),
    // Remove duplicates based on the product key
    RemoveDuplicateProducts = Table.Distinct(AppendManualProducts, {"chave_produto"})
in
    RemoveDuplicateProducts;
shared dim_produto_DataDestination = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "94658e27-4b41-4bdf-b511-de32f3e1c90b"]}[Data],
  TableNavigation = Navigation_2{[Item = "dim_produto", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
