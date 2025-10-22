section Section1;
shared staging_clientes = let
    Origem = Lakehouse.Contents([]),
    Workspace = Origem{[workspaceId="41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Lakehouse = Workspace{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  Tabela = Lakehouse{[Id = "staging_clientes", ItemKind = "Table"]}[Data]
in
    Tabela;
shared staging_cad_geral = let
    Origem = Lakehouse.Contents([]),
    Workspace = Origem{[workspaceId="41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Lakehouse = Workspace{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  Tabela = Lakehouse{[Id = "staging_cad_geral", ItemKind = "Table"]}[Data]
in
    Tabela;
shared staging_contratos_clientes = let
    Origem = Lakehouse.Contents([]),
    Workspace = Origem{[workspaceId="41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    Lakehouse = Workspace{[lakehouseId="8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
    Tabela = Lakehouse{[Id="staging_contratos_clientes", ItemKind="Table"]}[Data],
  #"Linhas filtradas" = Table.SelectRows(Tabela, each ([status] = "A"))
in
    #"Linhas filtradas";
shared staging_enderecos = let
    Origem = Lakehouse.Contents([]),
    Workspace = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    Lakehouse = Workspace{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
    Tabela = Lakehouse{[Id = "staging_enderecos", ItemKind = "Table"]}[Data]
in
    Tabela;
shared sup_status_de_clientes_da_esteira = let
    Origem = Lakehouse.Contents([]),
    Workspace = Origem{[workspaceId="41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    Lakehouse = Workspace{[lakehouseId="8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
    Tabela = Lakehouse{[Id = "sup_status_de_clientes_da_esteira", ItemKind = "Table"]}[Data]
in
    Tabela;
shared staging_status_clientes_esteira_limpa = let
    Origem = sup_status_de_clientes_da_esteira,
    #"Colunas renomeadas" = Table.RenameColumns(Origem, {{"codstatuscliente", "cod_status_cliente"}, {"status_do_cliente", "status_cliente_esteira"}, {"macroprocesso", "macroprocesso_esteira"}, {"fase", "fase_esteira"}})
in
    #"Colunas renomeadas";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_cliente_DataDestination (2)", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "sk_cliente", DestinationColumnName = "sk_cliente"], [SourceColumnName = "cod_cliente", DestinationColumnName = "cod_cliente"], [SourceColumnName = "cpf_cnpj", DestinationColumnName = "cpf_cnpj"], [SourceColumnName = "nome_cliente", DestinationColumnName = "nome_cliente"], [SourceColumnName = "cnae", DestinationColumnName = "cnae"], [SourceColumnName = "data_status", DestinationColumnName = "data_status"], [SourceColumnName = "cod_status_cliente", DestinationColumnName = "cod_status_cliente"], [SourceColumnName = "status_cliente_esteira", DestinationColumnName = "status_cliente_esteira"], [SourceColumnName = "macroprocesso_esteira", DestinationColumnName = "macroprocesso_esteira"], [SourceColumnName = "fase_esteira", DestinationColumnName = "fase_esteira"], [SourceColumnName = "cod_rating", DestinationColumnName = "cod_rating"], [SourceColumnName = "cod_rating_sug", DestinationColumnName = "cod_rating_sug"], [SourceColumnName = "recuperacao_judicial", DestinationColumnName = "recuperacao_judicial"], [SourceColumnName = "endereco", DestinationColumnName = "endereco"], [SourceColumnName = "numero", DestinationColumnName = "numero"], [SourceColumnName = "bairro", DestinationColumnName = "bairro"], [SourceColumnName = "cidade", DestinationColumnName = "cidade"], [SourceColumnName = "uf", DestinationColumnName = "uf"], [SourceColumnName = "cep", DestinationColumnName = "cep"], [SourceColumnName = "cod_contrato", DestinationColumnName = "cod_contrato"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared dim_cliente = let
    origem = staging_clientes,
    join_com_cad_geral = Table.NestedJoin(origem, {"cpf_cnpj"}, staging_cad_geral, {"cpf_cnpj"}, "cad_geral", JoinKind.LeftOuter),
    expandir_nome = Table.ExpandTableColumn(join_com_cad_geral, "cad_geral", {"nome"}, {"nome_cliente"}),
    join_com_enderecos = Table.NestedJoin(expandir_nome, {"cpf_cnpj"}, staging_enderecos, {"cpf_cnpj"}, "enderecos", JoinKind.LeftOuter),
    expandir_endereco = Table.ExpandTableColumn(join_com_enderecos, "enderecos", {"endereco", "numero", "bairro", "cidade", "uf", "cep"}, {"endereco", "numero", "bairro", "cidade", "uf", "cep"}),
    join_com_status = Table.NestedJoin(expandir_endereco, {"cod_status_cliente"}, staging_status_clientes_esteira_limpa, {"cod_status_cliente"}, "status_esteira", JoinKind.LeftOuter),
    expandir_status = Table.ExpandTableColumn(join_com_status, "status_esteira", {"status_cliente_esteira", "macroprocesso_esteira", "fase_esteira"}, {"status_cliente_esteira", "macroprocesso_esteira", "fase_esteira"}),
    join_com_contratos = Table.NestedJoin(expandir_status, {"cod_cliente"}, staging_contratos_clientes, {"cod_cliente"}, "contratos", JoinKind.LeftOuter),
    expandir_contratos = Table.ExpandTableColumn(join_com_contratos, "contratos", {"cod_contrato"}, {"cod_contrato"}),
    adicionar_sk = Table.AddColumn(expandir_contratos, "sk_cliente", each Text.From([cod_cliente]), type text),
    colunas_finais = Table.SelectColumns(adicionar_sk, {
        "sk_cliente", "cod_cliente", "cpf_cnpj", "nome_cliente", "cnae", "data_status", "cod_status_cliente", "status_cliente_esteira", "macroprocesso_esteira", "fase_esteira",
        "cod_rating", "cod_rating_sug", "recuperacao_judicial", "endereco", "numero", "bairro", "cidade", "uf", "cep", "cod_contrato"}),
    remover_duplicatas = Table.Distinct(colunas_finais, {"sk_cliente"})
in
    remover_duplicatas;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_cliente_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_cliente_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "056c8023-ea69-497e-9d3e-51c3ad689a9c"]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_cliente", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"dim_cliente_DataDestination (2)" = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "94658e27-4b41-4bdf-b511-de32f3e1c90b"]}[Data],
  TableNavigation = Navigation_2{[Item = "dim_cliente", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
