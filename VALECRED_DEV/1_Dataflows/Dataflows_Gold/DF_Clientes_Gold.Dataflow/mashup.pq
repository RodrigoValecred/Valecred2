section Section1;

// ---------------- SOURCES (from Bronze/Silver Lakehouse) ----------------
shared source_cad_clientes = let
    Origem = Lakehouse.Contents([]),
    Workspace = Origem{[workspaceId="41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    Lakehouse = Workspace{[lakehouseId="553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    Tabela = Lakehouse{[Id="cad_clientes", ItemKind="Table"]}[Data]
in
    Tabela;

shared source_cad_geral = let
    Origem = Lakehouse.Contents([]),
    Workspace = Origem{[workspaceId="41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    Lakehouse = Workspace{[lakehouseId="553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    Tabela = Lakehouse{[Id="cad_geral_pf_pj", ItemKind="Table"]}[Data]
in
    Tabela;

shared source_cad_contratos_clientes = let
    Origem = Lakehouse.Contents([]),
    Workspace = Origem{[workspaceId="41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    Lakehouse = Workspace{[lakehouseId="553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    Tabela = Lakehouse{[Id="cad_contratos_clientes", ItemKind="Table"]}[Data]
in
    Tabela;

shared source_cad_enderecos = let
    Origem = Lakehouse.Contents([]),
    Workspace = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    Lakehouse = Workspace{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    Tabela = Lakehouse{[Id = "cad_enderecos", ItemKind = "Table"]}[Data]
in
    Tabela;

shared sup_status_de_clientes_da_esteira = let
    Origem = Lakehouse.Contents([]),
    Workspace = Origem{[workspaceId="41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    Lakehouse = Workspace{[lakehouseId="8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
    Tabela = Lakehouse{[Id = "sup_status_de_clientes_da_esteira", ItemKind = "Table"]}[Data]
in
    Tabela;

// ---------------- STAGING ----------------

shared staging_clientes_limpa = let
    Origem = source_cad_clientes,
    #"Colunas renomeadas" = Table.RenameColumns(Origem, {{"CODCLIENTE", "cod_cliente"}, {"CPFCNPJ", "cpf_cnpj"}, {"CODATIVIDADE", "cnae"}, {"DATASTATUS", "data_status"}, {"CODSTATUSCLIENTE", "cod_status_cliente"}, {"CODRATING", "cod_rating"}, {"CODRATINGSUG", "cod_rating_sug"}, {"DATAINCLUSAO", "data_inclusao"}, {"DATAALTERACAO", "data_alteracao"}, {"RECUPERACAOJUDICIAL", "recuperacao_judicial"}})
in
    #"Colunas renomeadas";

shared staging_cad_geral_limpa = let
    Origem = source_cad_geral,
    #"Colunas renomeadas" = Table.RenameColumns(Origem, {{"CPFCNPJ", "cpf_cnpj"}, {"NOME", "nome"}})
in
    #"Colunas renomeadas";

shared staging_contratos_clientes_limpa = let
    Origem = source_cad_contratos_clientes,
    #"Colunas renomeadas" = Table.RenameColumns(Origem, {{"CODCLIENTE", "cod_cliente"}, {"CODCONTRATO", "cod_contrato"}, {"DATAINCLUSAO", "data_inclusao_contrato"}, {"STATUS", "status_contrato"}})
in
    #"Colunas renomeadas";

shared staging_enderecos_limpa = let
    Origem = source_cad_enderecos,
    #"Colunas renomeadas" = Table.RenameColumns(Origem, {{"CPFCNPJ", "cpf_cnpj"}, {"ENDERECO", "endereco"}, {"NUMERO", "numero"}, {"BAIRRO", "bairro"}, {"CIDADE", "cidade"}, {"UF", "uf"}, {"CEP", "cep"}})
in
    #"Colunas renomeadas";

shared staging_status_clientes_esteira_limpa = let
    Origem = sup_status_de_clientes_da_esteira,
    #"Colunas renomeadas" = Table.RenameColumns(Origem, {{"codstatuscliente", "cod_status_cliente"}, {"status_do_cliente", "status_cliente_esteira"}, {"macroprocesso", "macroprocesso_esteira"}, {"fase", "fase_esteira"}})
in
    #"Colunas renomeadas";

// ---------------- DIMENSION ----------------

shared dim_cliente = let
    origem = staging_clientes_limpa,
    join_com_cad_geral = Table.NestedJoin(origem, {"cpf_cnpj"}, staging_cad_geral_limpa, {"cpf_cnpj"}, "cad_geral", JoinKind.LeftOuter),
    expandir_nome = Table.ExpandTableColumn(join_com_cad_geral, "cad_geral", {"nome"}, {"nome_cliente"}),
    join_com_enderecos = Table.NestedJoin(expandir_nome, {"cpf_cnpj"}, staging_enderecos_limpa, {"cpf_cnpj"}, "enderecos", JoinKind.LeftOuter),
    expandir_endereco = Table.ExpandTableColumn(join_com_enderecos, "enderecos", {"endereco", "numero", "bairro", "cidade", "uf", "cep"}, {"endereco", "numero", "bairro", "cidade", "uf", "cep"}),
    join_com_status = Table.NestedJoin(expandir_endereco, {"cod_status_cliente"}, staging_status_clientes_esteira_limpa, {"cod_status_cliente"}, "status_esteira", JoinKind.LeftOuter),
    expandir_status = Table.ExpandTableColumn(join_com_status, "status_esteira", {"status_cliente_esteira", "macroprocesso_esteira", "fase_esteira"}, {"status_cliente_esteira", "macroprocesso_esteira", "fase_esteira"}),
    join_com_contratos = Table.NestedJoin(expandir_status, {"cod_cliente"}, staging_contratos_clientes_limpa, {"cod_cliente"}, "contratos", JoinKind.LeftOuter),
    expandir_contratos = Table.ExpandTableColumn(join_com_contratos, "contratos", {"cod_contrato", "data_inclusao_contrato", "status_contrato"}, {"cod_contrato", "data_inclusao_contrato", "status_contrato"}),
    adicionar_sk = Table.AddColumn(expandir_contratos, "sk_cliente", each Text.From([cod_cliente]), type text),
    colunas_finais = Table.SelectColumns(adicionar_sk, {
        "sk_cliente", "cod_cliente", "cpf_cnpj", "nome_cliente", "cnae",
        "data_status", "cod_status_cliente", "status_cliente_esteira", "macroprocesso_esteira", "fase_esteira",
        "cod_rating", "cod_rating_sug", "recuperacao_judicial",
        "endereco", "numero", "bairro", "cidade", "uf", "cep",
        "cod_contrato", "data_inclusao_contrato", "status_contrato",
        "data_inclusao", "data_alteracao"
    }),
    // Remover duplicatas por sk_cliente, mantendo o mais recente
    ordenar_por_data = Table.Sort(colunas_finais,{{"data_alteracao", Order.Descending}}),
    remover_duplicatas = Table.Distinct(ordenar_por_data, {"sk_cliente"})
in
    remover_duplicatas;

// ---------------- DATA DESTINATION ----------------
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_cliente_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_cliente_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "056c8023-ea69-497e-9d3e-51c3ad689a9c"]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_cliente", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;