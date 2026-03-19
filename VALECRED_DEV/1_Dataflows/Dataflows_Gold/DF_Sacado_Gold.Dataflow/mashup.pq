section Section1;

// ---------------- FONTES (do Lakehouse Bronze/Silver) ----------------
shared source_tab_titulos = let
    Origem = Lakehouse.Contents([]),
    Workspace = Origem{[workspaceId="41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    Lakehouse = Workspace{[lakehouseId="553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    Tabela = Lakehouse{[Id="tab_titulos", ItemKind="Table"]}[Data]
in
    Tabela;

shared source_cad_geral = let
    Origem = Lakehouse.Contents([]),
    Workspace = Origem{[workspaceId="41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    Lakehouse = Workspace{[lakehouseId="553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    Tabela = Lakehouse{[Id="cad_geral_pf_pj", ItemKind="Table"]}[Data]
in
    Tabela;

shared source_cad_enderecos = let
    Origem = Lakehouse.Contents([]),
    Workspace = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    Lakehouse = Workspace{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    Tabela = Lakehouse{[Id = "cad_enderecos", ItemKind = "Table"]}[Data]
in
    Tabela;

// ---------------- STAGING ----------------

shared staging_titulos_limpa = let
    Origem = source_tab_titulos,
    #"Colunas renomeadas" = Table.RenameColumns(Origem, {{"CPFCNPJSACADO", "cpf_cnpj_sacado"}})
in
    #"Colunas renomeadas";

shared staging_cad_geral_limpa = let
    Origem = source_cad_geral,
    #"Colunas renomeadas" = Table.RenameColumns(Origem, {{"CPFCNPJ", "cpf_cnpj"}, {"NOME", "nome"}, {"EMAILS", "emails"}, {"TELEFONES", "telefones"}})
in
    #"Colunas renomeadas";

shared staging_enderecos_limpa = let
    Origem = source_cad_enderecos,
    #"Colunas renomeadas" = Table.RenameColumns(Origem, {{"CPFCNPJ", "cpf_cnpj"}, {"CIDADE", "cidade"}, {"UF", "uf"}, {"CEP", "cep"}})
in
    #"Colunas renomeadas";

// ---------------- DIMENSION ----------------

shared dim_sacado = let
    origem = staging_titulos_limpa,
    colunas_selecionadas = Table.SelectColumns(origem, {"cpf_cnpj_sacado"}),
    duplicatas_removidas = Table.Distinct(colunas_selecionadas, {"cpf_cnpj_sacado"}),
    join_com_cad_geral = Table.NestedJoin(duplicatas_removidas, {"cpf_cnpj_sacado"}, staging_cad_geral_limpa, {"cpf_cnpj"}, "cad_geral", JoinKind.LeftOuter),
    expandido_cad_geral = Table.ExpandTableColumn(join_com_cad_geral, "cad_geral", {"nome", "emails", "telefones"}, {"nome_sacado", "emails", "telefones"}),
    join_com_enderecos = Table.NestedJoin(expandido_cad_geral, {"cpf_cnpj_sacado"}, staging_enderecos_limpa, {"cpf_cnpj"}, "enderecos", JoinKind.LeftOuter),
    expandir_endereco = Table.ExpandTableColumn(join_com_enderecos, "enderecos", {"cidade", "uf", "cep"}, {"cidade", "uf", "cep"}),
    adicionar_sk = Table.AddColumn(expandir_endereco, "sk_sacado", each [cpf_cnpj_sacado], type text),
    selecao_colunas_finais = Table.SelectColumns(adicionar_sk, {"sk_sacado", "cpf_cnpj_sacado", "nome_sacado", "cep", "cidade", "uf", "emails", "telefones"})
in
    selecao_colunas_finais;


// ---------------- DATA DESTINATION ----------------
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_sacado_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_sacado_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "056c8023-ea69-497e-9d3e-51c3ad689a9c"]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_sacado", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;