section Section1;

// ---------------- SOURCES (from Bronze/Silver Lakehouse) ----------------
shared source_cad_brokers = let
    Origem = Lakehouse.Contents([]),
    Workspace = Origem{[workspaceId="41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    Lakehouse = Workspace{[lakehouseId="553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    Tabela = Lakehouse{[Id="cad_brokers", ItemKind="Table"]}[Data]
in
    Tabela;

shared source_cad_geral = let
    Origem = Lakehouse.Contents([]),
    Workspace = Origem{[workspaceId="41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    Lakehouse = Workspace{[lakehouseId="553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    Tabela = Lakehouse{[Id="cad_geral_pf_pj", ItemKind="Table"]}[Data]
in
    Tabela;

shared source_cad_agencias = let
    Origem = Lakehouse.Contents([]),
    Workspace = Origem{[workspaceId="41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    Lakehouse = Workspace{[lakehouseId="553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    Tabela = Lakehouse{[Id="cad_agencias", ItemKind="Table"]}[Data]
in
    Tabela;

shared sup_gestor_de_plataforma = let
    Origem = Lakehouse.Contents([]),
    Workspace = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    Lakehouse = Workspace{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
    Tabela = Lakehouse{[Id = "sup_gestor_de_plataforma", ItemKind = "Table"]}[Data]
in
    Tabela;

// ---------------- STAGING ----------------

shared staging_gerentes_limpa = let
    Origem = source_cad_brokers,
    #"Colunas renomeadas" = Table.RenameColumns(Origem, {{"CODBROKER", "cod_broker"}, {"CPFCNPJ", "cpf_cnpj"}, {"CODAGENCIA", "cod_agencia"}, {"CODUSUARIO", "cod_usuario"}})
in
    #"Colunas renomeadas";

shared staging_cad_geral_limpa = let
    Origem = source_cad_geral,
    #"Colunas renomeadas" = Table.RenameColumns(Origem, {{"CPFCNPJ", "cpf_cnpj"}, {"NOME", "nome"}})
in
    #"Colunas renomeadas";

shared staging_plataformas_limpa = let
    Origem = source_cad_agencias,
    #"Colunas renomeadas" = Table.RenameColumns(Origem, {{"CODAGENCIA", "cod_agencia"}, {"NOME", "nome_plataforma"}}),
    #"Mesclar com Gestor" = Table.NestedJoin(#"Colunas renomeadas", {"cod_agencia"}, sup_gestor_de_plataforma, {"cod_agencia"}, "gestor_info", JoinKind.LeftOuter),
    #"Expandir Gestor" = Table.ExpandTableColumn(#"Mesclar com Gestor", "gestor_info", {"gestor_da_plataforma"}, {"gestor_da_plataforma"})
in
    #"Expandir Gestor";


// ---------------- DIMENSION ----------------

shared dim_gerente = let
    origem = staging_gerentes_limpa,
    join_com_cad_geral = Table.NestedJoin(origem, {"cpf_cnpj"}, staging_cad_geral_limpa, {"cpf_cnpj"}, "cad_geral", JoinKind.LeftOuter),
    expandir_nome = Table.ExpandTableColumn(join_com_cad_geral, "cad_geral", {"nome"}, {"nome_gerente"}),
    join_com_plataforma = Table.NestedJoin(expandir_nome, {"cod_agencia"}, staging_plataformas_limpa, {"cod_agencia"}, "plataforma", JoinKind.LeftOuter),
    expandir_plataforma = Table.ExpandTableColumn(join_com_plataforma, "plataforma", {"nome_plataforma", "gestor_da_plataforma"}, {"nome_plataforma", "gestor_da_plataforma"}),
    adicionar_sk = Table.AddColumn(expandir_plataforma, "sk_gerente", each Text.From([cod_broker]), type text),
    colunas_finais = Table.SelectColumns(adicionar_sk, {
        "sk_gerente", "cod_broker", "cpf_cnpj", "nome_gerente",
        "cod_agencia", "nome_plataforma", "gestor_da_plataforma", "cod_usuario"
    }),
    texto_em_maiuscula = Table.TransformColumns(colunas_finais, {{"nome_gerente", each Text.Upper(_), type nullable text}})
in
    texto_em_maiuscula;


// ---------------- DATA DESTINATION ----------------
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_gerente", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_gerente_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "056c8023-ea69-497e-9d3e-51c3ad689a9c"]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_gerente", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;