section Section1;
shared staging_gerentes = let
    Origem = Lakehouse.Contents([]),
    Workspace = Origem{[workspaceId="41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    Lakehouse = Workspace{[lakehouseId="8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
    Tabela = Lakehouse{[Id="staging_gerentes", ItemKind="Table"]}[Data]
in
    Tabela;
shared staging_cad_geral = let
    Origem = Lakehouse.Contents([]),
    Workspace = Origem{[workspaceId="41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Lakehouse = Workspace{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  Tabela = Lakehouse{[Id = "staging_cad_geral", ItemKind = "Table"]}[Data]
in
    Tabela;
shared staging_plataformas = let
    Origem = Lakehouse.Contents([]),
    Workspace = Origem{[workspaceId="41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    Lakehouse = Workspace{[lakehouseId="8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  Navegação = Lakehouse{[Id = "staging_plataformas", ItemKind = "Table"]}[Data]
in
    Navegação;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_gerente_DataDestination (2)", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_gerente = let
    origem = staging_gerentes,
    join_com_cad_geral = Table.NestedJoin(origem, {"cpf_cnpj"}, staging_cad_geral, {"cpf_cnpj"}, "cad_geral", JoinKind.LeftOuter),
    expandir_nome = Table.ExpandTableColumn(join_com_cad_geral, "cad_geral", {"nome"}, {"nome_gerente"}),
    join_com_plataforma = Table.NestedJoin(expandir_nome, {"cod_agencia"}, staging_plataformas, {"cod_agencia"}, "plataforma", JoinKind.LeftOuter),
    expandir_plataforma = Table.ExpandTableColumn(join_com_plataforma, "plataforma", {"nome_plataforma", "gestor_da_plataforma"}, {"nome_plataforma", "gestor_da_plataforma"}),
    adicionar_sk = Table.AddColumn(expandir_plataforma, "sk_gerente", each Text.From([cod_broker]), type text),
    colunas_finais = Table.SelectColumns(adicionar_sk, {
        "sk_gerente", "cod_broker", "cpf_cnpj", "nome_gerente",
        "cod_agencia", "nome_plataforma", "gestor_da_plataforma", "cod_usuario"
    }),
    texto_em_maiuscula = Table.TransformColumns(colunas_finais, {{"nome_gerente", each Text.Upper(_), type nullable text}}),
  #"Duplicatas removidas" = Table.Distinct(texto_em_maiuscula, {"sk_gerente"}),
  #"Colunas renomeadas" = Table.RenameColumns(#"Duplicatas removidas", {{"cod_broker", "cod_gerente"}, {"cod_agencia", "cod_plataforma"}}),
  #"Tipo de coluna alterado" = Table.TransformColumnTypes(#"Colunas renomeadas", {{"sk_gerente", Int64.Type}})
in
    #"Tipo de coluna alterado";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_gerente_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_gerente_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "056c8023-ea69-497e-9d3e-51c3ad689a9c"]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_gerente", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"dim_gerente_DataDestination (2)" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "ee40705b-0100-49bc-8f35-81d71839f042"]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_gerente", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
