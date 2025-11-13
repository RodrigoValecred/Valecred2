[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared source_cad_usuarios = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "cad_usuarios", ItemKind = "Table"]}[Data]
in
    tabela_origem;
shared sup_nivel_usuario = let
  Origem = Lakehouse.Contents(null),
  Navegação = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 1" = Navegação{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Id = "sup_nivel_usuario", ItemKind = "Table"]}[Data]
in
  #"Navegação 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_usuarios_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_usuarios = let
  Origem = source_cad_usuarios,
  #"As outras colunas foram removidas" = Table.SelectColumns(Origem, {"CODUSUARIO", "CPFCNPJ", "NOME", "NIVEL", "EMAIL", "FUNCAO", "APELIDO"}),

    // Converte a coluna NOME para maiúsculas.
    nomeEmMaiusculas = Table.TransformColumns(#"As outras colunas foram removidas", {{"NOME", Text.Upper, type text}}),

    // Converte a coluna FUNCAO para maiúsculas.
    funcaoEmMaiusculas = Table.TransformColumns(nomeEmMaiusculas, {{"FUNCAO", Text.Upper, type nullable text}}),
  #"Consultas mescladas" = Table.NestedJoin(funcaoEmMaiusculas, {"NIVEL"}, sup_nivel_usuario, {"nivel"}, "sup_nivel_usuario", JoinKind.LeftOuter),
  #"Expandido sup_nivel_usuario" = Table.ExpandTableColumn(#"Consultas mescladas", "sup_nivel_usuario", {"descricao"}, {"nivel"}),
  #"Colunas removidas" = Table.RemoveColumns(#"Expandido sup_nivel_usuario", {"NIVEL", "FUNCAO"}),
  #"Texto em maiúscula" = Table.TransformColumns(#"Colunas removidas", {{"APELIDO", each Text.Upper(_), type nullable text}}),
  #"Colunas renomeadas" = Table.RenameColumns(#"Texto em maiúscula", {{"nivel", "nivel"}, {"CODUSUARIO", "cod_usuario"}, {"CPFCNPJ", "cpf_cnpj"}, {"NOME", "nome_do_usuario"}, {"EMAIL", "email"}, {"APELIDO", "apelido"}}),
  #"Duplicatas removidas" = Table.Distinct(#"Colunas renomeadas", {"cod_usuario"})
  in
  #"Duplicatas removidas";
shared dim_usuarios_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_usuarios", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
