[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_gerentes_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_gerentes = let
    Fonte = source_cad_brokers,

// Mescla com a tabela geral de PF/PJ (replica_cad_geral_pf_pj) usando cpfCnpj.
cadastrosMesclados = Table.NestedJoin(
    Fonte, {"CPFCNPJ"}, // Chave de mesclagem em tiposDefinidos
    source_cad_geral, {"CPFCNPJ"}, // Chave de mesclagem em replica_cad_geral_pf_pj
    "Cadastros", JoinKind.LeftOuter
),

// Expande a coluna 'NOME' da tabela mesclada 'Cadastros' para 'nomeGerente'.
nomeGerenteExpandido = Table.ExpandTableColumn(
    cadastrosMesclados, "Cadastros", {"NOME"}, {"nome_gerente"}
),

// Mescla com Silver_Plataformas_Padronizacao usando codAgencia.
plataformasMescladas = Table.NestedJoin(
    nomeGerenteExpandido, {"CODAGENCIA"},
    dim_plataformas, {"cod_agencia"},
    "Plataformas", JoinKind.LeftOuter
),
    // Expande as colunas 'plataforma' e 'nomeGestorPlataforma' da tabela 'Plataformas'.
plataformasExpandidas = Table.ExpandTableColumn(plataformasMescladas, "Plataformas", {"plataforma", "gestor_da_plataforma"}, {"plataforma", "gestor"}),
  #"Colunas renomeadas" = Table.RenameColumns(plataformasExpandidas, {{"CODBROKER", "cod_broker"}, {"CPFCNPJ", "cpf_cnpj"}, {"STATUS", "status"}, {"CODAGENCIA", "cod_agencia"}}),
  #"Duplicatas removidas" = Table.Distinct(#"Colunas renomeadas", {"cod_broker"})
in
    #"Duplicatas removidas";
shared source_cad_brokers = let
    origem = Lakehouse.Contents([]),
    navegacao_1 = origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
    navegacao_2 = navegacao_1{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
    tabela_origem = navegacao_2{[Id = "cad_brokers", ItemKind = "Table"]}[Data],
  #"Linhas classificadas" = Table.Sort(tabela_origem, {{"CODBROKER", Order.Descending}}),
  #"As outras colunas foram removidas" = Table.SelectColumns(#"Linhas classificadas", {"CODBROKER", "CPFCNPJ", "STATUS", "CODAGENCIA"})
in
    #"As outras colunas foram removidas";
shared source_cad_geral = let
  Origem = Lakehouse.Contents(null),
  Navegação = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 1" = Navegação{[lakehouseId = "553c2931-573b-4db0-838d-a70a01306d32"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Id = "cad_geral_pf_pj", ItemKind = "Table"]}[Data],
  #"As outras colunas foram removidas" = Table.SelectColumns(#"Navegação 2", {"CPFCNPJ", "NOME", "RJ"}),
  #"Duplicatas removidas" = Table.Distinct(#"As outras colunas foram removidas", {"CPFCNPJ"})
in
  #"Duplicatas removidas";
shared dim_gerentes_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_gerentes", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_plataformas = let
  Origem = Lakehouse.Contents(null),
  Navegação = Origem{[workspaceId = "41ae19db-f71d-471f-9ac7-ccbc2c75ce11"]}[Data],
  #"Navegação 1" = Navegação{[lakehouseId = "8f85c372-56ad-4f3f-acf9-3be2e9b99513"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Id = "dim_plataformas", ItemKind = "Table"]}[Data]
in
  #"Navegação 2";
